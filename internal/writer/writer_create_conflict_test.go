package writer_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B6 — Create collision returns raw Postgres error instead of typed sentinel.
//
// When Write is called with ExpectedVersion=0 and the resource already exists,
// the INSERT hits a duplicate-key constraint (23505). The error surfaces as a
// wrapped *pgconn.PgError instead of a typed ErrAlreadyExists. This breaks
// caller retry/read-back logic that checks errors.Is.
//
// The counter increment rolls back correctly (I1 holds), so the next successful
// write gets the expected seq.
//
// Expected current failure: errors.Is(err, ErrAlreadyExists) returns false;
// the error is a raw "create resource: ERROR: duplicate key value..." message.
func TestCreateConflict_ReturnsAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "test-replica")
	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dup-create",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "test-replica", LeaseEpoch: epoch,
	}

	// First create succeeds
	_, err = w.Write(ctx, req)
	require.NoError(t, err)

	// Second create with ExpectedVersion=0 hits duplicate key
	_, err = w.Write(ctx, req)
	require.Error(t, err)

	assert.ErrorIs(t, err, writer.ErrAlreadyExists,
		"B6: duplicate create must return ErrAlreadyExists, got: %v", err)

	// Counter must have rolled back — next write gets seq=2 (not 3)
	req2 := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dup-create-other",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "test-replica", LeaseEpoch: epoch,
	}
	result, err := w.Write(ctx, req2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Seq,
		"counter must roll back after failed create — next seq should be 2")
}
