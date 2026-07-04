package writer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupLeaseAndWriter(t *testing.T, db *testinfra.TestDB) (*writer.Writer, int64) {
	t.Helper()
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewManager(leaseConn, "test-replica")
	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)
	return w, epoch
}

func makeReq(epoch int64) model.WriteRequest {
	return model.WriteRequest{
		GVK:         "apps/v1/Deployment",
		Namespace:   "default",
		Name:        "nginx",
		BucketID:    1,
		Spec:        json.RawMessage(`{"replicas":3}`),
		Status:      json.RawMessage(`{}`),
		Metadata:    json.RawMessage(`{"labels":{}}`),
		LeaseHolder: "test-replica",
		LeaseEpoch:  epoch,
	}
}

func TestCreateResource(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	result, err := w.Write(ctx, makeReq(epoch))
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Seq)
	assert.Equal(t, int64(1), result.ObjectVersion)
	assert.NotEmpty(t, result.UID)
}

func TestUpdateResource(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	req := makeReq(epoch)
	result1, err := w.Write(ctx, req)
	require.NoError(t, err)

	req.Spec = json.RawMessage(`{"replicas":5}`)
	req.ExpectedVersion = result1.ObjectVersion
	result2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result2.Seq)
	assert.Equal(t, int64(2), result2.ObjectVersion)
	assert.Equal(t, result1.UID, result2.UID)
}

func TestConflictReturns409(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	req := makeReq(epoch)
	_, err := w.Write(ctx, req)
	require.NoError(t, err)

	req.ExpectedVersion = 999
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrConflict)
}

func TestFenceViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	writerConn := db.Connect(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewManager(leaseConn, "test-replica")
	_, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	w := writer.New(writerConn, nil)
	req := makeReq(999) // wrong epoch
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrFenceViolation)
}

func TestSequentialWritesAreGapless(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		req := makeReq(epoch)
		req.Name = fmt.Sprintf("resource-%d", i)
		result, err := w.Write(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int64(i+1), result.Seq)
	}
}
