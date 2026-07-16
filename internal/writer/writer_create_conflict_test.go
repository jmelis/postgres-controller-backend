package writer_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B6 — Replayed create with identical content is suppressed (no-op).
// Replayed create with different content returns ErrAlreadyExists.
func TestCreateConflict_ReturnsAlreadyExists(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dup-create",
		Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
	}

	// First create succeeds
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r1.Changed)

	// Second create with identical content — suppressed (no-op)
	r2, err := w.Write(ctx, req)
	require.NoError(t, err, "replayed create with identical content must succeed as no-op")
	assert.False(t, r2.Changed, "replayed create with identical content must be suppressed")
	assert.Equal(t, r1.ObjectVersion, r2.ObjectVersion)

	// Create with DIFFERENT content — ErrAlreadyExists
	req.Spec = json.RawMessage(`{"replicas":99}`)
	_, err = w.Write(ctx, req)
	require.Error(t, err)
	assert.ErrorIs(t, err, writer.ErrAlreadyExists,
		"B6: duplicate create with different content must return ErrAlreadyExists, got: %v", err)

	// Txid is assigned from xid8 — next write should get a new txid > previous
	req2 := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dup-create-other",
		Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
	}
	result, err := w.Write(ctx, req2)
	require.NoError(t, err)
	assert.Greater(t, result.Txid, r1.Txid,
		"next write after failed create should get a txid greater than previous")
}
