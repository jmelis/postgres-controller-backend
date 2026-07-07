package race_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R10 — 409 handling corrupting the stream (I1).
// A version conflict on the resource upsert must roll back the counter increment.
// Defense: counter and upsert are in the same transaction; rollback undoes both.
func TestR10_ConflictRollbacksCounter(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	w := newWriter(t, nil)

	// Create the initial resource
	createReq := makeWriteReq("apps/v1/Deployment", "default", "nginx", 1)
	result, err := w.Write(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Seq)

	// Read counter value after create
	conn := freshConn(t)
	var counterBefore int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counterBefore)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counterBefore)

	// Attempt update with stale version (409)
	updateReq := model.WriteRequest{
		GVK:             "apps/v1/Deployment",
		Namespace:       "default",
		Name:            "nginx",
		BucketID:        1,
		Spec:            json.RawMessage(`{"replicas":99}`),
		Status:          json.RawMessage(`{}`),
		Metadata:        json.RawMessage(`{}`),
		ExpectedVersion: 999, // stale
	}
	_, err = w.Write(ctx, updateReq)
	assert.ErrorIs(t, err, writer.ErrConflict)

	// Counter must be unchanged — the increment was rolled back
	var counterAfter int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counterAfter)
	require.NoError(t, err)
	assert.Equal(t, counterBefore, counterAfter, "counter must not advance on 409")
}
