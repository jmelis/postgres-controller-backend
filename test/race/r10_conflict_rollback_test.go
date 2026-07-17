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

// R10 — 409 handling: transaction rollback on conflict (I1).
// A version conflict on the resource upsert must roll back the entire
// transaction, including the txid_stamp assignment.
// Defense: txid acquisition and upsert are in the same transaction; rollback undoes both.
func TestR10_ConflictRollback(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	w := newWriter(t, nil)

	// Create the initial resource
	createReq := makeWriteReq("apps/v1/Deployment", "default", "nginx")
	result, err := w.Write(ctx, createReq)
	require.NoError(t, err)
	assert.Greater(t, result.Txid, uint64(0))

	// Read txid_stamp after create
	conn := freshConn(t)
	var txidBefore int64
	err = conn.QueryRow(ctx,
		`SELECT txid_stamp::text::bigint FROM kubernetes_resources WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'nginx'`,
	).Scan(&txidBefore)
	require.NoError(t, err)
	assert.Greater(t, txidBefore, int64(0))

	// Attempt update with stale version (409)
	updateReq := model.WriteRequest{
		GVK:             "apps/v1/Deployment",
		Namespace:       "default",
		Name:            "nginx",
		Spec:            json.RawMessage(`{"replicas":99}`),
		Status:          json.RawMessage(`{}`),
		Metadata:        json.RawMessage(`{}`),
		ExpectedVersion: 999, // stale
	}
	_, err = w.Write(ctx, updateReq)
	assert.ErrorIs(t, err, writer.ErrConflict)

	// txid_stamp must be unchanged — the transaction was rolled back
	var txidAfter int64
	err = conn.QueryRow(ctx,
		`SELECT txid_stamp::text::bigint FROM kubernetes_resources WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'nginx'`,
	).Scan(&txidAfter)
	require.NoError(t, err)
	assert.Equal(t, txidBefore, txidAfter, "txid_stamp must not change on 409")
}
