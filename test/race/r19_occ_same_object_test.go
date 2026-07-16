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

// R19 — OCC concurrent update on the same object.
// Two writers race to update the same resource with the same ExpectedVersion.
// The row-level lock serializes them: A commits first, B's upsert finds a
// stale object_version and gets ErrConflict. B's transaction rolls back, so
// no txid_stamp is assigned.
func TestR19_ConcurrentUpdateSameObject(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	w0 := newWriter(t, nil)
	createReq := makeWriteReq("apps/v1/Deployment", "default", "r19")
	res0, err := w0.Write(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, int64(1), res0.ObjectVersion)

	hookA := newBlockingHook()
	wA := newWriter(t, hookA)
	wB := newWriter(t, nil)

	type outcome struct {
		result model.WriteResult
		err    error
	}
	chA := make(chan outcome, 1)
	chB := make(chan outcome, 1)

	updateA := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "r19",
		ExpectedVersion: 1,
		Spec: json.RawMessage(`{"writer":"A"}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}
	updateB := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "r19",
		ExpectedVersion: 1,
		Spec: json.RawMessage(`{"writer":"B"}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}

	go func() {
		r, e := wA.Write(ctx, updateA)
		chA <- outcome{r, e}
	}()

	// Wait for A to reach BeforeCommit (it holds the row lock).
	<-hookA.ready

	// B starts — it will block on the row lock until A commits.
	go func() {
		r, e := wB.Write(ctx, updateB)
		chB <- outcome{r, e}
	}()

	// Unblock A — it commits, releasing the lock.
	close(hookA.proceed)

	resA := <-chA
	resB := <-chB

	require.NoError(t, resA.err, "writer A must succeed")
	assert.True(t, resA.result.Changed)
	assert.Equal(t, int64(2), resA.result.ObjectVersion)

	assert.ErrorIs(t, resB.err, writer.ErrConflict, "writer B must get conflict")

	// txid_stamp must reflect A's update only. B's transaction rolled back.
	conn := freshConn(t)
	var txidStamp int64
	err = conn.QueryRow(ctx,
		`SELECT txid_stamp::text::bigint FROM kubernetes_resources
		 WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'r19'`,
	).Scan(&txidStamp)
	require.NoError(t, err)
	assert.Equal(t, int64(resA.result.Txid), txidStamp, "txid_stamp must match A's txid")

	// DB must have A's content.
	var spec json.RawMessage
	var version int64
	err = conn.QueryRow(ctx,
		`SELECT spec, object_version FROM kubernetes_resources
		 WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'r19'`,
	).Scan(&spec, &version)
	require.NoError(t, err)
	assert.Equal(t, int64(2), version)
	assert.JSONEq(t, `{"writer":"A"}`, string(spec))
}
