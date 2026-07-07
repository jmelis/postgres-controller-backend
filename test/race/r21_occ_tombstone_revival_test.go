package race_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R21 — OCC concurrent tombstone revival.
// Two writers simultaneously attempt to create a resource (ExpectedVersion=0)
// when a tombstone already exists. The first revives it; the second gets
// ErrAlreadyExists because deletion_timestamp is NULL after A's commit.
func TestR21_ConcurrentTombstoneRevival(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Create a tombstone: resource with DeletionTimestamp set, no finalizers.
	w0 := newWriter(t, nil)
	past := time.Now().Add(-time.Hour)
	tombstoneReq := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "r21",
		BucketID:          1,
		Spec:              json.RawMessage(`{"old":true}`),
		Status:            json.RawMessage(`{}`),
		Metadata:          json.RawMessage(`{}`),
		DeletionTimestamp: &past,
	}
	res0, err := w0.Write(ctx, tombstoneReq)
	require.NoError(t, err)
	oldUID := res0.UID

	hookA := newBlockingHook()
	wA := newWriter(t, hookA)
	wB := newWriter(t, nil)

	type outcome struct {
		result model.WriteResult
		err    error
	}
	chA := make(chan outcome, 1)
	chB := make(chan outcome, 1)

	revivalReq := func(who string) model.WriteRequest {
		return model.WriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default", Name: "r21",
			BucketID:        1,
			ExpectedVersion: 0,
			Spec:            json.RawMessage(`{"writer":"` + who + `"}`),
			Status:          json.RawMessage(`{}`),
			Metadata:        json.RawMessage(`{}`),
		}
	}

	go func() {
		r, e := wA.Write(ctx, revivalReq("A"))
		chA <- outcome{r, e}
	}()

	<-hookA.ready

	go func() {
		r, e := wB.Write(ctx, revivalReq("B"))
		chB <- outcome{r, e}
	}()

	close(hookA.proceed)

	resA := <-chA
	resB := <-chB

	require.NoError(t, resA.err, "first reviver must succeed")
	assert.Equal(t, int64(1), resA.result.ObjectVersion)
	assert.NotEqual(t, oldUID, resA.result.UID, "revival must assign a new UID")

	assert.ErrorIs(t, resB.err, writer.ErrAlreadyExists, "second reviver must get already-exists")

	// Counter: create=1, A's revival=2. B's increment rolled back.
	conn := freshConn(t)
	var counter int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counter)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counter)

	// DB must have exactly one row, live (no deletion_timestamp), with A's content.
	var rowCount int
	err = conn.QueryRow(ctx,
		`SELECT count(*) FROM kubernetes_resources
		 WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'r21'`,
	).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 1, rowCount)

	var spec json.RawMessage
	var delTS *time.Time
	err = conn.QueryRow(ctx,
		`SELECT spec, deletion_timestamp FROM kubernetes_resources
		 WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'r21'`,
	).Scan(&spec, &delTS)
	require.NoError(t, err)
	assert.Nil(t, delTS, "revived resource must not have deletion_timestamp")
	assert.JSONEq(t, `{"writer":"A"}`, string(spec))
}
