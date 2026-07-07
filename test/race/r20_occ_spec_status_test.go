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

// R20 — OCC concurrent spec and status writes on the same object.
// A spec update and a status update race with the same ExpectedVersion.
// Both bump object_version, so the loser gets ErrConflict regardless of
// which domain (spec vs status) it targets.
func TestR20_ConcurrentSpecStatusRace(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	w0 := newWriter(t, nil)
	createReq := makeWriteReq("apps/v1/Deployment", "default", "r20", 1)
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

	specUpdate := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "r20",
		BucketID: 1, ExpectedVersion: 1,
		Spec: json.RawMessage(`{"replicas":5}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}
	statusUpdate := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "r20",
		BucketID: 1, ExpectedVersion: 1,
		Status: json.RawMessage(`{"ready":true}`),
	}

	go func() {
		r, e := wA.Write(ctx, specUpdate)
		chA <- outcome{r, e}
	}()

	<-hookA.ready

	go func() {
		r, e := wB.WriteStatus(ctx, statusUpdate)
		chB <- outcome{r, e}
	}()

	close(hookA.proceed)

	resA := <-chA
	resB := <-chB

	require.NoError(t, resA.err, "spec writer must succeed")
	assert.True(t, resA.result.Changed)
	assert.Equal(t, int64(2), resA.result.ObjectVersion)

	assert.ErrorIs(t, resB.err, writer.ErrConflict, "status writer must get conflict")

	// Counter advanced once for create, once for spec update. Status rollback.
	conn := freshConn(t)
	var counter int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counter)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counter)

	// DB must have updated spec but original status.
	var spec, status json.RawMessage
	err = conn.QueryRow(ctx,
		`SELECT spec, status FROM kubernetes_resources
		 WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'r20'`,
	).Scan(&spec, &status)
	require.NoError(t, err)
	assert.JSONEq(t, `{"replicas":5}`, string(spec))
	assert.JSONEq(t, `{}`, string(status))
}
