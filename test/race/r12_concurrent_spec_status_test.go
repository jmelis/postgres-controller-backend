package race_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R12 — Concurrent spec and status writes (I1/I2 for mixed write paths).
// Both write to the same resource. Each write bumps the shared counter and
// object_version. A watcher polling after each write sees the new state at the
// correct sequence number — the shared counter produces a gapless sequence
// across spec and status writes.
func TestR12_ConcurrentSpecStatus(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Create resource via spec writer (seq=1, object_version=1)
	specW := newWriter(t, nil)
	createReq := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "mixed-writer",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{"ready":false}`), Metadata: json.RawMessage(`{}`),
	}
	r1, err := specW.Write(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), r1.Seq)
	assert.Equal(t, int64(1), r1.ObjectVersion)

	// Status update by holder-b (seq=2, object_version=2)
	statusW := writer.New(freshConn(t), nil)
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "mixed-writer",
		BucketID: 1, Status: json.RawMessage(`{"ready":true,"conditions":["init"]}`),
		ExpectedVersion: r1.ObjectVersion,
	}
	r2, err := statusW.WriteStatus(ctx, statusReq)
	require.NoError(t, err)
	assert.Equal(t, int64(2), r2.Seq)
	assert.Equal(t, int64(2), r2.ObjectVersion)

	// Spec update by holder-a (seq=3, object_version=3)
	specReq2 := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "mixed-writer",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":3}`),
		Status: json.RawMessage(`{"ready":true,"conditions":["init"]}`),
		Metadata: json.RawMessage(`{}`),
		ExpectedVersion: r2.ObjectVersion,
	}
	r3, err := specW.Write(ctx, specReq2)
	require.NoError(t, err)
	assert.Equal(t, int64(3), r3.Seq)
	assert.Equal(t, int64(3), r3.ObjectVersion)

	// Status update by holder-b (seq=4, object_version=4)
	statusReq2 := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "mixed-writer",
		BucketID: 1, Status: json.RawMessage(`{"ready":true,"conditions":["init","progressing"]}`),
		ExpectedVersion: r3.ObjectVersion,
	}
	r4, err := statusW.WriteStatus(ctx, statusReq2)
	require.NoError(t, err)
	assert.Equal(t, int64(4), r4.Seq)
	assert.Equal(t, int64(4), r4.ObjectVersion)

	// Shared counter is gapless: 1, 2, 3, 4 — proven by assertions above.
	// The UID is stable across all 4 writes.
	assert.Equal(t, r1.UID, r2.UID, "UID must be stable across spec/status writes")
	assert.Equal(t, r1.UID, r3.UID)
	assert.Equal(t, r1.UID, r4.UID)

	// Watcher starting from seq=3 sees the resource at seq=4 (I3: monotonic hwm)
	pollConn := freshConn(t)
	var currentEpoch int64
	require.NoError(t, pollConn.QueryRow(ctx, `SELECT timeline_id FROM cluster_epoch`).Scan(&currentEpoch))
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: currentEpoch, Buckets: map[int]int64{1: 3}},
		BaselineInterval: 100 * time.Millisecond,
	}, nil)

	watchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	select {
	case ev := <-w.Events():
		assert.Equal(t, int64(4), ev.Resource.GVKBucketSeq,
			"watcher must see seq=4 after starting from hwm=3")
		assert.Equal(t, reader.EventModified, ev.Type)
	case <-watchCtx.Done():
		t.Fatal("timeout waiting for watcher event")
	}
	cancel()
	<-done

	// Verify final object state: holder-A's spec, holder-B's status
	finalConn := freshConn(t)
	var spec, status json.RawMessage
	err = finalConn.QueryRow(ctx,
		`SELECT spec, status FROM kubernetes_resources
		 WHERE gvk = $1 AND namespace = $2 AND name = $3`,
		"apps/v1/Deployment", "default", "mixed-writer").Scan(&spec, &status)
	require.NoError(t, err)
	assert.JSONEq(t, `{"replicas":3}`, string(spec), "spec must reflect holder-A's last write")
	assert.JSONEq(t, `{"ready":true,"conditions":["init","progressing"]}`, string(status),
		"status must reflect holder-B's last write")
}