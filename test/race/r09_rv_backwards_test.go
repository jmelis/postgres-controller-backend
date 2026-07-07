package race_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
)

// R9 — RV backwards exposure (I4).
// Client presents an RV from a previous timeline epoch after failover.
// Defense: epoch comparison → 410 Gone, relist.
// Test: replay a pre-failover RV post-failover; assert rejection.
//
// Since we can't actually trigger a failover in a local PG, we simulate it:
// bump the timeline epoch in cluster_epoch, then try to start a watcher
// with the old epoch. The watcher should detect the epoch mismatch.
func TestR9_RVBackwardsExposure(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some data under epoch 1
	wr := newWriter(t, nil)
	_, err := wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"pre-failover", 1))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	// List to get RV under timeline epoch 1
	listConn := freshConn(t)
	listResult, err := reader.List(ctx, listConn, "apps/v1/Deployment", []int{1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	assert.Equal(t, int64(1), listResult.ResourceVersion.Epoch)

	// Simulate failover: bump timeline epoch to 2
	adminConn := freshConn(t)
	_, err = adminConn.Exec(ctx, `UPDATE cluster_epoch SET timeline_id = 2`)
	if err != nil {
		t.Fatalf("bump epoch: %v", err)
	}

	// Try to use the old RV (epoch=1) for a watch after "failover" (epoch=2)
	// The watcher should detect the stale epoch and return 410 Gone
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV: resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 1}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, reader.ErrGone,
			"watcher with stale epoch must get 410 Gone")
	case <-time.After(3 * time.Second):
		watchCancel()
		<-done
		t.Fatal("watcher did not reject stale epoch within 3s")
	}

	watchCancel()
	pollConn.Close(context.Background())
}
