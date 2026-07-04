package race_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R7 — Compaction vs. slow watcher (I7).
// Watcher resumes with hwm just below a freshly advanced horizon.
// Defense: horizon advanced transactionally with the delete; boundary check on poll.
// Test: freeze a watcher, compact past its hwm, resume; assert 410 Gone.
func TestR7_CompactionVsSlowWatcher(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Write some resources, some as tombstones with old timestamps
	wr := newWriter(t, nil)
	for i := 0; i < 3; i++ {
		past := time.Now().Add(-48 * time.Hour)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("compact-victim-%d", i), 1, "holder-a", epoch)
		req.DeletionTimestamp = &past
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Write a live resource at seq=4
	_, err := wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"survivor", 1, "holder-a", epoch))
	require.NoError(t, err)

	// Compact — removes the 3 tombstones, advances horizon to seq=3
	compactConn := freshConn(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Deleted)

	// Now start a watcher from hwm=1 (below the compaction horizon of 3)
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 1}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	// The watcher should return with a 410 Gone error
	select {
	case err := <-done:
		assert.ErrorIs(t, err, reader.ErrGone, "watcher must get 410 Gone when hwm < compacted_seq")
	case <-time.After(3 * time.Second):
		watchCancel()
		<-done
		t.Fatal("watcher did not return 410 Gone within 3s")
	}

	watchCancel()
	pollConn.Close(context.Background())
}

// Test the boundary case: hwm == horizon exactly must succeed
func TestR7_CompactionBoundary_Exact(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	wr := newWriter(t, nil)
	past := time.Now().Add(-48 * time.Hour)
	req := makeWriteReq("apps/v1/Deployment", "default", "compact-exact", 1, "holder-a", epoch)
	req.DeletionTimestamp = &past
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Write live at seq=2
	_, err = wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"exact-survivor", 1, "holder-a", epoch))
	require.NoError(t, err)

	// Compact: horizon at 1
	compactConn := freshConn(t)
	_, err = compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)

	// Watcher with hwm=1 (== horizon): must succeed, not 410
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 1}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	// Should receive seq=2 event, not 410
	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Equal(t, int64(2), ev.Resource.GVKBucketSeq)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for event at hwm==horizon boundary")
	}
}
