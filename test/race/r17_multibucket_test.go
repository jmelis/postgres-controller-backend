package race_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R17 — Multi-bucket watcher: interleaved delivery, per-channel doorbell,
// and partial 410 on a single multi-bucket watcher.

func TestR17_InterleavedDelivery(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:       "apps/v1/Deployment",
		BucketIDs: []int{1, 2, 3},
		StartRV: resourceversion.RV{
			Epoch:   1,
			Buckets: map[int]int64{1: 0, 2: 0, 3: 0},
		},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	// Write 5 resources per bucket, round-robin (15 total).
	buckets := []int{1, 2, 3}
	for i := 0; i < 5; i++ {
		for _, b := range buckets {
			wr := newWriter(t, nil)
			req := makeWriteReq("apps/v1/Deployment", "default",
				fmt.Sprintf("mb-b%d-%d", b, i), b)
			_, err := wr.Write(ctx, req)
			require.NoError(t, err)
		}
	}

	// Collect all 15 events.
	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 15 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/15 events", len(events))
		}
	}

	// Group by BucketID and assert per-bucket sequences are strictly ascending.
	byBucket := make(map[int][]int64)
	for _, ev := range events {
		byBucket[ev.Resource.BucketID] = append(byBucket[ev.Resource.BucketID], ev.Resource.GVKBucketSeq)
	}

	for _, b := range buckets {
		seqs := byBucket[b]
		require.Len(t, seqs, 5, "bucket %d must have 5 events", b)
		for i := 1; i < len(seqs); i++ {
			assert.Greater(t, seqs[i], seqs[i-1],
				"bucket %d: seq %d must be > seq %d", b, seqs[i], seqs[i-1])
		}
	}

	// Assert HWM reflects 5 events per bucket.
	hwm := w.HWM()
	for _, b := range buckets {
		assert.Equal(t, int64(5), hwm[b],
			"HWM for bucket %d must be 5", b)
	}
}

func TestR17_DoorbellPerChannel(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:       "apps/v1/Deployment",
		BucketIDs: []int{1, 2, 3},
		StartRV: resourceversion.RV{
			Epoch:   1,
			Buckets: map[int]int64{1: 0, 2: 0, 3: 0},
		},
		BaselineInterval: 10 * time.Second, // long baseline — doorbell must drive delivery
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	time.Sleep(200 * time.Millisecond)

	// Write ONE resource to bucket 3 only.
	wr := newWriter(t, nil)
	req := makeWriteReq("apps/v1/Deployment", "default",
		"doorbell-b3", 3)
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Assert event arrives in <2s — proves LISTEN was setup on resource_changes_b3.
	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Equal(t, 3, ev.Resource.BucketID)
		assert.Equal(t, "doorbell-b3", ev.Resource.Name)
	case <-time.After(2 * time.Second):
		t.Fatal("event from bucket 3 not delivered within 2s — LISTEN not setup on b3 channel")
	}
}

func TestR17_Partial410(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write 3 tombstones to bucket 2.
	wr := newWriter(t, nil)
	for i := 0; i < 3; i++ {
		past := time.Now().Add(-48 * time.Hour)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("tombstone-b2-%d", i), 2)
		req.DeletionTimestamp = &past
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Backdate updated_at so GREATEST(deletion_timestamp, updated_at) is old enough to compact
	backdateConn := freshConn(t)
	_, err := backdateConn.Exec(ctx, `UPDATE kubernetes_resources SET updated_at = deletion_timestamp WHERE name LIKE 'tombstone-b2-%'`)
	require.NoError(t, err)
	backdateConn.Close(context.Background())

	// Write 1 live resource to bucket 2 (seq=4).
	_, err = wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"live-b2", 2))
	require.NoError(t, err)

	// Compact bucket 2 — removes tombstones, advances horizon.
	compactConn := freshConn(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Deleted)

	// Start watcher on {1,2,3} with StartRV for bucket 2 at 0 (below horizon).
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:       "apps/v1/Deployment",
		BucketIDs: []int{1, 2, 3},
		StartRV: resourceversion.RV{
			Epoch:   1,
			Buckets: map[int]int64{1: 0, 2: 0, 3: 0},
		},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	// Assert Run returns reader.ErrGone within 3s.
	select {
	case err := <-done:
		assert.ErrorIs(t, err, reader.ErrGone,
			"watcher must get 410 Gone when bucket 2 hwm < compacted horizon")
		assert.True(t, strings.Contains(err.Error(), "bucket 2") ||
			strings.Contains(err.Error(), "2"),
			"error should reference bucket 2, got: %s", err.Error())
	case <-time.After(3 * time.Second):
		watchCancel()
		<-done
		t.Fatal("watcher did not return 410 Gone within 3s")
	}

	watchCancel()
	pollConn.Close(context.Background())
}
