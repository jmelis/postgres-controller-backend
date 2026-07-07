package race_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R13 — Concurrent poll race (B1: I4 availability).
//
// BEFORE Phase 2: the trailing-poll goroutine spawned by debouncedPoll called
// w.poll concurrently with the main loop's baseline poll, racing on the
// unprotected hwm map and causing pgx ErrConnBusy.
//
// AFTER Phase 2 (single-goroutine scheduler): concurrent polls are impossible
// by construction — all polling happens in the main loop goroutine. This test
// verifies the structural defense by rapidly mixing doorbells and baseline
// polls under the race detector. No error should surface.
func TestR13_ConcurrentPollRace(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	var pollCount atomic.Int32
	hooks := &countingWatchHooks{pollCount: &pollCount}

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 200 * time.Millisecond,
		DebounceFloor:    50 * time.Millisecond,
	}, hooks)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	// Rapid-fire writes to generate doorbells that interleave with baseline polls
	time.Sleep(50 * time.Millisecond)
	wr := newWriter(t, nil)
	for i := 0; i < 10; i++ {
		_, err := wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
			"r13-"+string(rune('a'+i)), 1))
		require.NoError(t, err)
		time.Sleep(30 * time.Millisecond)
	}

	// Collect all 10 events
	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 10 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/10 events", len(events))
		}
	}

	watchCancel()
	watchErr := <-done
	pollConn.Close(context.Background())
	listenConn.Close(context.Background())

	assert.Len(t, events, 10, "all writes delivered")
	assert.True(t, pollCount.Load() >= 2, "multiple polls fired (baseline + doorbell)")

	if watchErr != nil && watchErr != context.Canceled {
		t.Fatalf("watcher terminated with unexpected error (B1 defense check): %v", watchErr)
	}
}
