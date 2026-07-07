package race_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R2 — Dirty-flag swallow (latency only, but test anyway).
// Write lands between poll snapshot and flag handling.
// Defense: clear-before-snapshot, recheck-after.
// Test: inject a doorbell during the snapshot window; assert a trailing poll follows.
func TestR2_DirtyFlagSwallow(t *testing.T) {
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
		BaselineInterval: 10 * time.Second, // long baseline so doorbell drives polling
		DebounceFloor:    50 * time.Millisecond,
	}, hooks)

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
	initialPolls := pollCount.Load()

	// Write a resource — doorbell fires, should trigger at least one poll
	wr := newWriter(t, nil)
	req := makeWriteReq("apps/v1/Deployment", "default", "dirty-flag-test", 1)
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Wait for doorbell-triggered poll(s)
	time.Sleep(300 * time.Millisecond)

	finalPolls := pollCount.Load()
	assert.Greater(t, finalPolls, initialPolls, "doorbell must trigger at least one poll")

	// Drain event
	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("event not delivered")
	}
}

type countingWatchHooks struct {
	pollCount *atomic.Int32
}

func (h *countingWatchHooks) BeforePoll()                { h.pollCount.Add(1) }
func (h *countingWatchHooks) AfterPoll(_ []reader.Event) {}

func connectManualShared(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	return conn
}

// Also test with high iteration count to catch timing-sensitive swallows
func TestR2_DirtyFlagSwallow_Stress(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pollConn := connectManualShared(t)

	var pollCount atomic.Int32
	hooks := &countingWatchHooks{pollCount: &pollCount}

	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 200 * time.Millisecond,
		DebounceFloor:    10 * time.Millisecond,
	}, hooks)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	// Write 20 resources rapidly — all should be delivered
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wr := newWriter(t, nil)
			req := makeWriteReq("apps/v1/Deployment", "default",
				fmt.Sprintf("stress-%d", idx), 1)
			wr.Write(ctx, req)
		}(i)
	}
	wg.Wait()

	// Collect all 20 events
	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 20 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/20 events", len(events))
		}
	}

	assert.Len(t, events, 20, "all 20 writes must be delivered")
}
