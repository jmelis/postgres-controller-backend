package race_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R16 — Debounce suppression: the debouncer must coalesce a burst of doorbells
// into at most 2 polls (leading + trailing edge), with no event loss.

func TestR16_DebounceSuppression(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	var pollCount atomic.Int32
	hooks := &countingWatchHooks{pollCount: &pollCount}

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 10 * time.Second, // long baseline so doorbells drive polling
		DebounceFloor:    200 * time.Millisecond,
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

	// Wait for the initial poll to settle.
	time.Sleep(200 * time.Millisecond)
	initialPolls := pollCount.Load()

	// Fire 30 doorbells in <100ms using a separate connection.
	notifyConn := connectManualShared(t)
	defer notifyConn.Close(context.Background())
	for i := 0; i < 30; i++ {
		_, err := notifyConn.Exec(ctx, `SELECT pg_notify('resource_changes_b1', '')`)
		require.NoError(t, err)
	}

	// Wait for the trailing edge to fire (debounce floor + margin).
	time.Sleep(1 * time.Second)

	extraPolls := pollCount.Load() - initialPolls
	assert.GreaterOrEqual(t, extraPolls, int32(1),
		"burst must trigger at least 1 poll (leading edge)")
	assert.LessOrEqual(t, extraPolls, int32(2),
		"burst must trigger at most 2 polls (leading + trailing edge)")

	// Write one real resource AFTER the burst to verify delivery survived suppression.
	wr := newWriter(t, nil)
	req := makeWriteReq("apps/v1/Deployment", "default", "debounce-survivor", 1, "holder-a", epoch)
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Equal(t, "debounce-survivor", ev.Resource.Name)
	case <-time.After(2 * time.Second):
		t.Fatal("event not delivered after doorbell burst — delivery guarantee violated")
	}
}

func TestR16_TrailingEdgeTiming(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	setupLease(t, 1, "holder-a", 60_000_000_000)

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	// Record poll timestamps under a mutex.
	var mu sync.Mutex
	var pollTimestamps []time.Time
	hooks := &timestampWatchHooks{
		mu:             &mu,
		pollTimestamps: &pollTimestamps,
	}

	const debounceFloor = 200 * time.Millisecond

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 10 * time.Second,
		DebounceFloor:    debounceFloor,
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

	// Wait for initial poll to complete.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pollTimestamps) >= 1
	}, 3*time.Second, 10*time.Millisecond, "initial poll must fire")

	// Record baseline count after initial poll.
	mu.Lock()
	baseCount := len(pollTimestamps)
	mu.Unlock()

	// Fire one doorbell — triggers leading-edge poll.
	notifyConn := connectManualShared(t)
	defer notifyConn.Close(context.Background())
	_, err := notifyConn.Exec(ctx, `SELECT pg_notify('resource_changes_b1', '')`)
	require.NoError(t, err)

	// Wait for the leading poll to register.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pollTimestamps) >= baseCount+1
	}, 3*time.Second, 10*time.Millisecond, "leading-edge poll must fire")

	mu.Lock()
	leadingPollTime := pollTimestamps[len(pollTimestamps)-1]
	leadingCount := len(pollTimestamps)
	mu.Unlock()

	// Fire a second doorbell immediately after the leading poll is detected.
	_, err = notifyConn.Exec(ctx, `SELECT pg_notify('resource_changes_b1', '')`)
	require.NoError(t, err)

	// Wait for the trailing poll.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pollTimestamps) >= leadingCount+1
	}, 3*time.Second, 10*time.Millisecond, "trailing-edge poll must fire")

	mu.Lock()
	trailingPollTime := pollTimestamps[len(pollTimestamps)-1]
	mu.Unlock()

	gap := trailingPollTime.Sub(leadingPollTime)
	assert.GreaterOrEqual(t, gap, debounceFloor,
		"trailing poll must be at least DebounceFloor after leading poll")
	assert.LessOrEqual(t, gap, debounceFloor+200*time.Millisecond,
		"trailing poll must be within generous bound of DebounceFloor")
}

// timestampWatchHooks records the timestamp of each poll under a mutex.
type timestampWatchHooks struct {
	mu             *sync.Mutex
	pollTimestamps *[]time.Time
}

func (h *timestampWatchHooks) BeforePoll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.pollTimestamps = append(*h.pollTimestamps, time.Now())
}

func (h *timestampWatchHooks) AfterPoll(_ []reader.Event) {}

// Stress variant: high-volume doorbell burst must not lose any written events.
func TestR16_DebounceSuppression_NoEventLoss(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 10 * time.Second,
		DebounceFloor:    200 * time.Millisecond,
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

	// Write 10 resources — each write also rings the doorbell.
	for i := 0; i < 10; i++ {
		wr := newWriter(t, nil)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("debounce-stress-%d", i), 1, "holder-a", epoch)
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Collect all 10 events.
	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 10 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/10 events — debounce suppressed delivery", len(events))
		}
	}

	assert.Len(t, events, 10, "all 10 writes must be delivered despite debounce suppression")
}
