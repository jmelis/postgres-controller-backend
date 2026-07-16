package race_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDoorbell_DeliveryLatency measures end-to-end latency from writer.Write()
// returning to the event arriving on watcher.Events(). The full pipeline:
// Write() commit → doorbell.Ring() → debouncer tick (≤50ms) → pg_notify →
// watcher LISTEN wakes → debounce floor → poll() → Events().
func TestDoorbell_DeliveryLatency(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		StartRV:          resourceversion.RV{Watermark: 0},
		BaselineInterval: 10 * time.Second,
		DebounceFloor:    50 * time.Millisecond,
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

	const numWrites = 50

	type arrival struct {
		name    string
		arrived time.Time
	}
	arrivals := make(chan arrival, numWrites)
	go func() {
		for {
			select {
			case ev, ok := <-w.Events():
				if !ok {
					return
				}
				arrivals <- arrival{name: ev.Resource.Name, arrived: time.Now()}
			case <-ctx.Done():
				return
			}
		}
	}()

	writeTimes := make(map[string]time.Time, numWrites)
	wr := newWriter(t, nil)
	for i := range numWrites {
		name := fmt.Sprintf("latency-%d", i)
		req := makeWriteReq("apps/v1/Deployment", "default", name)
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
		writeTimes[name] = time.Now()
		time.Sleep(20 * time.Millisecond)
	}

	var latencies []time.Duration
	deadline := time.After(10 * time.Second)
	for len(latencies) < numWrites {
		select {
		case a := <-arrivals:
			if wt, ok := writeTimes[a.name]; ok {
				latencies = append(latencies, a.arrived.Sub(wt))
			}
		case <-deadline:
			t.Fatalf("timeout: got %d/%d events", len(latencies), numWrites)
		}
	}

	slices.SortFunc(latencies, func(a, b time.Duration) int { return int(a - b) })

	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	mean := sum / time.Duration(len(latencies))

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	max := latencies[len(latencies)-1]

	t.Logf("=== Doorbell Delivery Latency (debouncer=50ms, floor=50ms) ===")
	t.Logf("  writes:  %d", numWrites)
	t.Logf("  mean:    %v", mean)
	t.Logf("  p50:     %v", p50)
	t.Logf("  p95:     %v", p95)
	t.Logf("  p99:     %v", p99)
	t.Logf("  max:     %v", max)

	assert.Len(t, latencies, numWrites, "all events must be delivered")
	assert.Less(t, p99, 500*time.Millisecond,
		"p99 delivery latency should be under 500ms with 50ms debouncer + 50ms floor")
}
