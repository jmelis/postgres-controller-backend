package loadtest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pollTiming struct {
	mu        sync.Mutex
	durations []time.Duration
}

// Phase 5 — Poll cost & delivery latency (DESIGN.md §7).
// 2000 seeded resources, 10 watchers.
// Criteria:
//   - Poll-cycle p99 ≤ 50ms (strict mode)
//   - Healthy-doorbell write→delivery p99 ≤ 500ms (strict mode)
//   - Notify-loss drill: every event arrives, p99 ≤ 2×baseline + 500ms
//   - Verifier silent (zero invariant violations)
func TestPhase5_PollCostAndDeliveryLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in short mode")
	}

	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const (
		numSeed        = 2000
		numWatchers    = 10
		idleDuration   = 10 * time.Second
		writeCount     = 100
		gvk            = "apps/v1/Deployment"
		baselineForAll = 500 * time.Millisecond
	)

	// Seed 2000 resources
	seedConn := manualConn(t)
	seedWriter := writer.New(seedConn, nil)
	for i := range numSeed {
		req := model.WriteRequest{
			GVK: gvk, Namespace: "phase5-seed",
			Name:     fmt.Sprintf("seed-%d", i),
			Spec:     json.RawMessage(`{"seed":true}`),
			Status:   json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`),
		}
		_, err := seedWriter.Write(ctx, req)
		require.NoError(t, err)
	}
	seedConn.Close(context.Background())
	t.Logf("seeded %d resources", numSeed)

	// Get current HWM from a quick watcher start
	probeConn := manualConn(t)
	probeWatcher := reader.NewWatcher(probeConn, nil, reader.WatcherConfig{
		GVK:              gvk,
		StartRV:          resourceversion.RV{},
		BaselineInterval: 100 * time.Millisecond,
	}, nil)
	probeCtx, probeCancel := context.WithCancel(ctx)
	probeDone := make(chan error, 1)
	go func() { probeDone <- probeWatcher.Run(probeCtx) }()
	// Wait for all seeds to be delivered
	seedDeadline := time.After(15 * time.Second)
	delivered := 0
	for delivered < numSeed {
		select {
		case _, ok := <-probeWatcher.Events():
			if !ok {
				t.Fatalf("probe watcher closed early after %d events", delivered)
			}
			delivered++
		case <-seedDeadline:
			t.Fatalf("probe watcher timeout: %d/%d events", delivered, numSeed)
		}
	}
	seedHWM := probeWatcher.HWM()
	probeCancel()
	<-probeDone
	probeConn.Close(context.Background())
	t.Logf("seed HWM: %v", seedHWM)

	// ============================================
	// Phase A: idle poll cost (no writes occurring)
	// ============================================
	t.Log("--- Phase A: idle poll cost ---")

	var idleTimings [numWatchers]*pollTiming
	idleWatchers := make([]*reader.Watcher, numWatchers)
	idleDones := make([]chan error, numWatchers)
	idleConns := make([]*pgx.Conn, numWatchers)

	idleCtx, idleCancel := context.WithCancel(ctx)
	for i := range numWatchers {
		pc := manualConn(t)
		idleConns[i] = pc
		timing := &pollTiming{}
		idleTimings[i] = timing

		hooks := &phase5TimingHooks{timing: timing}
		w := reader.NewWatcher(pc, nil, reader.WatcherConfig{
			GVK:              gvk,
			StartRV:          resourceversion.RV{Watermark: seedHWM},
			BaselineInterval: baselineForAll,
		}, hooks)
		idleWatchers[i] = w
		done := make(chan error, 1)
		idleDones[i] = done
		go func() { done <- w.Run(idleCtx) }()
	}

	time.Sleep(idleDuration)
	idleCancel()
	for i := range numWatchers {
		<-idleDones[i]
		idleConns[i].Close(context.Background())
	}

	// Aggregate idle poll durations
	var allIdleDurations []time.Duration
	for _, timing := range idleTimings {
		timing.mu.Lock()
		allIdleDurations = append(allIdleDurations, timing.durations...)
		timing.mu.Unlock()
	}
	sort.Slice(allIdleDurations, func(i, j int) bool { return allIdleDurations[i] < allIdleDurations[j] })

	var idleP50, idleP99 time.Duration
	if len(allIdleDurations) > 0 {
		idleP50 = percentile(allIdleDurations, 0.50)
		idleP99 = percentile(allIdleDurations, 0.99)
	}

	t.Logf("idle poll cycles: %d", len(allIdleDurations))
	t.Logf("idle poll p50:    %v", idleP50)
	t.Logf("idle poll p99:    %v", idleP99)

	if os.Getenv("PGCTL_STRICT_LATENCY") == "1" {
		assert.LessOrEqual(t, idleP99.Milliseconds(), int64(50),
			"idle poll p99 must be ≤50ms (got %v)", idleP99)
	} else {
		t.Logf("NOTE: idle poll p99=%v — strict latency gate skipped (set PGCTL_STRICT_LATENCY=1)", idleP99)
	}

	// ============================================
	// Phase B: write→delivery latency (healthy doorbell)
	// ============================================
	t.Log("--- Phase B: write→delivery with doorbell ---")

	// Start verifier
	verifyConn := manualConn(t)
	canaryConn := freshConn(t)
	ver := verifier.New(verifyConn, canaryConn, verifier.Config{
		GVK:            gvk,
		PollInterval:   200 * time.Millisecond,
		CanaryInterval: 500 * time.Millisecond,
	})
	verCtx, verCancel := context.WithCancel(ctx)
	verDone := make(chan error, 1)
	go func() { verDone <- ver.Run(verCtx) }()

	// Start watchers with listen conns
	type deliveryRecord struct {
		writeTime time.Time
		recvTime  time.Time
	}

	var deliveryMu sync.Mutex
	var deliveries []deliveryRecord

	dbWatchers := make([]*reader.Watcher, numWatchers)
	dbDones := make([]chan error, numWatchers)
	dbPollConns := make([]*pgx.Conn, numWatchers)
	dbListenConns := make([]*pgx.Conn, numWatchers)

	dbCtx, dbCancel := context.WithCancel(ctx)
	for i := range numWatchers {
		pc := manualConn(t)
		lc := manualConn(t)
		dbPollConns[i] = pc
		dbListenConns[i] = lc
		w := reader.NewWatcher(pc, lc, reader.WatcherConfig{
			GVK:              gvk,
			StartRV:          resourceversion.RV{Watermark: seedHWM},
			BaselineInterval: 10 * time.Second,
			DebounceFloor:    50 * time.Millisecond,
		}, nil)
		dbWatchers[i] = w
		done := make(chan error, 1)
		dbDones[i] = done
		go func() { done <- w.Run(dbCtx) }()
	}

	// Let watchers settle
	time.Sleep(200 * time.Millisecond)

	// Start collectors BEFORE writes so events are consumed as they arrive
	writeTimes := make(map[string]time.Time)
	var writeTimesMu sync.Mutex
	var collectWg sync.WaitGroup
	for i := range numWatchers {
		collectWg.Add(1)
		go func(idx int) {
			defer collectWg.Done()
			collected := 0
			deadline := time.After(30 * time.Second)
			for collected < writeCount {
				select {
				case ev, ok := <-dbWatchers[idx].Events():
					if !ok {
						return
					}
					if ev.Resource.Namespace == "phase5-db" {
						recvTime := time.Now()
						writeTimesMu.Lock()
						wt, exists := writeTimes[ev.Resource.Name]
						writeTimesMu.Unlock()
						if exists {
							deliveryMu.Lock()
							deliveries = append(deliveries, deliveryRecord{writeTime: wt, recvTime: recvTime})
							deliveryMu.Unlock()
						}
						collected++
					}
				case <-deadline:
					return
				}
			}
		}(i)
	}

	// Write 100 resources at ~20/s, record write timestamps
	writeConn := manualConn(t)
	wr := writer.New(writeConn, nil)
	for i := range writeCount {
		name := fmt.Sprintf("phase5-db-%d", i)
		req := model.WriteRequest{
			GVK: gvk, Namespace: "phase5-db", Name: name,
			Spec:     json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			Status:   json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`),
		}
		writeTimesMu.Lock()
		writeTimes[name] = time.Now()
		writeTimesMu.Unlock()
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond) // ~20/s
	}
	writeConn.Close(context.Background())
	collectWg.Wait()

	dbCancel()
	for i := range numWatchers {
		<-dbDones[i]
		dbPollConns[i].Close(context.Background())
		dbListenConns[i].Close(context.Background())
	}

	// Compute delivery latencies
	var dbLatencies []time.Duration
	for _, d := range deliveries {
		dbLatencies = append(dbLatencies, d.recvTime.Sub(d.writeTime))
	}
	sort.Slice(dbLatencies, func(i, j int) bool { return dbLatencies[i] < dbLatencies[j] })

	var dbP50, dbP99 time.Duration
	if len(dbLatencies) > 0 {
		dbP50 = percentile(dbLatencies, 0.50)
		dbP99 = percentile(dbLatencies, 0.99)
	}

	t.Logf("doorbell deliveries: %d (expected %d)", len(dbLatencies), writeCount*numWatchers)
	t.Logf("doorbell p50:        %v", dbP50)
	t.Logf("doorbell p99:        %v", dbP99)

	if os.Getenv("PGCTL_STRICT_LATENCY") == "1" {
		assert.LessOrEqual(t, dbP99.Milliseconds(), int64(500),
			"doorbell delivery p99 must be ≤500ms (got %v)", dbP99)
	} else {
		t.Logf("NOTE: doorbell p99=%v — strict latency gate skipped", dbP99)
	}

	// ============================================
	// Phase C: notify-loss drill (nil listen conns)
	// ============================================
	t.Log("--- Phase C: notify-loss drill ---")

	const lossBaseline = 1 * time.Second

	// Get current HWM after phase B writes
	var phaseC_HWM uint64
	for _, w := range dbWatchers {
		if hwm := w.HWM(); hwm > phaseC_HWM {
			phaseC_HWM = hwm
		}
	}

	var lossDeliveries []deliveryRecord
	var lossDeliveryMu sync.Mutex

	lossWatchers := make([]*reader.Watcher, numWatchers)
	lossDones := make([]chan error, numWatchers)
	lossConns := make([]*pgx.Conn, numWatchers)

	lossCtx, lossCancel := context.WithCancel(ctx)
	for i := range numWatchers {
		pc := manualConn(t)
		lossConns[i] = pc
		w := reader.NewWatcher(pc, nil, reader.WatcherConfig{
			GVK:              gvk,
			StartRV:          resourceversion.RV{Watermark: phaseC_HWM},
			BaselineInterval: lossBaseline,
		}, nil)
		lossWatchers[i] = w
		done := make(chan error, 1)
		lossDones[i] = done
		go func() { done <- w.Run(lossCtx) }()
	}

	time.Sleep(200 * time.Millisecond)

	// Start collectors BEFORE writes
	lossWriteTimes := make(map[string]time.Time)
	var lossWriteTimesMu sync.Mutex
	var lossCollectWg sync.WaitGroup
	var totalLossDelivered atomic.Int64
	for i := range numWatchers {
		lossCollectWg.Add(1)
		go func(idx int) {
			defer lossCollectWg.Done()
			collected := 0
			deadline := time.After(30 * time.Second)
			for collected < writeCount {
				select {
				case ev, ok := <-lossWatchers[idx].Events():
					if !ok {
						return
					}
					if ev.Resource.Namespace == "phase5-loss" {
						recvTime := time.Now()
						lossWriteTimesMu.Lock()
						wt, exists := lossWriteTimes[ev.Resource.Name]
						lossWriteTimesMu.Unlock()
						if exists {
							lossDeliveryMu.Lock()
							lossDeliveries = append(lossDeliveries, deliveryRecord{writeTime: wt, recvTime: recvTime})
							lossDeliveryMu.Unlock()
						}
						collected++
						totalLossDelivered.Add(1)
					}
				case <-deadline:
					return
				}
			}
		}(i)
	}

	// Write 100 more resources
	lossWriteConn := manualConn(t)
	lossWr := writer.New(lossWriteConn, nil)
	for i := range writeCount {
		name := fmt.Sprintf("phase5-loss-%d", i)
		req := model.WriteRequest{
			GVK: gvk, Namespace: "phase5-loss", Name: name,
			Spec:     json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			Status:   json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`),
		}
		lossWriteTimesMu.Lock()
		lossWriteTimes[name] = time.Now()
		lossWriteTimesMu.Unlock()
		_, err := lossWr.Write(ctx, req)
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}
	lossWriteConn.Close(context.Background())
	lossCollectWg.Wait()

	lossCancel()
	for i := range numWatchers {
		<-lossDones[i]
		lossConns[i].Close(context.Background())
	}

	var lossLatencies []time.Duration
	for _, d := range lossDeliveries {
		lossLatencies = append(lossLatencies, d.recvTime.Sub(d.writeTime))
	}
	sort.Slice(lossLatencies, func(i, j int) bool { return lossLatencies[i] < lossLatencies[j] })

	var lossP50, lossP99 time.Duration
	if len(lossLatencies) > 0 {
		lossP50 = percentile(lossLatencies, 0.50)
		lossP99 = percentile(lossLatencies, 0.99)
	}

	t.Logf("loss-drill deliveries: %d (expected %d)", totalLossDelivered.Load(), int64(writeCount)*int64(numWatchers))
	t.Logf("loss-drill p50:        %v", lossP50)
	t.Logf("loss-drill p99:        %v", lossP99)

	// Hard assert: every event arrives
	assert.Equal(t, int64(writeCount)*int64(numWatchers), totalLossDelivered.Load(),
		"every event must arrive even without doorbell")

	// Hard assert: p99 ≤ 2×baseline + 500ms
	maxAcceptable := time.Duration(float64(lossBaseline)*2) + 500*time.Millisecond
	assert.LessOrEqual(t, lossP99, maxAcceptable,
		"loss-drill p99 must be ≤ %v (got %v)", maxAcceptable, lossP99)

	// Verifier check
	time.Sleep(2 * time.Second)
	verCancel()
	<-verDone
	verifyConn.Close(context.Background())

	verResult := ver.Result()
	t.Logf("=== Phase 5 Results ===")
	t.Logf("Verifier events:    %d", verResult.EventsChecked)
	t.Logf("Verifier violations: %d", len(verResult.Violations))
	for _, v := range verResult.Violations {
		t.Logf("  VIOLATION: %s", v)
	}
	assert.Empty(t, verResult.Violations, "verifier must report zero violations")
}

// phase5TimingHooks records poll durations.
type phase5TimingHooks struct {
	timing *pollTiming
	start  time.Time
}

func (h *phase5TimingHooks) BeforePoll() {
	h.start = time.Now()
}

func (h *phase5TimingHooks) AfterPoll(_ []reader.Event) {
	dur := time.Since(h.start)
	h.timing.mu.Lock()
	h.timing.durations = append(h.timing.durations, dur)
	h.timing.mu.Unlock()
}
