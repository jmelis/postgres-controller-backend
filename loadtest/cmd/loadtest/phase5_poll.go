package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase5Name = "phase5_poll"

// phase5TimingHooks records poll durations.
type phase5TimingHooks struct {
	mu        sync.Mutex
	durations []time.Duration
	start     time.Time
}

func (h *phase5TimingHooks) BeforePoll() {
	h.start = time.Now()
}

func (h *phase5TimingHooks) AfterPoll(_ []reader.Event) {
	dur := time.Since(h.start)
	h.mu.Lock()
	h.durations = append(h.durations, dur)
	h.mu.Unlock()
}

// RunPhase5 runs the poll cost and delivery latency test.
//
// Phase A: idle poll cost (no writes, measure poll cycle duration)
// Phase B: doorbell delivery latency (writes at configured rate, measure write-to-delivery)
// Phase C: notify-loss drill (no listen conn, baseline-only delivery)
func RunPhase5(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase5Poll
	numWatchers := pCfg.NumWatchers
	writeRate := pCfg.WriteRate
	writeCount := pCfg.WriteCount
	baselineInterval := cfg.Cluster.BaselinePollInterval

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	if numWatchers <= 0 {
		numWatchers = 10
	}
	if writeRate <= 0 {
		writeRate = 20
	}
	if writeCount <= 0 {
		writeCount = 100
	}

	log.Printf("phase5: starting poll test — %d watchers, %d writes at %d/s",
		numWatchers, writeCount, writeRate)

	// Get starting HWM: use a probe watcher to discover the current sequence position.
	probeConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase5: probe conn: %w", err)
	}
	defer probeConn.Close(context.Background())

	var startRV resourceversion.RV

	// Build initial HWM from a list query.
	listResult, err := reader.List(ctx, probeConn, gvk)
	if err != nil {
		log.Printf("phase5: list for HWM failed (starting from zero): %v", err)
	} else {
		startRV = listResult.ResourceVersion
	}

	start := time.Now()
	var allErrors int64

	// ========================================================
	// Phase A: idle poll cost (no writes occurring)
	// ========================================================
	log.Printf("phase5: --- Phase A: idle poll cost ---")
	const idleDuration = 10 * time.Second

	idleTimings := make([]*phase5TimingHooks, numWatchers)
	idleCtx, idleCancel := context.WithCancel(ctx)
	idleDones := make([]chan error, numWatchers)
	idleConns := make([]*pgx.Conn, numWatchers)

	for i := 0; i < numWatchers; i++ {
		pc, err := pgx.Connect(ctx, dsn)
		if err != nil {
			idleCancel()
			return nil, fmt.Errorf("phase5: idle watcher conn %d: %w", i, err)
		}
		idleConns[i] = pc

		hooks := &phase5TimingHooks{}
		idleTimings[i] = hooks

		w := reader.NewWatcher(pc, nil, reader.WatcherConfig{
			GVK:              gvk,
			StartRV:          startRV,
			BaselineInterval: baselineInterval,
		}, hooks).WithMetrics(libWatcherMetrics)

		done := make(chan error, 1)
		idleDones[i] = done
		go func(watcher *reader.Watcher, d chan error) { d <- watcher.Run(idleCtx) }(w, done)
	}

	time.Sleep(idleDuration)
	idleCancel()
	for i := 0; i < numWatchers; i++ {
		<-idleDones[i]
		idleConns[i].Close(context.Background())
	}

	// Aggregate idle poll durations.
	var allIdleDurations []time.Duration
	for _, t := range idleTimings {
		t.mu.Lock()
		allIdleDurations = append(allIdleDurations, t.durations...)
		t.mu.Unlock()
	}
	sort.Slice(allIdleDurations, func(i, j int) bool { return allIdleDurations[i] < allIdleDurations[j] })

	var idleP50, idleP99 time.Duration
	if len(allIdleDurations) > 0 {
		idleP50 = percentile(allIdleDurations, 0.50)
		idleP99 = percentile(allIdleDurations, 0.99)
	}
	log.Printf("phase5-A: idle poll cycles=%d, p50=%v, p99=%v", len(allIdleDurations), idleP50, idleP99)

	// ========================================================
	// Phase B: write -> delivery latency (healthy doorbell)
	// ========================================================
	log.Printf("phase5: --- Phase B: doorbell delivery latency ---")

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
	for i := 0; i < numWatchers; i++ {
		pc, err := pgx.Connect(ctx, dsn)
		if err != nil {
			dbCancel()
			return nil, fmt.Errorf("phase5: db poll conn %d: %w", i, err)
		}
		lc, err := pgx.Connect(ctx, dsn)
		if err != nil {
			pc.Close(context.Background())
			dbCancel()
			return nil, fmt.Errorf("phase5: db listen conn %d: %w", i, err)
		}
		dbPollConns[i] = pc
		dbListenConns[i] = lc

		w := reader.NewWatcher(pc, lc, reader.WatcherConfig{
			GVK:              gvk,
			StartRV:          startRV,
			BaselineInterval: 10 * time.Second,
			DebounceFloor:    50 * time.Millisecond,
		}, nil).WithMetrics(libWatcherMetrics)
		dbWatchers[i] = w
		done := make(chan error, 1)
		dbDones[i] = done
		go func(watcher *reader.Watcher, d chan error) { d <- watcher.Run(dbCtx) }(w, done)
	}

	time.Sleep(200 * time.Millisecond) // let watchers settle

	// Collectors.
	writeTimes := make(map[string]time.Time)
	var writeTimesMu sync.Mutex
	var collectWg sync.WaitGroup

	for i := 0; i < numWatchers; i++ {
		collectWg.Add(1)
		go func(idx int) {
			defer collectWg.Done()
			collected := 0
			deadline := time.After(60 * time.Second)
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

	// Write at configured rate.
	writeConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		dbCancel()
		return nil, fmt.Errorf("phase5: write conn: %w", err)
	}
	wr := writer.New(writeConn, nil).WithMetrics(libWriterMetrics)
	writeInterval := time.Duration(float64(time.Second) / float64(writeRate))

	for i := 0; i < writeCount; i++ {
		name := fmt.Sprintf("phase5-db-%d", i)
		req := model.WriteRequest{
			GVK:       gvk,
			Namespace: "phase5-db",
			Name:      name,
			Spec:      json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			Status:    json.RawMessage(`{}`),
			Metadata:  json.RawMessage(`{}`),
		}
		writeTimesMu.Lock()
		writeTimes[name] = time.Now()
		writeTimesMu.Unlock()
		if _, err := wr.Write(ctx, req); err != nil {
			allErrors++
		}
		time.Sleep(writeInterval)
	}
	writeConn.Close(context.Background())
	collectWg.Wait()

	dbCancel()
	for i := 0; i < numWatchers; i++ {
		<-dbDones[i]
		dbPollConns[i].Close(context.Background())
		dbListenConns[i].Close(context.Background())
	}

	// Compute doorbell delivery latencies.
	var dbLatencies []time.Duration
	for _, d := range deliveries {
		lat := d.recvTime.Sub(d.writeTime)
		dbLatencies = append(dbLatencies, lat)
		deliveryLatency.WithLabelValues(phase5Name).Observe(lat.Seconds())
	}
	sort.Slice(dbLatencies, func(i, j int) bool { return dbLatencies[i] < dbLatencies[j] })

	var dbP50, dbP99 time.Duration
	if len(dbLatencies) > 0 {
		dbP50 = percentile(dbLatencies, 0.50)
		dbP99 = percentile(dbLatencies, 0.99)
	}
	log.Printf("phase5-B: doorbell deliveries=%d, p50=%v, p99=%v", len(dbLatencies), dbP50, dbP99)

	// ========================================================
	// Phase C: notify-loss drill (nil listen conns)
	// ========================================================
	var lossP99 time.Duration
	totalLossDelivered := int64(0)

	if pCfg.NotifyLossDrill {
		log.Printf("phase5: --- Phase C: notify-loss drill ---")

		// Get HWM from phase B watchers.
		var lossHWM uint64
		for _, w := range dbWatchers {
			if h := w.HWM(); h > lossHWM {
				lossHWM = h
			}
		}

		var lossDeliveries []deliveryRecord
		var lossDeliveryMu sync.Mutex

		lossCtx, lossCancel := context.WithCancel(ctx)
		lossDones := make([]chan error, numWatchers)
		lossConns := make([]*pgx.Conn, numWatchers)
		lossWatchers := make([]*reader.Watcher, numWatchers)

		for i := 0; i < numWatchers; i++ {
			pc, err := pgx.Connect(ctx, dsn)
			if err != nil {
				lossCancel()
				return nil, fmt.Errorf("phase5: loss watcher conn %d: %w", i, err)
			}
			lossConns[i] = pc

			w := reader.NewWatcher(pc, nil, reader.WatcherConfig{
				GVK:              gvk,
				StartRV:          resourceversion.RV{Watermark: lossHWM},
				BaselineInterval: baselineInterval,
			}, nil).WithMetrics(libWatcherMetrics)
			lossWatchers[i] = w
			done := make(chan error, 1)
			lossDones[i] = done
			go func(watcher *reader.Watcher, d chan error) { d <- watcher.Run(lossCtx) }(w, done)
		}

		time.Sleep(200 * time.Millisecond)

		// Collectors.
		lossWriteTimes := make(map[string]time.Time)
		var lossWriteTimesMu sync.Mutex
		var lossCollectWg sync.WaitGroup
		var lossDeliveredCount atomic.Int64

		for i := 0; i < numWatchers; i++ {
			lossCollectWg.Add(1)
			go func(idx int) {
				defer lossCollectWg.Done()
				collected := 0
				deadline := time.After(60 * time.Second)
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
							lossDeliveredCount.Add(1)
						}
					case <-deadline:
						return
					}
				}
			}(i)
		}

		// Write.
		lossWriteConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			lossCancel()
			return nil, fmt.Errorf("phase5: loss write conn: %w", err)
		}
		lossWr := writer.New(lossWriteConn, nil).WithMetrics(libWriterMetrics)
		for i := 0; i < writeCount; i++ {
			name := fmt.Sprintf("phase5-loss-%d", i)
			req := model.WriteRequest{
				GVK:       gvk,
				Namespace: "phase5-loss",
				Name:      name,
				Spec:      json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
				Status:    json.RawMessage(`{}`),
				Metadata:  json.RawMessage(`{}`),
			}
			lossWriteTimesMu.Lock()
			lossWriteTimes[name] = time.Now()
			lossWriteTimesMu.Unlock()
			if _, err := lossWr.Write(ctx, req); err != nil {
				allErrors++
			}
			time.Sleep(writeInterval)
		}
		lossWriteConn.Close(context.Background())
		lossCollectWg.Wait()

		lossCancel()
		for i := 0; i < numWatchers; i++ {
			<-lossDones[i]
			lossConns[i].Close(context.Background())
		}

		var lossLatencies []time.Duration
		for _, d := range lossDeliveries {
			lossLatencies = append(lossLatencies, d.recvTime.Sub(d.writeTime))
		}
		sort.Slice(lossLatencies, func(i, j int) bool { return lossLatencies[i] < lossLatencies[j] })

		if len(lossLatencies) > 0 {
			lossP99 = percentile(lossLatencies, 0.99)
		}

		totalLossDelivered = lossDeliveredCount.Load()
		log.Printf("phase5-C: loss-drill deliveries=%d (expected %d), p99=%v",
			totalLossDelivered, int64(writeCount)*int64(numWatchers), lossP99)
	}

	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase5Name).Observe(elapsed.Seconds())

	// Evaluate pass/fail.
	passed := true
	if pCfg.DoorbellDeliveryP99Ms > 0 && dbP99.Milliseconds() > int64(pCfg.DoorbellDeliveryP99Ms) {
		passed = false
		log.Printf("phase5: FAIL — doorbell p99 %v exceeds target %dms", dbP99, pCfg.DoorbellDeliveryP99Ms)
	}
	if pCfg.NotifyLossDrill {
		expectedDeliveries := int64(writeCount) * int64(numWatchers)
		if totalLossDelivered < expectedDeliveries {
			passed = false
			log.Printf("phase5: FAIL — loss drill delivered %d/%d", totalLossDelivered, expectedDeliveries)
		}
	}

	totalWriteCount := int64(writeCount)
	if pCfg.NotifyLossDrill {
		totalWriteCount += int64(writeCount)
	}

	return &PhaseResult{
		Name:        phase5Name,
		Passed:      passed,
		Duration:    elapsed,
		TotalWrites: totalWriteCount,
		RPS:         float64(totalWriteCount) / elapsed.Seconds(),
		P50:         dbP50,
		P99:         dbP99,
		P999:        idleP99, // report idle p99 as p999 slot for reference
		Errors: map[string]int64{
			"write_errors": allErrors,
			"loss_p99_ms":  lossP99.Milliseconds(),
			"idle_p99_ms":  idleP99.Milliseconds(),
		},
	}, nil
}
