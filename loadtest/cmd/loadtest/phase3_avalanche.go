package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase3Name = "phase3_avalanche"

// RunPhase3 runs the kill-and-recover test.
//  1. Starts N writers, each writing continuously.
//  2. After a warmup period, kills kill_fraction of them.
//  3. New writers take over.
//  4. Verifier checks commit-ordered stream across handover.
func RunPhase3(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase3Avalanche
	killFraction := pCfg.KillFraction
	numWriters := 8

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	const warmupDuration = 10 * time.Second
	const postKillDuration = 20 * time.Second

	log.Printf("phase3: starting avalanche test — %d writers, kill_fraction=%.1f",
		numWriters, killFraction)

	// Start verifier.
	var ver *verifier.Verifier
	var verCancel context.CancelFunc
	var verDone chan error
	if cfg.Verifier.Enabled {
		verConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("phase3: verifier conn: %w", err)
		}
		defer verConn.Close(context.Background())

		ver = verifier.New(verConn, nil, verifier.Config{
			GVK:          gvk,
			PollInterval: 200 * time.Millisecond,
		}).WithMetrics(libVerifierMetrics)
		var verCtx context.Context
		verCtx, verCancel = context.WithCancel(ctx)
		defer verCancel()
		verDone = make(chan error, 1)
		go func() { verDone <- ver.Run(verCtx) }()
	}

	var mu sync.Mutex
	var allLatencies []time.Duration
	var totalWrites atomic.Int64
	var totalSerFail, totalOtherErr atomic.Int64

	// writerLoop is a single writer that writes continuously until
	// its context is cancelled.
	writerLoop := func(ctx context.Context, writerID string, wg *sync.WaitGroup) {
		defer wg.Done()

		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			totalOtherErr.Add(1)
			return
		}
		defer conn.Close(context.Background())

		wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
		writeNum := 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			name := fmt.Sprintf("p3-%s-r%d", writerID, writeNum)
			req := model.WriteRequest{
				GVK:       gvk,
				Namespace: "phase3",
				Name:      name,
				Spec:      json.RawMessage(fmt.Sprintf(`{"w":"%s","n":%d}`, writerID, writeNum)),
				Status:    json.RawMessage(`{}`),
				Metadata:  json.RawMessage(`{}`),
			}

			t0 := time.Now()
			_, writeErr := wr.Write(ctx, req)
			lat := time.Since(t0)

			if writeErr != nil {
				if isSerializationFailure(writeErr) {
					totalSerFail.Add(1)
				} else {
					totalOtherErr.Add(1)
				}
				continue
			}

			totalWrites.Add(1)
			writeLatency.WithLabelValues(phase3Name, gvk).Observe(lat.Seconds())
			writesTotal.WithLabelValues(phase3Name, gvk).Inc()

			mu.Lock()
			allLatencies = append(allLatencies, lat)
			mu.Unlock()
			writeNum++
		}
	}

	start := time.Now()

	// Phase A: start writers (warmup).
	log.Printf("phase3: warmup — starting %d writers for %v", numWriters, warmupDuration)
	warmupCtx, warmupCancel := context.WithCancel(ctx)
	var warmupWg sync.WaitGroup
	writerCancels := make([]context.CancelFunc, numWriters)

	for i := 0; i < numWriters; i++ {
		wCtx, wCancel := context.WithCancel(warmupCtx)
		writerCancels[i] = wCancel
		warmupWg.Add(1)
		go writerLoop(wCtx, fmt.Sprintf("holder-%d", i), &warmupWg)
	}

	time.Sleep(warmupDuration)

	// Phase B: kill kill_fraction of writers.
	numToKill := int(float64(numWriters) * killFraction)
	if numToKill < 1 && killFraction > 0 {
		numToKill = 1
	}
	if numToKill > numWriters {
		numToKill = numWriters
	}

	// Randomly select which writers to kill.
	//nolint:gosec
	perm := rand.Perm(numWriters)
	killedWorkers := make([]int, numToKill)
	survivingWorkers := make([]int, 0, numWriters-numToKill)
	for i := 0; i < numToKill; i++ {
		killedWorkers[i] = perm[i]
	}
	for i := numToKill; i < numWriters; i++ {
		survivingWorkers = append(survivingWorkers, perm[i])
	}

	log.Printf("phase3: killing writers %v", killedWorkers)
	for _, w := range killedWorkers {
		writerCancels[w]()
	}

	// Phase C: new writers take over.
	var newWriterWg sync.WaitGroup
	newCtx, newCancel := context.WithCancel(ctx)
	for i, w := range killedWorkers {
		newWriterWg.Add(1)
		go writerLoop(newCtx, fmt.Sprintf("survivor-%d-%d", w, i), &newWriterWg)
	}
	log.Printf("phase3: new writers started for %d killed workers", len(killedWorkers))

	// Let the system run for postKillDuration.
	log.Printf("phase3: running post-kill for %v", postKillDuration)
	time.Sleep(postKillDuration)

	// Stop all writers.
	warmupCancel()
	newCancel()
	warmupWg.Wait()
	newWriterWg.Wait()

	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase3Name).Observe(elapsed.Seconds())

	// Let verifier catch up.
	var violations []string
	if ver != nil {
		time.Sleep(2 * time.Second)
		verCancel()
		<-verDone
		for _, v := range ver.Violations() {
			violations = append(violations, v.String())
		}
	}

	// Aggregate results.
	mu.Lock()
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	latCopy := make([]time.Duration, len(allLatencies))
	copy(latCopy, allLatencies)
	mu.Unlock()

	total := totalWrites.Load()
	rps := float64(total) / elapsed.Seconds()

	var p50, p99, p999 time.Duration
	if len(latCopy) > 0 {
		p50 = percentile(latCopy, 0.50)
		p99 = percentile(latCopy, 0.99)
		p999 = percentile(latCopy, 0.999)
	}

	serFail := totalSerFail.Load()
	otherErr := totalOtherErr.Load()

	if serFail > 0 {
		errorsTotal.WithLabelValues(phase3Name, "serialization").Add(float64(serFail))
	}

	log.Printf("phase3: completed — %d writes (%.1f w/s), violations=%d",
		total, rps, len(violations))
	log.Printf("phase3: killed=%v, surviving=%v", killedWorkers, survivingWorkers)

	passed := true
	if len(violations) > 0 {
		passed = false
	}

	return &PhaseResult{
		Name:        phase3Name,
		Passed:      passed,
		Duration:    elapsed,
		TotalWrites: total,
		RPS:         rps,
		P50:         p50,
		P99:         p99,
		P999:        p999,
		Errors: map[string]int64{
			"serialization": serFail,
			"other":         otherErr,
		},
		VerifierViolations: violations,
	}, nil
}
