package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase1Name = "phase1_ceiling"

// RunPhase1 runs the ceiling test, optionally sweeping worker counts.
func RunPhase1(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase1Ceiling

	if len(pCfg.WorkerSweep) == 0 {
		return runPhase1Single(ctx, dsn, cfg, pCfg.WorkersPerBucket)
	}

	runs := pCfg.Runs
	log.Printf("phase1: worker sweep — %v (%d runs each)", pCfg.WorkerSweep, runs)

	var sweepResults []SweepEntry
	bestRPS := 0.0
	allPassed := true
	totalDuration := time.Duration(0)

	for _, workers := range pCfg.WorkerSweep {
		var rpsValues []float64
		var bestResult *PhaseResult
		var totalErrors int64

		for run := 0; run < runs; run++ {
			// Truncate and reseed.
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return nil, fmt.Errorf("phase1 sweep: connect: %w", err)
			}
			if run == 0 {
				log.Printf("phase1: sweep — clearing and reseeding for %d workers", workers)
			}
			if _, err := conn.Exec(ctx, "TRUNCATE kubernetes_resources, compaction_horizon"); err != nil {
				conn.Close(context.Background())
				return nil, fmt.Errorf("phase1 sweep: truncate: %w", err)
			}

			if err := Seed(ctx, conn, cfg); err != nil {
				conn.Close(context.Background())
				return nil, fmt.Errorf("phase1 sweep: seed for %d workers: %w", workers, err)
			}
			conn.Close(context.Background())

			result, err := runPhase1Single(ctx, dsn, cfg, workers)
			if err != nil {
				return nil, fmt.Errorf("phase1 sweep %d workers run %d: %w", workers, run+1, err)
			}

			rpsValues = append(rpsValues, result.RPS)
			totalDuration += result.Duration
			for _, c := range result.Errors {
				totalErrors += c
			}
			if bestResult == nil || result.RPS > bestResult.RPS {
				bestResult = result
			}
			if !result.Passed {
				allPassed = false
			}

			if runs > 1 {
				log.Printf("phase1: sweep — %d workers run %d/%d: %.1f w/s",
					workers, run+1, runs, result.RPS)
			}
		}

		meanRPS := mean(rpsValues)
		stddevRPS := stddev(rpsValues)

		sweepResults = append(sweepResults, SweepEntry{
			Workers:    workers,
			RPS:        meanRPS,
			RPSStdDev:  stddevRPS,
			P50:        bestResult.P50,
			P99:        bestResult.P99,
			P999:       bestResult.P999,
			ErrorCount: totalErrors,
			Runs:       runs,
		})

		log.Printf("phase1: sweep — %d workers: %.1f ± %.1f w/s, p50=%v, p99=%v, errors=%d",
			workers, meanRPS, stddevRPS, bestResult.P50, bestResult.P99, totalErrors)

		if meanRPS > bestRPS {
			bestRPS = meanRPS
		}
	}

	log.Printf("phase1: sweep complete — best RPS: %.1f", bestRPS)

	return &PhaseResult{
		Name:        phase1Name,
		Passed:      allPassed,
		Duration:    totalDuration,
		TotalWrites: 0,
		RPS:         bestRPS,
		Sweep:       sweepResults,
	}, nil
}

func runPhase1Single(ctx context.Context, dsn string, cfg *Config, totalWorkers int) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase1Ceiling
	duration := pCfg.Duration

	// Use the first configured GVK, or default.
	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	warmUp := pCfg.WarmUp

	log.Printf("phase1: starting ceiling test — %d workers, duration %v (warm-up %v)",
		totalWorkers, duration, warmUp)

	// Start verifier if enabled.
	var ver *verifier.Verifier
	var verCancel context.CancelFunc
	var verDone chan error
	if cfg.Verifier.Enabled {
		verConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("phase1: verifier conn: %w", err)
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

	// Per-worker latency collection.
	type workerResult struct {
		latencies   []time.Duration
		writes      int64
		serFailures int64
		otherErrors int64
	}

	results := make([]workerResult, totalWorkers)
	var totalWrites atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	warmUpEnd := start.Add(warmUp)
	deadline := start.Add(duration)

	for w := 0; w < totalWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				results[workerID].otherErrors++
				return
			}
			defer conn.Close(context.Background())

			wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
			var writeNum int
			warmedUp := warmUp == 0

			for time.Now().Before(deadline) {
				if !warmedUp && time.Now().After(warmUpEnd) {
					warmedUp = true
				}

				name := fmt.Sprintf("p1-w%d-r%d", workerID, writeNum)
				req := model.WriteRequest{
					GVK:       gvk,
					Namespace: "phase1",
					Name:      name,
					Spec:      json.RawMessage(fmt.Sprintf(`{"w":%d,"n":%d}`, workerID, writeNum)),
					Status:    json.RawMessage(`{}`),
					Metadata:  json.RawMessage(`{}`),
				}

				t0 := time.Now()
				_, writeErr := wr.Write(ctx, req)
				lat := time.Since(t0)

				if writeErr != nil {
					if isSerializationFailure(writeErr) {
						results[workerID].serFailures++
					} else {
						results[workerID].otherErrors++
					}
					continue
				}

				if warmedUp {
					results[workerID].latencies = append(results[workerID].latencies, lat)
					results[workerID].writes++
					totalWrites.Add(1)
				}
				writeLatency.WithLabelValues(phase1Name, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase1Name, gvk).Inc()
				writeNum++
			}
		}(w)
	}

	wg.Wait()
	measureDuration := duration - warmUp
	if measureDuration <= 0 {
		measureDuration = duration
	}
	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase1Name).Observe(elapsed.Seconds())

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
	var allLatencies []time.Duration
	var totalSerFail, totalOtherErr int64
	for _, r := range results {
		allLatencies = append(allLatencies, r.latencies...)
		totalSerFail += r.serFailures
		totalOtherErr += r.otherErrors
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	total := totalWrites.Load()
	rps := float64(total) / measureDuration.Seconds()

	var p50, p99, p999 time.Duration
	if len(allLatencies) > 0 {
		p50 = percentile(allLatencies, 0.50)
		p99 = percentile(allLatencies, 0.99)
		p999 = percentile(allLatencies, 0.999)
	}

	// Record errors in metrics.
	if totalSerFail > 0 {
		errorsTotal.WithLabelValues(phase1Name, "serialization").Add(float64(totalSerFail))
	}
	if totalOtherErr > 0 {
		errorsTotal.WithLabelValues(phase1Name, "other").Add(float64(totalOtherErr))
	}

	log.Printf("phase1: completed — %d writes in %v (%.1f w/s), p50=%v p99=%v p999=%v",
		total, elapsed.Round(time.Millisecond), rps, p50, p99, p999)
	log.Printf("phase1: serialization_failures=%d other_errors=%d verifier_violations=%d",
		totalSerFail, totalOtherErr, len(violations))

	// Evaluate pass/fail.
	passed := true
	if pCfg.TargetRPS > 0 && rps < pCfg.TargetRPS {
		passed = false
	}
	if pCfg.TargetP99Ms > 0 && p99.Milliseconds() > int64(pCfg.TargetP99Ms) {
		passed = false
	}
	if totalSerFail > 0 {
		passed = false
	}
	if len(violations) > 0 {
		passed = false
	}

	return &PhaseResult{
		Name:        phase1Name,
		Passed:      passed,
		Duration:    elapsed,
		TotalWrites: total,
		RPS:         rps,
		P50:         p50,
		P99:         p99,
		P999:        p999,
		Errors: map[string]int64{
			"serialization": totalSerFail,
			"other":         totalOtherErr,
		},
		VerifierViolations: violations,
	}, nil
}

// --- helpers ---

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "serialization") || strings.Contains(msg, "40001")
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func stddev(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	m := mean(vals)
	sum := 0.0
	for _, v := range vals {
		d := v - m
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(vals)-1))
}
