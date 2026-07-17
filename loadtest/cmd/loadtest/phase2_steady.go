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
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase2Name = "phase2_steady"

// RunPhase2 runs the steady-state test for the configured duration.
// It maintains target_rps using a ticker-based rate limiter,
// and periodically bursts to burst_rps for 10s every 5 minutes.
func RunPhase2(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase2Steady
	duration := pCfg.Duration
	targetRPS := pCfg.TargetRPS
	burstRPS := pCfg.BurstRPS

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	log.Printf("phase2: starting steady-state test — target %.1f rps, burst %.1f rps, duration %v",
		targetRPS, burstRPS, duration)

	// Start verifier.
	var ver *verifier.Verifier
	var verCancel context.CancelFunc
	var verDone chan error
	if cfg.Verifier.Enabled {
		verConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("phase2: verifier conn: %w", err)
		}
		defer verConn.Close(context.Background())

		ver = verifier.New(verConn, nil, verifier.Config{
			GVK:          gvk,
			PollInterval: cfg.Verifier.PollInterval,
		}).WithMetrics(libVerifierMetrics)

		var verCtx context.Context
		verCtx, verCancel = context.WithCancel(ctx)
		defer verCancel()
		verDone = make(chan error, 1)
		go func() { verDone <- ver.Run(verCtx) }()
	}

	// Rate-limited writer pool.
	type writeJob struct {
		writeNum int
	}

	numWriters := 4
	jobCh := make(chan writeJob, numWriters*10)

	var mu sync.Mutex
	var allLatencies []time.Duration
	var totalWrites atomic.Int64
	var totalSerFail, totalOtherErr atomic.Int64

	var writerWg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()

			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				totalOtherErr.Add(1)
				return
			}
			defer conn.Close(context.Background())

			wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)

			for job := range jobCh {
				name := fmt.Sprintf("p2-r%d", job.writeNum)
				req := model.WriteRequest{
					GVK:       gvk,
					Namespace: "phase2",
					Name:      name,
					Spec:      json.RawMessage(fmt.Sprintf(`{"n":%d}`, job.writeNum)),
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
				writeLatency.WithLabelValues(phase2Name, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase2Name, gvk).Inc()

				mu.Lock()
				allLatencies = append(allLatencies, lat)
				mu.Unlock()
			}
		}()
	}

	// Rate control loop: emit writes at the target rate, burst periodically.
	start := time.Now()
	end := start.Add(duration)

	const burstDuration = 10 * time.Second
	const burstInterval = 5 * time.Minute

	currentRPS := targetRPS
	lastBurstStart := start
	inBurst := false
	writeCounter := 0

loop:
	for time.Now().Before(end) {
		now := time.Now()

		// Decide if we should be in burst mode.
		if burstRPS > 0 {
			timeSinceLastBurst := now.Sub(lastBurstStart)
			if inBurst && timeSinceLastBurst >= burstDuration {
				inBurst = false
				currentRPS = targetRPS
				log.Printf("phase2: burst ended, back to %.1f rps", targetRPS)
			} else if !inBurst && timeSinceLastBurst >= burstInterval {
				inBurst = true
				lastBurstStart = now
				currentRPS = burstRPS
				log.Printf("phase2: burst started at %.1f rps for %v", burstRPS, burstDuration)
			}
		}

		if currentRPS <= 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		interval := time.Duration(float64(time.Second) / currentRPS)

		select {
		case jobCh <- writeJob{writeNum: writeCounter}:
			writeCounter++
		case <-ctx.Done():
			break loop
		}

		// Sleep to maintain rate.
		time.Sleep(interval)
	}

	close(jobCh)
	writerWg.Wait()
	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase2Name).Observe(elapsed.Seconds())

	// Collect verifier results.
	var violations []string
	if ver != nil {
		time.Sleep(2 * time.Second)
		verCancel()
		<-verDone
		for _, v := range ver.Violations() {
			violations = append(violations, v.String())
		}
	}

	// Aggregate.
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
		errorsTotal.WithLabelValues(phase2Name, "serialization").Add(float64(serFail))
	}
	if otherErr > 0 {
		errorsTotal.WithLabelValues(phase2Name, "other").Add(float64(otherErr))
	}

	log.Printf("phase2: completed — %d writes in %v (%.1f w/s), p50=%v p99=%v",
		total, elapsed.Round(time.Millisecond), rps, p50, p99)

	passed := true
	if pCfg.TargetRPS > 0 && rps < pCfg.TargetRPS*0.95 { // allow 5% slack for sustained
		passed = false
	}
	if pCfg.TargetP50Ms > 0 && p50.Milliseconds() > int64(pCfg.TargetP50Ms) {
		passed = false
	}
	if len(violations) > 0 {
		passed = false
	}

	return &PhaseResult{
		Name:        phase2Name,
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
