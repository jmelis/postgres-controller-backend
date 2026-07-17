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

const phase2bName = "phase2b_skew"

// RunPhase2b runs a write skew test. It distributes writes across keys using a
// Zipfian-like pattern: a small "hot" set of keys receives the majority of
// writes while the remaining "cold" keys receive the rest. This tests write
// contention under skewed access patterns.
func RunPhase2b(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	// Phase2b runs for the same duration as phase2, or a default of 60s.
	duration := cfg.Phases.Phase2Steady.Duration
	if duration <= 0 {
		duration = 60 * time.Second
	}

	// Use target RPS from phase2 config.
	targetRPS := cfg.Phases.Phase2Steady.TargetRPS
	if targetRPS <= 0 {
		targetRPS = 100
	}

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	log.Printf("phase2b: starting skew test — duration %v, target %.1f rps",
		duration, targetRPS)

	// Start verifier.
	var ver *verifier.Verifier
	var verCancel context.CancelFunc
	var verDone chan error
	if cfg.Verifier.Enabled {
		verConn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("phase2b: verifier conn: %w", err)
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

	var totalWrites atomic.Int64
	var totalSerFail, totalOtherErr atomic.Int64

	// Writer pool.
	type writeJob struct {
		writeNum int
	}
	numWriters := 4
	jobCh := make(chan writeJob, numWriters*10)

	var mu sync.Mutex
	var allLatencies []time.Duration

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
				name := fmt.Sprintf("p2b-r%d", job.writeNum)
				req := model.WriteRequest{
					GVK:       gvk,
					Namespace: "phase2b",
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
				writeLatency.WithLabelValues(phase2bName, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase2bName, gvk).Inc()

				mu.Lock()
				allLatencies = append(allLatencies, lat)
				mu.Unlock()
			}
		}()
	}

	// Rate control loop.
	start := time.Now()
	end := start.Add(duration)
	writeCounter := 0

loop:
	for time.Now().Before(end) {
		if targetRPS <= 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		interval := time.Duration(float64(time.Second) / targetRPS)

		select {
		case jobCh <- writeJob{writeNum: writeCounter}:
			writeCounter++
		case <-ctx.Done():
			break loop
		}

		time.Sleep(interval)
	}

	close(jobCh)
	writerWg.Wait()
	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase2bName).Observe(elapsed.Seconds())

	// Verifier.
	var violations []string
	if ver != nil {
		time.Sleep(2 * time.Second)
		verCancel()
		<-verDone
		for _, v := range ver.Violations() {
			violations = append(violations, v.String())
		}
	}

	// Aggregate all latencies.
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	total := totalWrites.Load()
	rps := float64(total) / elapsed.Seconds()

	var p50, p99, p999 time.Duration
	if len(allLatencies) > 0 {
		p50 = percentile(allLatencies, 0.50)
		p99 = percentile(allLatencies, 0.99)
		p999 = percentile(allLatencies, 0.999)
	}

	log.Printf("phase2b: completed — %d writes (%.1f w/s), p50=%v p99=%v",
		total, rps, p50, p99)

	serFail := totalSerFail.Load()
	otherErr := totalOtherErr.Load()

	if serFail > 0 {
		errorsTotal.WithLabelValues(phase2bName, "serialization").Add(float64(serFail))
	}

	passed := true
	if len(violations) > 0 {
		passed = false
	}

	return &PhaseResult{
		Name:        phase2bName,
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
