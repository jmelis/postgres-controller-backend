package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase2Name = "phase2_steady"

// RunPhase2 runs the steady-state test for the configured duration.
// It maintains target_rps across all buckets using a ticker-based rate limiter,
// and periodically bursts to burst_rps for 10s every 5 minutes.
func RunPhase2(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase2Steady
	numBuckets := cfg.Cluster.Buckets
	duration := pCfg.Duration
	targetRPS := pCfg.TargetRPS
	burstRPS := pCfg.BurstRPS
	holder := "phase2-holder"
	ttl := cfg.Cluster.LeaseTTL

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	log.Printf("phase2: starting steady-state test — target %.1f rps, burst %.1f rps, duration %v",
		targetRPS, burstRPS, duration)

	// Acquire leases.
	leaseConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase2: lease conn: %w", err)
	}
	defer leaseConn.Close(context.Background())

	bucketEpochs, err := acquireAllLeases(ctx, leaseConn, numBuckets, holder, ttl)
	if err != nil {
		return nil, fmt.Errorf("phase2: %w", err)
	}

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
			BucketIDs:    makeBucketIDs(numBuckets),
			PollInterval: cfg.Verifier.PollInterval,
		}).WithMetrics(libVerifierMetrics)

		var verCtx context.Context
		verCtx, verCancel = context.WithCancel(ctx)
		defer verCancel()
		verDone = make(chan error, 1)
		go func() { verDone <- ver.Run(verCtx) }()
	}

	// Lease renewal goroutine — renew every ttl/3.
	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()
	go func() {
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		renewMgr := lease.NewSpecManager(leaseConn, holder).WithMetrics(libLeaseMetrics)
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				for b := 1; b <= numBuckets; b++ {
					_ = renewMgr.Renew(renewCtx, b, ttl)
				}
			}
		}
	}()

	// Rate-limited writer pool.
	// Each writer goroutine picks writes off a shared channel.
	type writeJob struct {
		bucketID int
		writeNum int
	}

	numWriters := numBuckets // one writer conn per bucket
	jobCh := make(chan writeJob, numWriters*10)

	var mu sync.Mutex
	var allLatencies []time.Duration
	var totalWrites atomic.Int64
	var totalSerFail, totalFenceFail, totalOtherErr atomic.Int64

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
				name := fmt.Sprintf("p2-b%d-r%d", job.bucketID, job.writeNum)
				req := model.WriteRequest{
					GVK:         gvk,
					Namespace:   "phase2",
					Name:        name,
					BucketID:    job.bucketID,
					Spec:        json.RawMessage(fmt.Sprintf(`{"b":%d,"n":%d}`, job.bucketID, job.writeNum)),
					Status:      json.RawMessage(`{}`),
					Metadata:    json.RawMessage(`{}`),
					LeaseHolder: holder,
					LeaseEpoch:  bucketEpochs[job.bucketID],
				}

				t0 := time.Now()
				_, writeErr := wr.Write(ctx, req)
				lat := time.Since(t0)

				if writeErr != nil {
					if isSerializationFailure(writeErr) {
						totalSerFail.Add(1)
					} else if isFenceViolation(writeErr) {
						totalFenceFail.Add(1)
					} else {
						totalOtherErr.Add(1)
					}
					continue
				}

				totalWrites.Add(1)
				writeLatency.WithLabelValues(phase2Name, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase2Name, gvk, strconv.Itoa(job.bucketID)).Inc()

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
		bucketID := (writeCounter % numBuckets) + 1

		select {
		case jobCh <- writeJob{bucketID: bucketID, writeNum: writeCounter}:
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
	renewCancel()

	// Collect verifier results.
	var violations []string
	if ver != nil {
		time.Sleep(2 * time.Second)
		verCancel()
		<-verDone
		for _, v := range ver.Violations() {
			violations = append(violations, fmt.Sprintf("[%s] bucket=%d gvk=%s: %s",
				v.Invariant, v.Bucket, v.GVK, v.Detail))
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
	fenceFail := totalFenceFail.Load()
	otherErr := totalOtherErr.Load()

	if serFail > 0 {
		errorsTotal.WithLabelValues(phase2Name, "serialization").Add(float64(serFail))
	}
	if fenceFail > 0 {
		errorsTotal.WithLabelValues(phase2Name, "fence_violation").Add(float64(fenceFail))
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
			"serialization":  serFail,
			"fence_violation": fenceFail,
			"other":          otherErr,
		},
		VerifierViolations: violations,
	}, nil
}
