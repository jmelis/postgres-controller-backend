package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase1Name = "phase1_ceiling"

// RunPhase1 runs the counter ceiling test.
// N workers per bucket, all write unique objects for the configured duration.
func RunPhase1(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase1Ceiling
	numBuckets := cfg.Cluster.Buckets
	workersPerBucket := pCfg.WorkersPerBucket
	totalWorkers := workersPerBucket * numBuckets
	duration := pCfg.Duration
	holder := "phase1-holder"
	ttl := cfg.Cluster.LeaseTTL

	// Use the first configured GVK, or default.
	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	log.Printf("phase1: starting ceiling test — %d workers x %d buckets, duration %v",
		workersPerBucket, numBuckets, duration)

	// Acquire leases for all buckets.
	leaseConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase1: lease conn: %w", err)
	}
	defer leaseConn.Close(context.Background())

	bucketEpochs := make(map[int]int64)
	leaseMgr := lease.NewSpecManager(leaseConn, holder).WithMetrics(libLeaseMetrics)
	for b := 1; b <= numBuckets; b++ {
		epoch, err := leaseMgr.Acquire(ctx, b, ttl)
		if err != nil {
			return nil, fmt.Errorf("phase1: acquire lease bucket %d: %w", b, err)
		}
		bucketEpochs[b] = epoch
	}

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

		bucketIDs := makeBucketIDs(numBuckets)
		ver = verifier.New(verConn, nil, verifier.Config{
			GVK:          gvk,
			BucketIDs:    bucketIDs,
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
		latencies     []time.Duration
		writes        int64
		serFailures   int64
		fenceFailures int64
		otherErrors   int64
	}

	results := make([]workerResult, totalWorkers)
	var totalWrites atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
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

			bucketID := (workerID % numBuckets) + 1
			wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
			var writeNum int

			for time.Now().Before(deadline) {
				name := fmt.Sprintf("p1-w%d-r%d", workerID, writeNum)
				req := model.WriteRequest{
					GVK:         gvk,
					Namespace:   "phase1",
					Name:        name,
					BucketID:    bucketID,
					Spec:        json.RawMessage(fmt.Sprintf(`{"w":%d,"n":%d}`, workerID, writeNum)),
					Status:      json.RawMessage(`{}`),
					Metadata:    json.RawMessage(`{}`),
					LeaseHolder: holder,
					LeaseEpoch:  bucketEpochs[bucketID],
				}

				t0 := time.Now()
				_, writeErr := wr.Write(ctx, req)
				lat := time.Since(t0)

				if writeErr != nil {
					if isSerializationFailure(writeErr) {
						results[workerID].serFailures++
					} else if isFenceViolation(writeErr) {
						results[workerID].fenceFailures++
					} else {
						results[workerID].otherErrors++
					}
					continue
				}

				results[workerID].latencies = append(results[workerID].latencies, lat)
				results[workerID].writes++
				totalWrites.Add(1)
				writeLatency.WithLabelValues(phase1Name, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase1Name, gvk, strconv.Itoa(bucketID)).Inc()
				writeNum++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	phaseDuration.WithLabelValues(phase1Name).Observe(elapsed.Seconds())

	// Let verifier catch up.
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

	// Aggregate results.
	var allLatencies []time.Duration
	var totalSerFail, totalFenceFail, totalOtherErr int64
	for _, r := range results {
		allLatencies = append(allLatencies, r.latencies...)
		totalSerFail += r.serFailures
		totalFenceFail += r.fenceFailures
		totalOtherErr += r.otherErrors
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	total := totalWrites.Load()
	rps := float64(total) / elapsed.Seconds()

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
	if totalFenceFail > 0 {
		errorsTotal.WithLabelValues(phase1Name, "fence_violation").Add(float64(totalFenceFail))
	}
	if totalOtherErr > 0 {
		errorsTotal.WithLabelValues(phase1Name, "other").Add(float64(totalOtherErr))
	}

	log.Printf("phase1: completed — %d writes in %v (%.1f w/s), p50=%v p99=%v p999=%v",
		total, elapsed.Round(time.Millisecond), rps, p50, p99, p999)
	log.Printf("phase1: serialization_failures=%d fence_violations=%d other_errors=%d verifier_violations=%d",
		totalSerFail, totalFenceFail, totalOtherErr, len(violations))

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
	if totalFenceFail > 0 {
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
			"serialization":  totalSerFail,
			"fence_violation": totalFenceFail,
			"other":          totalOtherErr,
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

func isFenceViolation(err error) bool {
	return errors.Is(err, writer.ErrFenceViolation)
}

func makeBucketIDs(n int) []int {
	ids := make([]int, n)
	for i := range n {
		ids[i] = i + 1
	}
	return ids
}
