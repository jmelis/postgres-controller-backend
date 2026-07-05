package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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

const phase2bName = "phase2b_skew"

// RunPhase2b runs the hot-bucket Zipfian skew test.
// hot_bucket_write_pct of writes go to bucket 1, the rest are spread across
// remaining buckets. We measure cold-bucket p99 to ensure no starvation.
func RunPhase2b(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase2bSkew
	numBuckets := cfg.Cluster.Buckets
	holder := "phase2b-holder"
	ttl := cfg.Cluster.LeaseTTL

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

	hotPct := pCfg.HotBucketWritePct
	if hotPct <= 0 {
		hotPct = 80
	}

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	if numBuckets < 2 {
		return &PhaseResult{
			Name:   phase2bName,
			Passed: false,
			Errors: map[string]int64{"config": 1},
		}, fmt.Errorf("phase2b requires at least 2 buckets, got %d", numBuckets)
	}

	log.Printf("phase2b: starting skew test — %d%% writes to bucket 1, %d cold buckets, duration %v",
		hotPct, numBuckets-1, duration)

	// Acquire leases.
	leaseConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase2b: lease conn: %w", err)
	}
	defer leaseConn.Close(context.Background())

	bucketEpochs, err := acquireAllLeases(ctx, leaseConn, numBuckets, holder, ttl)
	if err != nil {
		return nil, fmt.Errorf("phase2b: %w", err)
	}

	// Lease renewal.
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
			BucketIDs:    makeBucketIDs(numBuckets),
			PollInterval: cfg.Verifier.PollInterval,
		}).WithMetrics(libVerifierMetrics)
		var verCtx context.Context
		verCtx, verCancel = context.WithCancel(ctx)
		defer verCancel()
		verDone = make(chan error, 1)
		go func() { verDone <- ver.Run(verCtx) }()
	}

	// Zipfian bucket selector: returns bucket 1 with probability hotPct/100,
	// otherwise uniformly selects from buckets 2..numBuckets.
	pickBucket := func() int {
		//nolint:gosec // non-cryptographic random
		if rand.Intn(100) < hotPct {
			return 1
		}
		//nolint:gosec
		return rand.Intn(numBuckets-1) + 2
	}

	// Per-bucket latency tracking.
	type bucketLatencies struct {
		mu        sync.Mutex
		latencies []time.Duration
	}
	bucketLats := make(map[int]*bucketLatencies)
	for b := 1; b <= numBuckets; b++ {
		bucketLats[b] = &bucketLatencies{}
	}

	var totalWrites atomic.Int64
	var totalSerFail, totalFenceFail, totalOtherErr atomic.Int64

	// Writer pool — one conn per bucket.
	type writeJob struct {
		bucketID int
		writeNum int
	}
	jobCh := make(chan writeJob, numBuckets*10)

	var writerWg sync.WaitGroup
	for w := 0; w < numBuckets; w++ {
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
				name := fmt.Sprintf("p2b-b%d-r%d", job.bucketID, job.writeNum)
				req := model.WriteRequest{
					GVK:         gvk,
					Namespace:   "phase2b",
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
				writeLatency.WithLabelValues(phase2bName, gvk).Observe(lat.Seconds())
				writesTotal.WithLabelValues(phase2bName, gvk, strconv.Itoa(job.bucketID)).Inc()

				bl := bucketLats[job.bucketID]
				bl.mu.Lock()
				bl.latencies = append(bl.latencies, lat)
				bl.mu.Unlock()
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
		bucketID := pickBucket()

		select {
		case jobCh <- writeJob{bucketID: bucketID, writeNum: writeCounter}:
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
	renewCancel()

	// Verifier.
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

	// Aggregate all latencies.
	var allLatencies []time.Duration
	for _, bl := range bucketLats {
		bl.mu.Lock()
		allLatencies = append(allLatencies, bl.latencies...)
		bl.mu.Unlock()
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })

	total := totalWrites.Load()
	rps := float64(total) / elapsed.Seconds()

	var p50, p99, p999 time.Duration
	if len(allLatencies) > 0 {
		p50 = percentile(allLatencies, 0.50)
		p99 = percentile(allLatencies, 0.99)
		p999 = percentile(allLatencies, 0.999)
	}

	// Compute cold-bucket p99 (all buckets except bucket 1).
	var coldLatencies []time.Duration
	for b := 2; b <= numBuckets; b++ {
		bl := bucketLats[b]
		bl.mu.Lock()
		coldLatencies = append(coldLatencies, bl.latencies...)
		bl.mu.Unlock()
	}
	sort.Slice(coldLatencies, func(i, j int) bool { return coldLatencies[i] < coldLatencies[j] })

	var coldP99 time.Duration
	if len(coldLatencies) > 0 {
		coldP99 = percentile(coldLatencies, 0.99)
	}

	// Hot bucket stats.
	hotBL := bucketLats[1]
	hotBL.mu.Lock()
	hotCount := len(hotBL.latencies)
	hotBL.mu.Unlock()

	coldCount := len(coldLatencies)
	actualHotPct := 0.0
	if total > 0 {
		actualHotPct = float64(hotCount) / float64(total) * 100
	}

	log.Printf("phase2b: completed — %d writes (%.1f w/s), hot_bucket=%d writes (%.0f%%), cold_p99=%v",
		total, rps, hotCount, actualHotPct, coldP99)
	log.Printf("phase2b: cold_buckets=%d writes, overall p50=%v p99=%v",
		coldCount, p50, p99)

	serFail := totalSerFail.Load()
	fenceFail := totalFenceFail.Load()
	otherErr := totalOtherErr.Load()

	if serFail > 0 {
		errorsTotal.WithLabelValues(phase2bName, "serialization").Add(float64(serFail))
	}

	passed := true
	if pCfg.ColdP99Ms > 0 && coldP99.Milliseconds() > int64(pCfg.ColdP99Ms) {
		passed = false
		log.Printf("phase2b: FAIL — cold bucket p99 %v exceeds target %dms", coldP99, pCfg.ColdP99Ms)
	}
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
			"serialization":  serFail,
			"fence_violation": fenceFail,
			"other":          otherErr,
		},
		VerifierViolations: violations,
	}, nil
}
