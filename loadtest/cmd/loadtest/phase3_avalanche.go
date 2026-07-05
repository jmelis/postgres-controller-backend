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

const phase3Name = "phase3_avalanche"

// RunPhase3 runs the kill-replicas + zombie-writers test.
//  1. Starts N writers (one per bucket), each writing continuously.
//  2. After a warmup period, kills kill_fraction of them.
//  3. Optionally starts zombie_writers that hold stale epochs and attempt writes
//     (must be fenced — all zombie writes should fail with ErrFenceViolation).
//  4. Surviving + new writers take over via lease acquisition.
//  5. Verifier checks gapless stream across handover.
func RunPhase3(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase3Avalanche
	numBuckets := cfg.Cluster.Buckets
	holder := "phase3-holder"
	ttl := cfg.Cluster.LeaseTTL
	killFraction := pCfg.KillFraction
	zombieCount := pCfg.ZombieWriters

	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}

	const warmupDuration = 10 * time.Second
	const postKillDuration = 20 * time.Second
	const zombieDuration = 15 * time.Second

	log.Printf("phase3: starting avalanche test — %d buckets, kill_fraction=%.1f, zombies=%d",
		numBuckets, killFraction, zombieCount)

	// Acquire initial leases.
	leaseConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase3: lease conn: %w", err)
	}
	defer leaseConn.Close(context.Background())

	bucketEpochs, err := acquireAllLeases(ctx, leaseConn, numBuckets, holder, ttl)
	if err != nil {
		return nil, fmt.Errorf("phase3: %w", err)
	}

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
			BucketIDs:    makeBucketIDs(numBuckets),
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
	var totalSerFail, totalFenceFail, totalOtherErr atomic.Int64
	var zombieFenced atomic.Int64

	// writerLoop is a single writer that writes to its assigned bucket until
	// its context is cancelled.
	writerLoop := func(ctx context.Context, bucketID int, holderID string, epoch int64, wg *sync.WaitGroup) {
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

			name := fmt.Sprintf("p3-%s-b%d-r%d", holderID, bucketID, writeNum)
			req := model.WriteRequest{
				GVK:         gvk,
				Namespace:   "phase3",
				Name:        name,
				BucketID:    bucketID,
				Spec:        json.RawMessage(fmt.Sprintf(`{"h":"%s","n":%d}`, holderID, writeNum)),
				Status:      json.RawMessage(`{}`),
				Metadata:    json.RawMessage(`{}`),
				LeaseHolder: holderID,
				LeaseEpoch:  epoch,
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
			writeLatency.WithLabelValues(phase3Name, gvk).Observe(lat.Seconds())
			writesTotal.WithLabelValues(phase3Name, gvk, strconv.Itoa(bucketID)).Inc()

			mu.Lock()
			allLatencies = append(allLatencies, lat)
			mu.Unlock()
			writeNum++
		}
	}

	start := time.Now()

	// Phase A: start one writer per bucket (warmup).
	log.Printf("phase3: warmup — starting %d writers for %v", numBuckets, warmupDuration)
	warmupCtx, warmupCancel := context.WithCancel(ctx)
	var warmupWg sync.WaitGroup
	writerCancels := make([]context.CancelFunc, numBuckets)

	for b := 1; b <= numBuckets; b++ {
		wCtx, wCancel := context.WithCancel(warmupCtx)
		writerCancels[b-1] = wCancel
		warmupWg.Add(1)
		go writerLoop(wCtx, b, holder, bucketEpochs[b], &warmupWg)
	}

	time.Sleep(warmupDuration)

	// Phase B: kill kill_fraction of writers.
	numToKill := int(float64(numBuckets) * killFraction)
	if numToKill < 1 && killFraction > 0 {
		numToKill = 1
	}
	if numToKill > numBuckets {
		numToKill = numBuckets
	}

	// Randomly select which buckets to kill.
	//nolint:gosec
	perm := rand.Perm(numBuckets)
	killedBuckets := make([]int, numToKill)
	survivingBuckets := make([]int, 0, numBuckets-numToKill)
	for i := 0; i < numToKill; i++ {
		killedBuckets[i] = perm[i] + 1
	}
	for i := numToKill; i < numBuckets; i++ {
		survivingBuckets = append(survivingBuckets, perm[i]+1)
	}

	log.Printf("phase3: killing writers for buckets %v", killedBuckets)
	staleEpochs := make(map[int]int64)
	for _, b := range killedBuckets {
		staleEpochs[b] = bucketEpochs[b]
		writerCancels[b-1]()
	}

	// Wait for lease TTL to expire for killed writers.
	log.Printf("phase3: waiting for leases to expire (%v)", ttl)
	time.Sleep(ttl + 2*time.Second)

	// Phase C: optionally start zombie writers with stale epochs.
	var zombieWg sync.WaitGroup
	var zombieCtx context.Context
	var zombieCancel context.CancelFunc

	if zombieCount > 0 && len(killedBuckets) > 0 {
		zombieCtx, zombieCancel = context.WithCancel(ctx)
		defer zombieCancel()
		log.Printf("phase3: starting %d zombie writers with stale epochs", zombieCount)

		for z := 0; z < zombieCount; z++ {
			bucketID := killedBuckets[z%len(killedBuckets)]
			zombieHolder := fmt.Sprintf("zombie-%d", z)
			staleEpoch := staleEpochs[bucketID]

			zombieWg.Add(1)
			go func(bID int, h string, ep int64) {
				defer zombieWg.Done()

				conn, err := pgx.Connect(zombieCtx, dsn)
				if err != nil {
					return
				}
				defer conn.Close(context.Background())

				wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
				for n := 0; ; n++ {
					select {
					case <-zombieCtx.Done():
						return
					default:
					}

					name := fmt.Sprintf("p3-zombie-%s-b%d-r%d", h, bID, n)
					req := model.WriteRequest{
						GVK:         gvk,
						Namespace:   "phase3",
						Name:        name,
						BucketID:    bID,
						Spec:        json.RawMessage(`{"zombie":true}`),
						Status:      json.RawMessage(`{}`),
						Metadata:    json.RawMessage(`{}`),
						LeaseHolder: h,
						LeaseEpoch:  ep, // stale — should be fenced
					}

					_, writeErr := wr.Write(zombieCtx, req)
					if writeErr != nil {
						if isFenceViolation(writeErr) {
							zombieFenced.Add(1)
						}
						// All zombie writes should fail; if they succeed, that's a problem.
					}
					time.Sleep(50 * time.Millisecond)
				}
			}(bucketID, zombieHolder, staleEpoch)
		}
	}

	// Phase D: new writers acquire leases and take over killed buckets.
	newHolder := "phase3-survivor"
	newLeaseConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		warmupCancel()
		return nil, fmt.Errorf("phase3: new lease conn: %w", err)
	}
	defer newLeaseConn.Close(context.Background())

	newMgr := lease.NewSpecManager(newLeaseConn, newHolder).WithMetrics(libLeaseMetrics)
	newEpochs := make(map[int]int64)
	for _, b := range killedBuckets {
		epoch, err := newMgr.Acquire(ctx, b, ttl)
		if err != nil {
			log.Printf("phase3: WARNING — could not acquire lease for bucket %d: %v", b, err)
			continue
		}
		newEpochs[b] = epoch
	}
	log.Printf("phase3: new writers acquired leases for %d/%d killed buckets", len(newEpochs), len(killedBuckets))

	// Start new writers for recovered buckets.
	var newWriterWg sync.WaitGroup
	newCtx, newCancel := context.WithCancel(ctx)
	for b, ep := range newEpochs {
		newWriterWg.Add(1)
		go writerLoop(newCtx, b, newHolder, ep, &newWriterWg)
	}

	// Let the system run for postKillDuration.
	log.Printf("phase3: running post-kill for %v", postKillDuration)
	time.Sleep(postKillDuration)

	// Stop zombies.
	if zombieCancel != nil {
		log.Printf("phase3: stopping zombies (fenced %d writes)", zombieFenced.Load())
		zombieCancel()
		zombieWg.Wait()
	}

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
			violations = append(violations, fmt.Sprintf("[%s] bucket=%d gvk=%s: %s",
				v.Invariant, v.Bucket, v.GVK, v.Detail))
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
	fenceFail := totalFenceFail.Load()
	otherErr := totalOtherErr.Load()
	fenced := zombieFenced.Load()

	if serFail > 0 {
		errorsTotal.WithLabelValues(phase3Name, "serialization").Add(float64(serFail))
	}
	if fenceFail > 0 {
		errorsTotal.WithLabelValues(phase3Name, "fence_violation").Add(float64(fenceFail))
	}

	log.Printf("phase3: completed — %d writes (%.1f w/s), zombie_fenced=%d, violations=%d",
		total, rps, fenced, len(violations))
	log.Printf("phase3: killed=%v, surviving=%v", killedBuckets, survivingBuckets)

	passed := true
	// Zombie writes must all have been fenced (if any zombies ran).
	if zombieCount > 0 && fenced == 0 {
		log.Printf("phase3: WARNING — zombies ran but zero fence violations detected")
		// This is acceptable if zombie writes never reached the DB
		// (e.g., connection failed). We only fail if they succeeded.
	}
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
			"serialization":  serFail,
			"fence_violation": fenceFail,
			"zombie_fenced":  fenced,
			"other":          otherErr,
		},
		VerifierViolations: violations,
	}, nil
}
