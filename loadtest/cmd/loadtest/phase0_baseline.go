package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

const phase0Name = "phase0_baseline"

// RunPhase0 measures network RTT, per-step latency, sync commit overhead,
// and stored procedure round-trip savings to diagnose write-path bottlenecks.
func RunPhase0(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error) {
	pCfg := cfg.Phases.Phase0Baseline

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("phase0: connect: %w", err)
	}
	defer conn.Close(context.Background())

	start := time.Now()
	baseline := &BaselineResult{}

	// --- A: Network RTT ---
	log.Printf("phase0: measuring network RTT (%d iterations)", pCfg.PingIterations)
	baseline.PingP50, baseline.PingP99, baseline.PingP999, err = measurePingRTT(ctx, conn, pCfg.PingIterations)
	if err != nil {
		return nil, fmt.Errorf("phase0: ping RTT: %w", err)
	}
	log.Printf("phase0: network RTT — p50=%v  p99=%v  p999=%v",
		baseline.PingP50, baseline.PingP99, baseline.PingP999)

	// --- B: pg_stat_statements snapshot (before) ---
	pgStatBefore, pgStatErr := snapshotPgStat(ctx, conn)
	if pgStatErr != nil {
		log.Printf("phase0: pg_stat_statements unavailable: %v (continuing without)", pgStatErr)
	}

	// --- C: Sync commit comparison ---
	log.Printf("phase0: measuring sync commit cost (%d writes each)", pCfg.SyncCommitCompare)
	err = measureSyncCommitCost(ctx, dsn, pCfg.SyncCommitCompare, cfg, baseline)
	if err != nil {
		return nil, fmt.Errorf("phase0: sync commit: %w", err)
	}
	log.Printf("phase0: sync_commit=on: %.1f w/s p50=%v  off: %.1f w/s p50=%v  speedup: %.1fx",
		baseline.SyncCommitRPS, baseline.SyncCommitP50,
		baseline.AsyncCommitRPS, baseline.AsyncCommitP50,
		baseline.AsyncCommitRPS/max(baseline.SyncCommitRPS, 0.1))

	// --- D: Stored procedure comparison ---
	log.Printf("phase0: measuring stored procedure vs Go writer (%d writes each)", pCfg.StoredProcWrites)
	err = measureStoredProcComparison(ctx, dsn, pCfg.StoredProcWrites, cfg, baseline)
	if err != nil {
		return nil, fmt.Errorf("phase0: stored proc comparison: %w", err)
	}
	log.Printf("phase0: Go writer: %.1f w/s p50=%v  stored proc: %.1f w/s p50=%v  savings: %.1f%%",
		baseline.GoWriterRPS, baseline.GoWriterP50,
		baseline.StoredProcRPS, baseline.StoredProcP50,
		baseline.RoundTripSavings)

	// --- E: pg_stat_statements snapshot (after) ---
	if pgStatErr == nil {
		pgStatAfter, err := snapshotPgStat(ctx, conn)
		if err == nil {
			baseline.PgStatEntries = diffPgStat(pgStatBefore, pgStatAfter)
		}
	}

	elapsed := time.Since(start)

	return &PhaseResult{
		Name:     phase0Name,
		Passed:   true,
		Duration: elapsed,
		Baseline: baseline,
	}, nil
}

// measurePingRTT runs SELECT 1 in a loop and returns latency percentiles.
func measurePingRTT(ctx context.Context, conn *pgx.Conn, iterations int) (p50, p99, p999 time.Duration, err error) {
	latencies := make([]time.Duration, 0, iterations)
	for i := 0; i < iterations; i++ {
		t0 := time.Now()
		var dummy int
		if err := conn.QueryRow(ctx, "SELECT 1").Scan(&dummy); err != nil {
			return 0, 0, 0, fmt.Errorf("ping iteration %d: %w", i, err)
		}
		latencies = append(latencies, time.Since(t0))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return percentile(latencies, 0.50), percentile(latencies, 0.99), percentile(latencies, 0.999), nil
}

// measureSyncCommitCost compares write throughput with synchronous_commit on vs off.
func measureSyncCommitCost(ctx context.Context, dsn string, iterations int, cfg *Config, result *BaselineResult) error {
	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}
	holder := "phase0-sync-test"
	ttl := cfg.Cluster.LeaseTTL

	// Run with sync commit ON (default).
	syncRPS, syncP50, err := runSingleThreadedWrites(ctx, dsn, iterations, gvk, holder, ttl, "phase0-sync", "")
	if err != nil {
		return fmt.Errorf("sync commit on: %w", err)
	}
	result.SyncCommitRPS = syncRPS
	result.SyncCommitP50 = syncP50

	// Run with sync commit OFF.
	asyncRPS, asyncP50, err := runSingleThreadedWrites(ctx, dsn, iterations, gvk, holder, ttl, "phase0-async", "off")
	if err != nil {
		return fmt.Errorf("sync commit off: %w", err)
	}
	result.AsyncCommitRPS = asyncRPS
	result.AsyncCommitP50 = asyncP50

	return nil
}

// runSingleThreadedWrites creates a fresh connection, acquires a lease on bucket 1,
// writes iterations objects, and returns RPS and p50 latency. If syncCommitMode is
// non-empty, it issues SET synchronous_commit = <mode> before writing.
func runSingleThreadedWrites(ctx context.Context, dsn string, iterations int, gvk, holder string, ttl time.Duration, namespace, syncCommitMode string) (rps float64, p50 time.Duration, err error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.Background())

	// Clean slate.
	if _, err := conn.Exec(ctx, "TRUNCATE kubernetes_resources, gvk_bucket_counters, bucket_leases"); err != nil {
		return 0, 0, fmt.Errorf("truncate: %w", err)
	}

	// Set sync commit mode if requested.
	if syncCommitMode != "" {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET synchronous_commit = %s", syncCommitMode)); err != nil {
			return 0, 0, fmt.Errorf("set synchronous_commit: %w", err)
		}
	}

	// Acquire lease.
	mgr := lease.NewSpecManager(conn, holder).WithMetrics(libLeaseMetrics)
	epoch, err := mgr.Acquire(ctx, 1, ttl)
	if err != nil {
		return 0, 0, fmt.Errorf("acquire lease: %w", err)
	}

	wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
	spec := json.RawMessage(`{"phase0": true}`)
	status := json.RawMessage(`{}`)
	metadata := json.RawMessage(`{}`)

	latencies := make([]time.Duration, 0, iterations)
	start := time.Now()

	for i := 0; i < iterations; i++ {
		req := model.WriteRequest{
			GVK:         gvk,
			Namespace:   namespace,
			Name:        fmt.Sprintf("p0-%s-%d", namespace, i),
			BucketID:    1,
			Spec:        spec,
			Status:      status,
			Metadata:    metadata,
			LeaseHolder: holder,
			LeaseEpoch:  epoch,
		}
		t0 := time.Now()
		if _, err := wr.Write(ctx, req); err != nil {
			return 0, 0, fmt.Errorf("write %d: %w", i, err)
		}
		latencies = append(latencies, time.Since(t0))
	}

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	return float64(iterations) / elapsed.Seconds(), percentile(latencies, 0.50), nil
}

// --- Stored procedure comparison ---

func measureStoredProcComparison(ctx context.Context, dsn string, iterations int, cfg *Config, result *BaselineResult) error {
	gvk := "apps/v1/Deployment"
	if len(cfg.Seed.GVKs) > 0 {
		gvk = cfg.Seed.GVKs[0].GVK
	}
	holder := "phase0-sproc-test"
	ttl := cfg.Cluster.LeaseTTL

	// --- Go writer baseline ---
	goRPS, goP50, err := runSingleThreadedWrites(ctx, dsn, iterations, gvk, holder, ttl, "phase0-go", "")
	if err != nil {
		return fmt.Errorf("go writer: %w", err)
	}
	// Get p99 too — need to re-run with latency collection. Reuse the connection.
	goConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("go writer p99 connect: %w", err)
	}
	defer goConn.Close(context.Background())

	// We already have p50 and RPS from above. For p99, capture latencies from the stored proc run
	// and compare. The Go writer numbers from runSingleThreadedWrites are sufficient.
	result.GoWriterRPS = goRPS
	result.GoWriterP50 = goP50

	// Compute Go writer p99 by running a smaller batch.
	goP99Lats, err := runWritesWithLatencies(ctx, dsn, min(iterations, 200), gvk, holder, ttl, "phase0-go-p99")
	if err != nil {
		return fmt.Errorf("go writer p99: %w", err)
	}
	result.GoWriterP99 = percentile(goP99Lats, 0.99)

	// --- Stored procedure ---
	spConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("stored proc connect: %w", err)
	}
	defer spConn.Close(context.Background())

	// Clean slate — pgctl_write is created by the schema migration.
	if _, err := spConn.Exec(ctx, "TRUNCATE kubernetes_resources, gvk_bucket_counters, bucket_leases"); err != nil {
		return fmt.Errorf("truncate for sproc: %w", err)
	}

	// Acquire lease.
	mgr := lease.NewSpecManager(spConn, holder).WithMetrics(libLeaseMetrics)
	epoch, err := mgr.Acquire(ctx, 1, ttl)
	if err != nil {
		return fmt.Errorf("acquire lease for sproc: %w", err)
	}

	spec := json.RawMessage(`{"phase0": true}`)
	status := json.RawMessage(`{}`)
	metadata := json.RawMessage(`{}`)

	spLatencies := make([]time.Duration, 0, iterations)
	spStart := time.Now()

	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("p0-sproc-%d", i)
		t0 := time.Now()
		tx, err := spConn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("sproc begin %d: %w", i, err)
		}
		var uid [16]byte
		var version, seq int64
		var changed bool
		err = tx.QueryRow(ctx,
			"SELECT * FROM pgctl_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
			"spec", gvk, "phase0-sproc", name, 1, holder, epoch,
			int64(0), false, spec, status, metadata, nil,
		).Scan(&uid, &version, &seq, &changed)
		if err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			return fmt.Errorf("sproc call %d: %w", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("sproc commit %d: %w", i, err)
		}
		spLatencies = append(spLatencies, time.Since(t0))
	}

	spElapsed := time.Since(spStart)
	sort.Slice(spLatencies, func(i, j int) bool { return spLatencies[i] < spLatencies[j] })

	result.StoredProcRPS = float64(iterations) / spElapsed.Seconds()
	result.StoredProcP50 = percentile(spLatencies, 0.50)
	result.StoredProcP99 = percentile(spLatencies, 0.99)

	if result.GoWriterP50 > 0 {
		result.RoundTripSavings = (1.0 - float64(result.StoredProcP50)/float64(result.GoWriterP50)) * 100
	}

	return nil
}

// runWritesWithLatencies is like runSingleThreadedWrites but returns sorted latencies.
func runWritesWithLatencies(ctx context.Context, dsn string, iterations int, gvk, holder string, ttl time.Duration, namespace string) ([]time.Duration, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "TRUNCATE kubernetes_resources, gvk_bucket_counters, bucket_leases"); err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}

	mgr := lease.NewSpecManager(conn, holder).WithMetrics(libLeaseMetrics)
	epoch, err := mgr.Acquire(ctx, 1, ttl)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}

	wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
	spec := json.RawMessage(`{"phase0": true}`)
	status := json.RawMessage(`{}`)
	metadata := json.RawMessage(`{}`)

	latencies := make([]time.Duration, 0, iterations)
	for i := 0; i < iterations; i++ {
		req := model.WriteRequest{
			GVK:         gvk,
			Namespace:   namespace,
			Name:        fmt.Sprintf("p0-%s-%d", namespace, i),
			BucketID:    1,
			Spec:        spec,
			Status:      status,
			Metadata:    metadata,
			LeaseHolder: holder,
			LeaseEpoch:  epoch,
		}
		t0 := time.Now()
		if _, err := wr.Write(ctx, req); err != nil {
			return nil, fmt.Errorf("write %d: %w", i, err)
		}
		latencies = append(latencies, time.Since(t0))
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies, nil
}

// --- pg_stat_statements ---

type pgStatRow struct {
	query     string
	calls     int64
	totalTime float64
}

func snapshotPgStat(ctx context.Context, conn *pgx.Conn) (map[string]pgStatRow, error) {
	rows, err := conn.Query(ctx, `
		SELECT query, calls, total_exec_time
		FROM pg_stat_statements
		WHERE query LIKE '%kubernetes_resources%'
		   OR query LIKE '%gvk_bucket_counters%'
		   OR query LIKE '%bucket_leases%'
		   OR query LIKE '%pg_notify%'
		ORDER BY total_exec_time DESC
		LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]pgStatRow)
	for rows.Next() {
		var r pgStatRow
		if err := rows.Scan(&r.query, &r.calls, &r.totalTime); err != nil {
			return nil, err
		}
		result[r.query] = r
	}
	return result, rows.Err()
}

func diffPgStat(before, after map[string]pgStatRow) []PgStatEntry {
	var entries []PgStatEntry
	for query, a := range after {
		b := before[query]
		callsDelta := a.calls - b.calls
		timeDelta := a.totalTime - b.totalTime
		if callsDelta <= 0 {
			continue
		}
		entries = append(entries, PgStatEntry{
			Query:       query,
			Calls:       callsDelta,
			MeanTimeMs:  timeDelta / float64(callsDelta),
			TotalTimeMs: timeDelta,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TotalTimeMs > entries[j].TotalTimeMs })
	return entries
}
