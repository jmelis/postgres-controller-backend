package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/schema"
)

func main() {
	specPath := flag.String("spec", "", "path to load test YAML spec")
	dsn := flag.String("dsn", "", "PostgreSQL connection string (overrides PGCTL_DSN)")
	metricsAddr := flag.String("metrics-addr", ":9090", "address for Prometheus metrics server")
	reportPath := flag.String("report", "", "path to write JSON report (optional)")
	flag.Parse()

	// Resolve DSN.
	connStr := *dsn
	if connStr == "" {
		connStr = os.Getenv("PGCTL_DSN")
	}
	if connStr == "" {
		log.Fatal("--dsn flag or PGCTL_DSN environment variable required")
	}

	// Load spec.
	if *specPath == "" {
		log.Fatal("--spec flag required")
	}
	cfg, err := LoadConfig(*specPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	log.Printf("load test: %s — %s", cfg.Metadata.Name, cfg.Metadata.Description)
	log.Printf("cluster: %d buckets", cfg.Cluster.Buckets)
	log.Printf("seed: %d total objects", cfg.ComputeTotalObjects())
	log.Printf("checkpoint interval: %v", cfg.CheckpointInterval)

	// Start metrics server.
	StartMetricsServer(*metricsAddr)

	// Connect to Postgres and migrate.
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}

	log.Printf("running schema migration...")
	if err := schema.Migrate(ctx, conn); err != nil {
		log.Fatalf("schema migration failed: %v", err)
	}
	log.Printf("schema migration complete")

	// Clear previous run data and seed fresh.
	if len(cfg.Seed.GVKs) > 0 && cfg.ComputeTotalObjects() > 0 {
		log.Printf("clearing previous data...")
		if _, err := conn.Exec(ctx, "TRUNCATE kubernetes_resources, gvk_bucket_counters, compaction_horizon"); err != nil {
			log.Fatalf("truncate failed: %v", err)
		}
		log.Printf("seeding data...")
		if err := Seed(ctx, conn, cfg); err != nil {
			log.Fatalf("seed failed: %v", err)
		}
	}
	conn.Close(context.Background())

	// Set up checkpoint writer.
	checkpointPath := "/results/checkpoint.json"
	if *reportPath != "" {
		checkpointPath = filepath.Join(filepath.Dir(*reportPath), "checkpoint.json")
	}
	cpWriter := NewCheckpointWriter(cfg.Metadata.Name, checkpointPath)

	// Start checkpoint ticker.
	cpTicker := time.NewTicker(cfg.CheckpointInterval)
	cpDone := make(chan struct{})
	go func() {
		defer cpTicker.Stop()
		for {
			select {
			case <-cpTicker.C:
				cpWriter.Write()
			case <-cpDone:
				return
			}
		}
	}()

	// Start compaction goroutine for long-running tests.
	compactConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatalf("compaction conn: %v", err)
	}
	compactDone := make(chan struct{})
	go func() {
		defer compactConn.Close(context.Background())
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				res, err := compaction.Compact(ctx, compactConn, compaction.Config{
					Retention: 1 * time.Hour,
				})
				if err != nil {
					log.Printf("compaction error: %v", err)
				} else if res.Deleted > 0 {
					log.Printf("compaction: deleted %d tombstones", res.Deleted)
				}
			case <-compactDone:
				return
			}
		}
	}()

	// Run phases.
	report := &Report{
		SpecName:    cfg.Metadata.Name,
		Description: cfg.Metadata.Description,
		StartTime:   time.Now(),
	}

	allPassed := true
	phases := []struct {
		name    string
		enabled bool
		run     func(ctx context.Context, dsn string, cfg *Config) (*PhaseResult, error)
	}{
		{"phase0_baseline", cfg.Phases.Phase0Baseline.Enabled, RunPhase0},
		{"phase1_ceiling", cfg.Phases.Phase1Ceiling.Enabled, RunPhase1},
		{"phase2_steady", cfg.Phases.Phase2Steady.Enabled, RunPhase2},
		{"phase2b_skew", cfg.Phases.Phase2bSkew.Enabled, RunPhase2b},
		{"phase3_avalanche", cfg.Phases.Phase3Avalanche.Enabled, RunPhase3},
		{"phase5_poll", cfg.Phases.Phase5Poll.Enabled, RunPhase5},
	}

	for _, phase := range phases {
		if !phase.enabled {
			log.Printf("skipping %s (disabled)", phase.name)
			continue
		}

		log.Printf("=== starting %s ===", phase.name)
		phaseStart := time.Now()
		cpWriter.SetCurrentPhase(&PhaseProgress{
			Name:      phase.name,
			StartedAt: phaseStart,
		})

		phaseCtx := context.Background()
		result, err := phase.run(phaseCtx, connStr, cfg)
		if err != nil {
			log.Printf("phase %s error: %v", phase.name, err)
			result = &PhaseResult{
				Name:   phase.name,
				Passed: false,
				Errors: map[string]int64{"fatal": 1},
			}
		}

		report.Phases = append(report.Phases, *result)
		cpWriter.RecordPhaseComplete(*result)
		if !result.Passed {
			allPassed = false
		}

		log.Printf("=== %s: %s ===", phase.name, passFailStr(result.Passed))

		// Write a checkpoint after each phase completes.
		cpWriter.Write()
	}

	close(cpDone)
	close(compactDone)

	report.EndTime = time.Now()
	report.Duration = report.EndTime.Sub(report.StartTime)
	report.AllPassed = allPassed

	// Output.
	PrintSummary(report)

	if *reportPath != "" {
		if err := WriteJSON(report, *reportPath); err != nil {
			log.Printf("WARNING: failed to write report: %v", err)
		} else {
			log.Printf("report written to %s", *reportPath)
		}
	}

	if !allPassed {
		fmt.Fprintln(os.Stderr, "FAIL: one or more phases did not pass")
		os.Exit(1)
	}

	log.Printf("all phases passed")
}

func passFailStr(passed bool) string {
	if passed {
		return "PASSED"
	}
	return "FAILED"
}
