package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// PhaseResult holds the outcome of a single load test phase.
type PhaseResult struct {
	Name               string            `json:"name"`
	Passed             bool              `json:"passed"`
	Duration           time.Duration     `json:"duration"`
	TotalWrites        int64             `json:"total_writes"`
	RPS                float64           `json:"rps"`
	P50                time.Duration     `json:"p50"`
	P99                time.Duration     `json:"p99"`
	P999               time.Duration     `json:"p999"`
	Errors             map[string]int64  `json:"errors"`
	VerifierViolations []string          `json:"verifier_violations,omitempty"`
	Sweep              []SweepEntry      `json:"sweep,omitempty"`
	Baseline           *BaselineResult   `json:"baseline,omitempty"`
}

// SweepEntry records one data point in a worker-count sweep.
type SweepEntry struct {
	Workers    int           `json:"workers"`
	RPS        float64       `json:"rps"`
	RPSStdDev  float64       `json:"rps_stddev,omitempty"`
	P50        time.Duration `json:"p50"`
	P99        time.Duration `json:"p99"`
	P999       time.Duration `json:"p999"`
	ErrorCount int64         `json:"error_count"`
	Runs       int           `json:"runs,omitempty"`
}

// BaselineResult holds diagnostic measurements from Phase 0.
type BaselineResult struct {
	PingP50  time.Duration `json:"ping_p50"`
	PingP99  time.Duration `json:"ping_p99"`
	PingP999 time.Duration `json:"ping_p999"`

	StepTimings []StepTiming `json:"step_timings"`

	SyncCommitRPS  float64       `json:"sync_commit_rps"`
	AsyncCommitRPS float64       `json:"async_commit_rps"`
	SyncCommitP50  time.Duration `json:"sync_commit_p50"`
	AsyncCommitP50 time.Duration `json:"async_commit_p50"`

	GoWriterRPS      float64       `json:"go_writer_rps"`
	GoWriterP50      time.Duration `json:"go_writer_p50"`
	GoWriterP99      time.Duration `json:"go_writer_p99"`
	StoredProcRPS    float64       `json:"stored_proc_rps"`
	StoredProcP50    time.Duration `json:"stored_proc_p50"`
	StoredProcP99    time.Duration `json:"stored_proc_p99"`
	RoundTripSavings float64       `json:"round_trip_savings_pct"`

	PgStatEntries []PgStatEntry `json:"pg_stat_entries,omitempty"`
}

// StepTiming records latency for one isolated write-path step.
type StepTiming struct {
	Step string        `json:"step"`
	P50  time.Duration `json:"p50"`
	P99  time.Duration `json:"p99"`
	Mean time.Duration `json:"mean"`
}

// PgStatEntry holds a delta from pg_stat_statements for a single query.
type PgStatEntry struct {
	Query       string  `json:"query"`
	Calls       int64   `json:"calls"`
	MeanTimeMs  float64 `json:"mean_time_ms"`
	TotalTimeMs float64 `json:"total_time_ms"`
}

// Report is the full load test report.
type Report struct {
	SpecName    string        `json:"spec_name"`
	Description string        `json:"description"`
	StartTime   time.Time     `json:"start_time"`
	EndTime     time.Time     `json:"end_time"`
	Duration    time.Duration `json:"total_duration"`
	AllPassed   bool          `json:"all_passed"`
	Phases      []PhaseResult `json:"phases"`
}

// WriteJSON marshals the report to JSON and writes it to the given path.
func WriteJSON(report *Report, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write report to %s: %w", path, err)
	}
	return nil
}

// PrintSummary prints a human-readable table of the report to stdout.
func PrintSummary(report *Report) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("  LOAD TEST REPORT: %s\n", report.SpecName)
	fmt.Printf("  %s\n", report.Description)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("  Start:    %s\n", report.StartTime.Format(time.RFC3339))
	fmt.Printf("  End:      %s\n", report.EndTime.Format(time.RFC3339))
	fmt.Printf("  Duration: %s\n", report.Duration.Round(time.Second))
	fmt.Println()

	// Header
	fmt.Printf("  %-20s %-8s %10s %10s %10s %10s %10s\n",
		"Phase", "Result", "Writes", "RPS", "p50", "p99", "p99.9")
	fmt.Println("  " + strings.Repeat("-", 78))

	for _, p := range report.Phases {
		result := "PASS"
		if !p.Passed {
			result = "FAIL"
		}
		fmt.Printf("  %-20s %-8s %10d %10.1f %10s %10s %10s\n",
			p.Name, result, p.TotalWrites, p.RPS,
			p.P50.Round(time.Microsecond),
			p.P99.Round(time.Microsecond),
			p.P999.Round(time.Microsecond),
		)

		// Print errors if any
		for errType, count := range p.Errors {
			if count > 0 {
				fmt.Printf("    error: %-30s %d\n", errType, count)
			}
		}
		// Print violations if any
		for _, v := range p.VerifierViolations {
			fmt.Printf("    VIOLATION: %s\n", v)
		}
		// Print sweep results if any
		if len(p.Sweep) > 0 {
			fmt.Println()
			fmt.Printf("    %-10s %10s %10s %10s %10s %10s %8s\n",
				"Workers", "RPS", "+-stddev", "p50", "p99", "p999", "Errors")
			fmt.Println("    " + strings.Repeat("-", 72))
			for _, s := range p.Sweep {
				fmt.Printf("    %-10d %10.1f %10.1f %10s %10s %10s %8d\n",
					s.Workers, s.RPS, s.RPSStdDev,
					s.P50.Round(time.Microsecond),
					s.P99.Round(time.Microsecond),
					s.P999.Round(time.Microsecond),
					s.ErrorCount,
				)
			}
			fmt.Println()
		}
		if p.Baseline != nil {
			printBaselineSection(p.Baseline)
		}
	}

	fmt.Println()
	if report.AllPassed {
		fmt.Println("  OVERALL: ALL PHASES PASSED")
	} else {
		fmt.Println("  OVERALL: SOME PHASES FAILED")
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()
}

func printBaselineSection(b *BaselineResult) {
	fmt.Println()
	fmt.Println("    --- Baseline Measurements ---")
	fmt.Println()

	fmt.Println("    Network RTT (SELECT 1):")
	fmt.Printf("      p50=%v  p99=%v  p999=%v\n",
		b.PingP50.Round(time.Microsecond),
		b.PingP99.Round(time.Microsecond),
		b.PingP999.Round(time.Microsecond))
	fmt.Println()

	if len(b.StepTimings) > 0 {
		fmt.Println("    Per-Step Isolated Timing:")
		fmt.Printf("      %-25s %10s %10s %10s\n", "Step", "p50", "p99", "mean")
		fmt.Println("      " + strings.Repeat("-", 58))
		for _, s := range b.StepTimings {
			fmt.Printf("      %-25s %10s %10s %10s\n", s.Step,
				s.P50.Round(time.Microsecond),
				s.P99.Round(time.Microsecond),
				s.Mean.Round(time.Microsecond))
		}
		fmt.Println()
	}

	fmt.Println("    Sync Commit Comparison (single-threaded writes):")
	fmt.Printf("      synchronous_commit=on:   %8.1f w/s  p50=%v\n",
		b.SyncCommitRPS, b.SyncCommitP50.Round(time.Microsecond))
	fmt.Printf("      synchronous_commit=off:  %8.1f w/s  p50=%v\n",
		b.AsyncCommitRPS, b.AsyncCommitP50.Round(time.Microsecond))
	if b.SyncCommitRPS > 0 {
		fmt.Printf("      Async speedup: %.1fx\n", b.AsyncCommitRPS/b.SyncCommitRPS)
	}
	fmt.Println()

	fmt.Println("    Stored Procedure vs Go Writer (single-threaded):")
	fmt.Printf("      Go writer (7 RTTs):    %8.1f w/s  p50=%v  p99=%v\n",
		b.GoWriterRPS, b.GoWriterP50.Round(time.Microsecond), b.GoWriterP99.Round(time.Microsecond))
	fmt.Printf("      Stored proc (2 RTTs):  %8.1f w/s  p50=%v  p99=%v\n",
		b.StoredProcRPS, b.StoredProcP50.Round(time.Microsecond), b.StoredProcP99.Round(time.Microsecond))
	fmt.Printf("      Round-trip savings: %.1f%%\n", b.RoundTripSavings)
	fmt.Println()

	if len(b.PgStatEntries) > 0 {
		fmt.Println("    Server-Side Execution (pg_stat_statements delta):")
		fmt.Printf("      %-55s %8s %10s\n", "Query (truncated)", "Calls", "Mean(ms)")
		fmt.Println("      " + strings.Repeat("-", 76))
		for _, e := range b.PgStatEntries {
			q := e.Query
			if len(q) > 53 {
				q = q[:53] + ".."
			}
			fmt.Printf("      %-55s %8d %10.3f\n", q, e.Calls, e.MeanTimeMs)
		}
		fmt.Println()
	}
}
