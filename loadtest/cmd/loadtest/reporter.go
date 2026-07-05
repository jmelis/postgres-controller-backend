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
