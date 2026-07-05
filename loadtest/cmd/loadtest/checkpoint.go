package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Checkpoint is a periodic progress snapshot written during long-running tests.
type Checkpoint struct {
	Timestamp       time.Time     `json:"timestamp"`
	Elapsed         time.Duration `json:"elapsed"`
	SpecName        string        `json:"spec_name"`
	CompletedPhases []PhaseResult `json:"completed_phases"`
	CurrentPhase    *PhaseProgress `json:"current_phase,omitempty"`
	AllPassedSoFar  bool          `json:"all_passed_so_far"`
}

// PhaseProgress tracks the in-flight state of a currently running phase.
type PhaseProgress struct {
	Name        string           `json:"name"`
	StartedAt   time.Time        `json:"started_at"`
	Elapsed     time.Duration    `json:"elapsed"`
	TotalWrites int64            `json:"total_writes"`
	CurrentRPS  float64          `json:"current_rps"`
	Errors      map[string]int64 `json:"errors"`
}

// CheckpointWriter periodically writes checkpoint files so long-running tests
// can be inspected mid-run.
type CheckpointWriter struct {
	mu              sync.Mutex
	specName        string
	startTime       time.Time
	completedPhases []PhaseResult
	currentPhase    *PhaseProgress
	allPassedSoFar  bool
	path            string
}

func NewCheckpointWriter(specName, path string) *CheckpointWriter {
	return &CheckpointWriter{
		specName:       specName,
		startTime:      time.Now(),
		allPassedSoFar: true,
		path:           path,
	}
}

// RecordPhaseComplete adds a completed phase result and updates the pass state.
func (cw *CheckpointWriter) RecordPhaseComplete(result PhaseResult) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.completedPhases = append(cw.completedPhases, result)
	if !result.Passed {
		cw.allPassedSoFar = false
	}
	cw.currentPhase = nil
}

// SetCurrentPhase updates the in-flight phase progress.
func (cw *CheckpointWriter) SetCurrentPhase(progress *PhaseProgress) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.currentPhase = progress
}

// Write writes the current checkpoint to disk and logs a summary.
func (cw *CheckpointWriter) Write() {
	cw.mu.Lock()
	now := time.Now()
	cp := Checkpoint{
		Timestamp:       now,
		Elapsed:         now.Sub(cw.startTime),
		SpecName:        cw.specName,
		CompletedPhases: cw.completedPhases,
		CurrentPhase:    cw.currentPhase,
		AllPassedSoFar:  cw.allPassedSoFar,
	}
	cw.mu.Unlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		log.Printf("checkpoint: marshal error: %v", err)
		return
	}

	if err := os.WriteFile(cw.path, data, 0644); err != nil {
		log.Printf("checkpoint: write error: %v", err)
		return
	}

	// Log summary
	completed := len(cp.CompletedPhases)
	status := "all passing"
	if !cp.AllPassedSoFar {
		status = "FAILURES detected"
	}
	current := "idle"
	if cp.CurrentPhase != nil {
		current = fmt.Sprintf("%s (%.1f w/s, %s elapsed)",
			cp.CurrentPhase.Name, cp.CurrentPhase.CurrentRPS,
			cp.CurrentPhase.Elapsed.Round(time.Second))
	}
	log.Printf("checkpoint: %d phases complete, %s | current: %s | %s",
		completed, status, current, cp.Elapsed.Round(time.Second))
}
