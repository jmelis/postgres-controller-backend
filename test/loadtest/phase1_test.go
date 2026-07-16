package loadtest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sharedDB *testinfra.TestDB

func TestMain(m *testing.M) {
	sharedDB = testinfra.StartPostgresForTestMain()
	code := m.Run()
	sharedDB.Stop()
	os.Exit(code)
}

func freshConn(t *testing.T) *pgx.Conn {
	t.Helper()
	return sharedDB.Connect(t)
}

func manualConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	return conn
}

func truncateAll(t *testing.T) {
	t.Helper()
	conn := freshConn(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())
}

// Phase 1 — Write ceiling (§7).
// 50 workers, one GVK, single Postgres instance.
// Criteria:
//   - ≥200 commits/s sustained
//   - p99 ≤ 10ms
//   - Zero serialization failures
//   - Verifier silent (no invariant violations)
func TestPhase1_CounterCeiling(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in short mode")
	}

	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		numWorkers   = 50
		gvk          = "apps/v1/Deployment"
		testDuration = 10 * time.Second
	)

	// Start verifier
	verifyConn := manualConn(t)
	ver := verifier.New(verifyConn, nil, verifier.Config{
		GVK:          gvk,
		PollInterval: 200 * time.Millisecond,
	})
	verCtx, verCancel := context.WithCancel(ctx)
	verDone := make(chan error, 1)
	go func() { verDone <- ver.Run(verCtx) }()

	// Per-worker latency collection
	type workerResult struct {
		latencies   []time.Duration
		writes      int64
		serFailures int64
		otherErrors int64
	}

	results := make([]workerResult, numWorkers)

	var totalWrites atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	deadline := start.Add(testDuration)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := pgx.Connect(ctx, sharedDB.ConnStr)
			if err != nil {
				results[workerID].otherErrors++
				return
			}
			defer conn.Close(context.Background())

			wr := writer.New(conn, nil)
			var writeNum int

			for time.Now().Before(deadline) {
				name := fmt.Sprintf("w%d-r%d", workerID, writeNum)
				req := model.WriteRequest{
					GVK: gvk, Namespace: "loadtest", Name: name,
					Spec: json.RawMessage(`{"w":` + fmt.Sprintf("%d", workerID) + `}`),
					Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
				}

				t0 := time.Now()
				_, writeErr := wr.Write(ctx, req)
				lat := time.Since(t0)

				if writeErr != nil {
					if isSerializationFailure(writeErr) {
						results[workerID].serFailures++
					} else {
						results[workerID].otherErrors++
					}
					continue
				}

				results[workerID].latencies = append(results[workerID].latencies, lat)
				results[workerID].writes++
				totalWrites.Add(1)
				writeNum++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Let verifier catch up
	time.Sleep(2 * time.Second)
	verCancel()
	<-verDone
	verifyConn.Close(context.Background())

	// Aggregate results
	var allLatencies []time.Duration
	var totalSerFail, totalOtherErr int64
	for _, r := range results {
		allLatencies = append(allLatencies, r.latencies...)
		totalSerFail += r.serFailures
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

	verResult := ver.Result()

	t.Logf("=== Phase 1 Load Test Results ===")
	t.Logf("Duration:         %v", elapsed.Round(time.Millisecond))
	t.Logf("Workers:          %d", numWorkers)
	t.Logf("Total writes:     %d", total)
	t.Logf("Throughput:        %.1f writes/s", rps)
	t.Logf("p50 latency:       %v", p50)
	t.Logf("p99 latency:       %v", p99)
	t.Logf("p99.9 latency:     %v", p999)
	t.Logf("Serialization failures: %d", totalSerFail)
	t.Logf("Other errors:      %d", totalOtherErr)
	t.Logf("Verifier events:   %d", verResult.EventsChecked)
	t.Logf("Verifier violations: %d", len(verResult.Violations))
	for _, v := range verResult.Violations {
		t.Logf("  VIOLATION: %s", v)
	}

	// Assertions: §7 Phase 1 criteria
	// Correctness gates (hard — must pass everywhere)
	assert.GreaterOrEqual(t, rps, 200.0,
		"throughput must be ≥200 writes/s (got %.1f)", rps)
	assert.Equal(t, int64(0), totalSerFail,
		"zero serialization failures required")
	assert.Empty(t, verResult.Violations,
		"verifier must report zero violations")

	// Latency gate (strict mode: set PGCTL_STRICT_LATENCY=1 for production-tuned PG)
	if os.Getenv("PGCTL_STRICT_LATENCY") == "1" {
		assert.LessOrEqual(t, p99.Milliseconds(), int64(10),
			"p99 must be ≤10ms (got %v)", p99)
	} else {
		t.Logf("NOTE: p99=%v — strict latency gate skipped (set PGCTL_STRICT_LATENCY=1 for §7 criteria)", p99)
	}
}

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

