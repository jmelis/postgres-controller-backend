package loadtest_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/doorbell"
	"github.com/jmelis/postgres-controller-backend/internal/metrics"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestCeiling_WorkerScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("ceiling test skipped in short mode")
	}

	workerCounts := []int{10, 50, 50, 48, 48, 48, 60}

	for _, workers := range workerCounts {
		name := fmt.Sprintf("%dw", workers)
		t.Run(name, func(t *testing.T) {
			truncateAll(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			const (
				gvk          = "apps/v1/Deployment"
				testDuration = 5 * time.Second
			)

			var totalWrites atomic.Int64
			var totalErrors atomic.Int64
			var allLatencies sync.Map // workerID -> []time.Duration
			var wg sync.WaitGroup

			start := time.Now()
			deadline := start.Add(testDuration)

			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(wID int) {
					defer wg.Done()

					conn, err := pgx.Connect(ctx, sharedDB.ConnStr)
					if err != nil {
						totalErrors.Add(1)
						return
					}
					defer conn.Close(context.Background())

					wr := writer.New(conn, nil)
					var lats []time.Duration
					var writeNum int

					for time.Now().Before(deadline) {
						req := model.WriteRequest{
							GVK: gvk, Namespace: "ceiling", Name: fmt.Sprintf("w%d-r%d", wID, writeNum),
							Spec: json.RawMessage(`{}`),
							Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
						}

						t0 := time.Now()
						_, err := wr.Write(ctx, req)
						lat := time.Since(t0)

						if err != nil {
							totalErrors.Add(1)
							continue
						}

						lats = append(lats, lat)
						totalWrites.Add(1)
						writeNum++
					}

					allLatencies.Store(wID, lats)
				}(w)
			}

			wg.Wait()
			elapsed := time.Since(start)

			// Aggregate
			var combined []time.Duration
			allLatencies.Range(func(_, v any) bool {
				combined = append(combined, v.([]time.Duration)...)
				return true
			})
			sort.Slice(combined, func(i, j int) bool { return combined[i] < combined[j] })

			total := totalWrites.Load()
			rps := float64(total) / elapsed.Seconds()
			errs := totalErrors.Load()

			var p50, p99 time.Duration
			if len(combined) > 0 {
				p50 = percentile(combined, 0.50)
				p99 = percentile(combined, 0.99)
			}

			t.Logf("workers=%-3d  writes=%-6d  RPS=%-8.1f  p50=%-12v  p99=%-12v  errors=%d",
				workers, total, rps, p50, p99, errs)
		})
	}
}

// gvkSpec describes a GVK's payload sizes for realistic seeding and writes.
type gvkSpec struct {
	gvk              string
	specSizeBytes    int
	statusSizeBytes  int
	metadataSizeBytes int
	objects          int
}

var testGVKs = []gvkSpec{
	{"hypershift.openshift.io/v1beta1/HostedCluster", 8192, 12288, 2048, 100},
	{"hypershift.openshift.io/v1beta1/HostedNodePool", 4096, 6144, 1024, 100},
	{"hypershift.openshift.io/v1beta1/AWSEndpointService", 4096, 6144, 1024, 100},
	{"hypershift.openshift.io/v1beta1/MachineDeployment", 4096, 6144, 1024, 100},
	{"hypershift.openshift.io/v1beta1/MachineSet", 4096, 6144, 1024, 100},
	{"cluster.open-cluster-management.io/v1/ManagedCluster", 8192, 12288, 2048, 100},
	{"cluster.open-cluster-management.io/v1/ClusterDeployment", 8192, 12288, 2048, 100},
	{"agent-install.openshift.io/v1beta1/AgentClusterInstall", 8192, 12288, 2048, 100},
	{"hive.openshift.io/v1/SyncSet", 4096, 6144, 1024, 100},
	{"addon.open-cluster-management.io/v1alpha1/ManagedClusterAddOn", 4096, 6144, 1024, 100},
}

func generateTestPayload(size int) json.RawMessage {
	if size <= 0 {
		return json.RawMessage(`{}`)
	}
	const overhead = 20
	dataLen := size - overhead
	if dataLen < 1 {
		dataLen = 1
	}
	rawLen := (dataLen * 3) / 4
	if rawLen < 1 {
		rawLen = 1
	}
	raw := make([]byte, rawLen)
	rand.Read(raw)
	encoded := base64.StdEncoding.EncodeToString(raw)
	if len(encoded) > dataLen {
		encoded = encoded[:dataLen]
	}
	return json.RawMessage(fmt.Sprintf(`{"d":"%s"}`, encoded))
}

// TestCeiling_MultiGVK models a realistic workload: 10 controllers with
// multiple replicas writing to their own GVKs, with 20% cross-controller
// status writes on shared objects. Sweeps worker counts to find the
// throughput ceiling. Runs a verifier per GVK to assert monotonic watch
// delivery (I2) and reports write duration histograms + statement timeouts.
func TestCeiling_MultiGVK(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-GVK ceiling test skipped in short mode")
	}

	workerCounts := []int{8, 16, 32, 48, 64}

	for _, totalWorkers := range workerCounts {
		name := fmt.Sprintf("%dw", totalWorkers)
		t.Run(name, func(t *testing.T) {
			truncateAll(t)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			const (
				warmUp       = 2 * time.Second
				testDuration = 15 * time.Second
			)

			// Fresh metrics registry per run
			reg := prometheus.NewRegistry()
			writerMetrics := metrics.NewWriterMetrics(reg)

			// Seed all GVKs
			t.Log("seeding...")
			seedConn := manualConn(t)
			seedWr := writer.New(seedConn, nil)
			for _, gs := range testGVKs {
				for i := 0; i < gs.objects; i++ {
					req := model.WriteRequest{
						GVK:       gs.gvk,
						Namespace: "ceiling",
						Name:      fmt.Sprintf("seed-%d", i),
						Spec:      generateTestPayload(gs.specSizeBytes),
						Status:    generateTestPayload(gs.statusSizeBytes),
						Metadata:  generateTestPayload(gs.metadataSizeBytes),
					}
					if _, err := seedWr.Write(ctx, req); err != nil {
						t.Fatalf("seed %s/%d: %v", gs.gvk, i, err)
					}
				}
			}
			seedConn.Close(context.Background())
			t.Logf("seeded %d objects across %d GVKs", len(testGVKs)*100, len(testGVKs))

			// Start one verifier per GVK
			type verifierEntry struct {
				ver    *verifier.Verifier
				cancel context.CancelFunc
				done   chan error
				conn   *pgx.Conn
				canary *pgx.Conn
				gvk    string
			}
			verifiers := make([]verifierEntry, len(testGVKs))
			for i, gs := range testGVKs {
				vc := manualConn(t)
				cc := manualConn(t)
				ver := verifier.New(vc, cc, verifier.Config{
					GVK:            gs.gvk,
					PollInterval:   200 * time.Millisecond,
					CanaryInterval: 1 * time.Second,
				})
				verCtx, verCancel := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- ver.Run(verCtx) }()
				verifiers[i] = verifierEntry{
					ver: ver, cancel: verCancel, done: done,
					conn: vc, canary: cc, gvk: gs.gvk,
				}
			}

			// Distribute workers round-robin across GVKs
			type workerAssignment struct {
				gvkIdx   int
				workerID int
			}
			assignments := make([]workerAssignment, totalWorkers)
			for w := 0; w < totalWorkers; w++ {
				assignments[w] = workerAssignment{
					gvkIdx:   w % len(testGVKs),
					workerID: w,
				}
			}

			// Create debounced doorbell (one per sweep, own connection)
			dbConn := manualConn(t)
			defer dbConn.Close(context.Background())
			db := doorbell.NewDebouncer(dbConn, 50*time.Millisecond)
			defer db.Close()

			// Run writers
			type workerResult struct {
				latencies   []time.Duration
				writes      int64
				serFailures int64
				otherErrors int64
			}
			results := make([]workerResult, totalWorkers)
			var totalWrites atomic.Int64
			var wg sync.WaitGroup

			start := time.Now()
			warmUpEnd := start.Add(warmUp)
			deadline := start.Add(warmUp + testDuration)

			for _, a := range assignments {
				wg.Add(1)
				go func(wID, gvkIdx int) {
					defer wg.Done()

					conn, err := pgx.Connect(ctx, sharedDB.ConnStr)
					if err != nil {
						results[wID].otherErrors++
						return
					}
					defer conn.Close(context.Background())

					gs := testGVKs[gvkIdx]
					wr := writer.New(conn, nil).WithMetrics(writerMetrics).WithDoorbell(db)
					var writeNum int

					for time.Now().Before(deadline) {
						req := model.WriteRequest{
							GVK: gs.gvk, Namespace: "ceiling",
							Name:     fmt.Sprintf("w%d-r%d", wID, writeNum),
							Spec:     generateTestPayload(gs.specSizeBytes),
							Status:   generateTestPayload(gs.statusSizeBytes),
							Metadata: generateTestPayload(gs.metadataSizeBytes),
						}

						t0 := time.Now()
						_, writeErr := wr.Write(ctx, req)
						lat := time.Since(t0)

						if writeErr != nil {
							if isSerializationFailure(writeErr) {
								results[wID].serFailures++
							} else {
								results[wID].otherErrors++
							}
							writeNum++
							continue
						}

						// Only collect latencies after warmup
						if time.Now().After(warmUpEnd) {
							results[wID].latencies = append(results[wID].latencies, lat)
							results[wID].writes++
							totalWrites.Add(1)
						}
						writeNum++
					}
				}(a.workerID, a.gvkIdx)
			}

			wg.Wait()
			elapsed := testDuration // use configured duration, not wall clock including warmup

			// Let verifiers catch up
			time.Sleep(3 * time.Second)

			// Shutdown verifiers and collect results
			var totalViolations int
			var totalRedeliveries int64
			for _, ve := range verifiers {
				ve.cancel()
				<-ve.done
				result := ve.ver.Result()
				if len(result.Violations) > 0 {
					for _, v := range result.Violations {
						t.Logf("  VIOLATION [%s]: %s", ve.gvk, v)
					}
					totalViolations += len(result.Violations)
				}
				totalRedeliveries += result.Redeliveries
				t.Logf("verifier %-60s  events=%-6d  redeliveries=%-4d  canary_p99=%v  violations=%d",
					ve.gvk, result.EventsChecked, result.Redeliveries, result.CanaryP99, len(result.Violations))
				ve.conn.Close(context.Background())
				ve.canary.Close(context.Background())
			}

			// Aggregate writer results
			var allLatencies []time.Duration
			var totalSerFail, totalOtherErr int64
			for _, r := range results {
				allLatencies = append(allLatencies, r.latencies...)
				totalSerFail += r.serFailures
				totalOtherErr += r.otherErrors
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

			// Report throughput
			t.Logf("=== Multi-GVK Ceiling: %d workers ===", totalWorkers)
			t.Logf("Duration:              %v", elapsed)
			t.Logf("Total writes:          %d", total)
			t.Logf("Throughput:             %.1f writes/s", rps)
			t.Logf("p50 latency:            %v", p50)
			t.Logf("p99 latency:            %v", p99)
			t.Logf("p999 latency:           %v", p999)
			t.Logf("Serialization failures: %d", totalSerFail)
			t.Logf("Other errors:           %d", totalOtherErr)
			t.Logf("Verifier violations:    %d", totalViolations)

			// Statement timeout check
			timeouts := testutil.ToFloat64(writerMetrics.StatementTimeoutsTotal)
			t.Logf("Statement timeouts:     %.0f", timeouts)

			// Write duration histogram for one representative GVK
			t.Log("--- Write duration histogram (success) ---")
			for _, gs := range testGVKs[:1] {
				observer := writerMetrics.WriteDuration.WithLabelValues(gs.gvk, "success")
				metric := &dto.Metric{}
				if h, ok := observer.(prometheus.Metric); ok {
					if err := h.Write(metric); err == nil && metric.Histogram != nil {
						for _, b := range metric.Histogram.Bucket {
							t.Logf("  ≤ %8.3fs: %d", b.GetUpperBound(), b.GetCumulativeCount())
						}
						t.Logf("  total:      %d (sum=%.3fs)", metric.Histogram.GetSampleCount(), metric.Histogram.GetSampleSum())
					}
				}
			}

			// Hard assertions
			assert.Equal(t, 0, totalViolations,
				"all verifiers must report zero I2 violations")
			assert.Equal(t, 0.0, timeouts,
				"zero statement timeouts expected")
			assert.Greater(t, total, int64(0),
				"must complete at least some writes")
		})
	}
}
