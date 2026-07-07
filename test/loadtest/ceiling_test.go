package loadtest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

func TestCeiling_BucketScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("ceiling test skipped in short mode")
	}

	bucketConfigs := []struct {
		buckets        int
		workersPerBucket int
	}{
		{1, 10},
		{1, 50},
		{2, 25},
		{4, 12},
		{8, 6},
		{16, 3},
		{30, 2},
	}

	for _, bc := range bucketConfigs {
		name := fmt.Sprintf("%db_%dw", bc.buckets, bc.workersPerBucket)
		t.Run(name, func(t *testing.T) {
			truncateAll(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			totalWorkers := bc.buckets * bc.workersPerBucket
			const (
				gvk          = "apps/v1/Deployment"
				testDuration = 5 * time.Second
			)

			var totalWrites atomic.Int64
			var totalErrors atomic.Int64
			var allLatencies sync.Map // bucket -> []time.Duration
			var wg sync.WaitGroup

			start := time.Now()
			deadline := start.Add(testDuration)

			workerID := 0
			for b := 1; b <= bc.buckets; b++ {
				for w := 0; w < bc.workersPerBucket; w++ {
					wg.Add(1)
					go func(wID, bucketID int) {
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
								BucketID: bucketID, Spec: json.RawMessage(`{}`),
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
					}(workerID, b)
					workerID++
				}
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

			t.Logf("buckets=%-2d  workers=%-3d  writes=%-6d  RPS=%-8.1f  p50=%-12v  p99=%-12v  errors=%d",
				bc.buckets, totalWorkers, total, rps, p50, p99, errs)
		})
	}
}
