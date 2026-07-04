package race_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R4 — Counter first-write race (I1).
// Two transactions race the counter's first INSERT for the same (bucket, GVK).
// Defense: ON CONFLICT upsert under the unique PK.
// Assert: seqs are exactly {1, 2}, counter ends at 2.
func TestR4_CounterFirstWriteRace(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	w1 := newWriter(t, nil)
	w2 := newWriter(t, nil)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seqs []int64
		errs []error
	)

	barrier := make(chan struct{})

	for i, w := range []*writer.Writer{w1, w2} {
		wg.Add(1)
		go func(w2 *writer.Writer, idx int) {
			defer wg.Done()
			<-barrier
			req := makeWriteReq("apps/v1/Deployment", "default",
				fmt.Sprintf("resource-%d", idx), 1, "holder-a", epoch)
			result, err := w2.Write(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			} else {
				seqs = append(seqs, result.Seq)
			}
		}(w, i)
	}

	close(barrier)
	wg.Wait()

	require.Empty(t, errs, "both writes must succeed")
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	assert.Equal(t, []int64{1, 2}, seqs)

	// Verify counter value
	conn := freshConn(t)
	var counterVal int64
	err := conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counterVal)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counterVal)
}
