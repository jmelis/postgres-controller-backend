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

// R4 — First-write txid acquisition.
// Two transactions race to create resources for the same GVK.
// Defense: pg_current_xact_id() provides globally unique, monotonically
// increasing transaction IDs without any counter table.
// Assert: both writes succeed, txids are unique, positive, and ordered.
func TestR4_FirstWriteTxid(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	w1 := newWriter(t, nil)
	w2 := newWriter(t, nil)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		txids []uint64
		errs  []error
	)

	barrier := make(chan struct{})

	for i, w := range []*writer.Writer{w1, w2} {
		wg.Add(1)
		go func(w2 *writer.Writer, idx int) {
			defer wg.Done()
			<-barrier
			req := makeWriteReq("apps/v1/Deployment", "default",
				fmt.Sprintf("resource-%d", idx))
			result, err := w2.Write(ctx, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			} else {
				txids = append(txids, result.Txid)
			}
		}(w, i)
	}

	close(barrier)
	wg.Wait()

	require.Empty(t, errs, "both writes must succeed")
	require.Len(t, txids, 2, "must have two txids")

	// Both txids must be non-zero
	for _, txid := range txids {
		assert.Greater(t, txid, uint64(0), "txid must be positive")
	}

	// Txids must be unique
	sort.Slice(txids, func(i, j int) bool { return txids[i] < txids[j] })
	assert.NotEqual(t, txids[0], txids[1], "txids must be unique")

	// Verify monotonic ordering (smaller txid committed first)
	assert.Less(t, txids[0], txids[1], "txids must be monotonically ordered")
}
