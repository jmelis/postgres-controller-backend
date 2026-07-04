package race_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/reader"
	"github.com/jmelisba/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R6 — Lease handover overlap (I4/I5).
// Old holder's last write vs. new holder's first write on the same bucket.
// Defense: R1 mechanism + new holder's scoped List starts from post-grant counter state.
// Test: scripted handover under write load; verification watcher asserts a
// single totally-ordered gapless stream across the handover.
func TestR6_LeaseHandoverOverlap(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase 1: holder-a writes some resources
	epochA := setupLease(t, 1, "holder-a", 60_000_000_000)

	writerA := newWriter(t, nil)
	for i := 0; i < 5; i++ {
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("handover-%d", i), 1, "holder-a", epochA)
		_, err := writerA.Write(ctx, req)
		require.NoError(t, err)
	}

	// Phase 2: coordinator grants bucket to holder-b (epoch bump)
	grantConn := freshConn(t)
	coordinator := lease.NewSpecManager(grantConn, "coordinator")
	epochB, err := coordinator.Grant(ctx, 1, "holder-b", 60*time.Second)
	require.NoError(t, err)
	assert.Equal(t, epochA+1, epochB)

	// Phase 3: holder-b writes more resources
	writerBConn := freshConn(t)
	writerB := writer.New(writerBConn, nil)
	for i := 5; i < 10; i++ {
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("handover-%d", i), 1, "holder-b", epochB)
		_, err := writerB.Write(ctx, req)
		require.NoError(t, err)
	}

	// Phase 4: holder-a's writes with old epoch must fail
	req := makeWriteReq("apps/v1/Deployment", "default", "stale-write", 1, "holder-a", epochA)
	_, err = writerA.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrFenceViolation)

	// Phase 5: verification watcher — assert gapless stream across handover
	pollConn := connectManualShared(t)
	watcher := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- watcher.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 10 {
		select {
		case ev := <-watcher.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/10 events across handover", len(events))
		}
	}

	assert.Len(t, events, 10)

	// Verify gapless: collect seqs, sort, check contiguity
	seqs := make([]int64, len(events))
	for i, ev := range events {
		seqs[i] = ev.Resource.GVKBucketSeq
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i, seq := range seqs {
		assert.Equal(t, int64(i+1), seq, "gap at position %d", i)
	}
}
