package race_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R18 — Watcher restart/resume: simulates a controller redeploy. Watcher A
// receives events for 5 writes; cancel A; 5 more writes while no watcher runs;
// watcher B resumes from A's HWM. B must deliver exactly the 5 new events.

func TestR18_WatcherResume(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// ── Watcher A ──────────────────────────────────────────────────────────
	pollConnA := connectManualShared(t)
	listenConnA := connectManualShared(t)

	watcherA := reader.NewWatcher(pollConnA, listenConnA, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	ctxA, cancelA := context.WithCancel(ctx)
	doneA := make(chan error, 1)
	go func() { doneA <- watcherA.Run(ctxA) }()

	// Write 5 resources.
	for i := 0; i < 5; i++ {
		wr := newWriter(t, nil)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("resume-%d", i), 1, "holder-a", epoch)
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Collect all 5 events from watcher A.
	var eventsA []reader.Event
	deadlineA := time.After(5 * time.Second)
	for len(eventsA) < 5 {
		select {
		case ev := <-watcherA.Events():
			eventsA = append(eventsA, ev)
		case <-deadlineA:
			cancelA()
			<-doneA
			pollConnA.Close(context.Background())
			listenConnA.Close(context.Background())
			t.Fatalf("watcher A timeout: got %d/5 events", len(eventsA))
		}
	}

	require.Len(t, eventsA, 5, "watcher A must receive exactly 5 events")

	// Cancel watcher A and wait for it to stop.
	cancelA()
	<-doneA

	// Capture A's high-water marks before closing connections.
	hwmA := watcherA.HWM()

	pollConnA.Close(context.Background())
	listenConnA.Close(context.Background())

	// ── Gap: no watcher running, 5 more writes ────────────────────────────
	for i := 5; i < 10; i++ {
		wr := newWriter(t, nil)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("resume-%d", i), 1, "holder-a", epoch)
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// ── Watcher B resumes from A's HWM ────────────────────────────────────
	pollConnB := connectManualShared(t)
	listenConnB := connectManualShared(t)

	watcherB := reader.NewWatcher(pollConnB, listenConnB, reader.WatcherConfig{
		GVK:       "apps/v1/Deployment",
		BucketIDs: []int{1},
		StartRV: resourceversion.RV{
			Epoch:   1,
			Buckets: hwmA, // resume from A's high-water marks
		},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	ctxB, cancelB := context.WithCancel(ctx)
	doneB := make(chan error, 1)
	go func() { doneB <- watcherB.Run(ctxB) }()
	defer func() {
		cancelB()
		<-doneB
		pollConnB.Close(context.Background())
		listenConnB.Close(context.Background())
	}()

	// Collect events from watcher B — expect exactly 5 new events.
	var eventsB []reader.Event
	deadlineB := time.After(5 * time.Second)
	for len(eventsB) < 5 {
		select {
		case ev := <-watcherB.Events():
			eventsB = append(eventsB, ev)
		case <-deadlineB:
			t.Fatalf("watcher B timeout: got %d/5 events", len(eventsB))
		}
	}

	require.Len(t, eventsB, 5, "watcher B must receive exactly 5 new events")

	// Verify B received only seqs 6-10 (the gap writes).
	for _, ev := range eventsB {
		assert.GreaterOrEqual(t, ev.Resource.GVKBucketSeq, int64(6),
			"watcher B must not replay A's events")
		assert.LessOrEqual(t, ev.Resource.GVKBucketSeq, int64(10),
			"unexpected seq beyond 10")
	}

	// Verify the union of A and B covers exactly {1..10} with no duplicates.
	seenSeqs := make(map[int64]bool)
	for _, ev := range eventsA {
		assert.False(t, seenSeqs[ev.Resource.GVKBucketSeq],
			"duplicate seq %d in watcher A", ev.Resource.GVKBucketSeq)
		seenSeqs[ev.Resource.GVKBucketSeq] = true
	}
	for _, ev := range eventsB {
		assert.False(t, seenSeqs[ev.Resource.GVKBucketSeq],
			"duplicate seq %d across A and B", ev.Resource.GVKBucketSeq)
		seenSeqs[ev.Resource.GVKBucketSeq] = true
	}
	for seq := int64(1); seq <= 10; seq++ {
		assert.True(t, seenSeqs[seq],
			"seq %d missing from union of A + B events", seq)
	}
}
