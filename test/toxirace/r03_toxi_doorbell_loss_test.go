package toxirace_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/reader"
	"github.com/jmelisba/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R3 Toxi — Doorbell loss via reset_peer toxic (I5).
// The LISTEN connection goes through the proxy; a reset_peer toxic kills it mid-burst.
// The poll connection goes direct (not through proxy). The watcher must deliver
// all events via baseline poll despite the doorbell being murdered.
func TestR3_Toxi_DoorbellLoss_ResetPeer(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// pollConn goes DIRECT (bypasses proxy — always healthy)
	pollConn, err := pdb.DirectConn(ctx)
	require.NoError(t, err)

	// listenConn goes through PROXY (will be killed)
	listenConn, err := pdb.ProxiedConn(ctx)
	require.NoError(t, err)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 500 * time.Millisecond,
		DebounceFloor:    50 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	time.Sleep(100 * time.Millisecond)

	// Write first 3 resources (doorbell should be working)
	wr := directWriter(t, nil)
	for i := 0; i < 3; i++ {
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("toxi-r3-%d", i), 1, "holder-a", epoch)
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Collect the first 3 events
	var events []reader.Event
	firstBatch := time.After(3 * time.Second)
	for len(events) < 3 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-firstBatch:
			t.Fatalf("timeout waiting for first batch: got %d/3", len(events))
		}
	}

	// Kill the LISTEN connection via reset_peer toxic
	_, err = pdb.Proxy.AddToxic("kill-listen", "reset_peer", "downstream", 1.0, nil)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Remove the toxic so new proxied connections could work (but the old
	// listen conn is already dead)
	err = pdb.Proxy.RemoveToxic("kill-listen")
	require.NoError(t, err)

	// Write 5 more resources — doorbell is dead, only baseline poll will find these
	wr2 := directWriter(t, nil)
	for i := 3; i < 8; i++ {
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("toxi-r3-%d", i), 1, "holder-a", epoch)
		_, err := wr2.Write(ctx, req)
		require.NoError(t, err)
	}

	// All 5 post-kill events must arrive via baseline poll
	secondBatch := time.After(5 * time.Second)
	for len(events) < 8 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-secondBatch:
			t.Fatalf("timeout waiting for post-kill events: got %d/8", len(events))
		}
	}

	watchCancel()
	<-done
	pollConn.Close(context.Background())
	listenConn.Close(context.Background())

	assert.Len(t, events, 8)

	// Verify contiguous seqs, no duplicates
	seqs := make(map[int64]bool)
	for _, ev := range events {
		assert.False(t, seqs[ev.Resource.GVKBucketSeq],
			"duplicate seq %d", ev.Resource.GVKBucketSeq)
		seqs[ev.Resource.GVKBucketSeq] = true
	}
	for i := int64(1); i <= 8; i++ {
		assert.True(t, seqs[i], "missing seq %d", i)
	}
}
