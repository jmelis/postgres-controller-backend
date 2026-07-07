package toxirace_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R3 Toxi — Doorbell loss via reset_peer toxic (I3).
// The LISTEN connection goes through the proxy; a reset_peer toxic kills it mid-burst.
// The poll connection goes direct (not through proxy). The watcher must deliver
// all events via baseline poll despite the doorbell being murdered.
func TestR3_Toxi_DoorbellLoss_ResetPeer(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
			fmt.Sprintf("toxi-r3-%d", i), 1)
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
			fmt.Sprintf("toxi-r3-%d", i), 1)
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

// R3 Toxi — Doorbell reconnect via ListenConnFactory.
// The LISTEN connection is killed via reset_peer toxic, then restored.
// The watcher's ListenConnFactory reconnects, and the doorbell fast path is
// restored — proving reconnect works end-to-end.
func TestR3_Toxi_DoorbellReconnect(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// pollConn goes DIRECT (always healthy)
	pollConn, err := pdb.DirectConn(ctx)
	require.NoError(t, err)

	// listenConn goes through PROXY (will be killed and reconnected)
	listenConn, err := pdb.ProxiedConn(ctx)
	require.NoError(t, err)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 10 * time.Second, // long baseline so only doorbell triggers fast delivery
		DebounceFloor:    50 * time.Millisecond,
		ListenConnFactory: func(ctx context.Context) (*pgx.Conn, error) {
			return pdb.ProxiedConn(ctx)
		},
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	time.Sleep(200 * time.Millisecond)

	// Write first resource — doorbell should be working, expect fast delivery
	wr := directWriter(t, nil)
	req := makeWriteReq("apps/v1/Deployment", "default", "reconnect-0", 1)
	_, err = wr.Write(ctx, req)
	require.NoError(t, err)

	select {
	case <-w.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("first event should arrive via doorbell within 2s")
	}

	// Kill the LISTEN connection via reset_peer toxic.
	// reset_peer only fires when data flows, so we write a resource to trigger
	// a pg_notify that travels downstream through the proxy and triggers the RST.
	_, err = pdb.Proxy.AddToxic("kill-reconnect", "reset_peer", "downstream", 1.0, nil)
	require.NoError(t, err)

	wrKill := directWriter(t, nil)
	reqKill := makeWriteReq("apps/v1/Deployment", "default", "reconnect-kill", 1)
	_, err = wrKill.Write(ctx, reqKill)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	// Remove the toxic so reconnect can succeed
	err = pdb.Proxy.RemoveToxic("kill-reconnect")
	require.NoError(t, err)

	// Drain the "reconnect-kill" event that may arrive via baseline poll
	drainDeadline := time.After(5 * time.Second)
	select {
	case <-w.Events():
	case <-drainDeadline:
		t.Fatal("reconnect-kill event should arrive via baseline poll")
	}

	// Wait for reconnect — poll Stats().Reconnects with a deadline
	reconnectDeadline := time.After(15 * time.Second)
	for {
		stats := w.Stats()
		if stats.Reconnects >= 1 {
			break
		}
		select {
		case <-reconnectDeadline:
			t.Fatalf("reconnect did not happen: stats=%+v", w.Stats())
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Write another resource — doorbell should be restored
	wr2 := directWriter(t, nil)
	req2 := makeWriteReq("apps/v1/Deployment", "default", "reconnect-1", 1)
	_, err = wr2.Write(ctx, req2)
	require.NoError(t, err)

	select {
	case <-w.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("post-reconnect event should arrive via restored doorbell within 2s")
	}

	watchCancel()
	<-done
	pollConn.Close(context.Background())
	listenConn.Close(context.Background())

	t.Logf("reconnect stats: %+v", w.Stats())
}
