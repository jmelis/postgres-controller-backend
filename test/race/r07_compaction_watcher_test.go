package race_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/pkg/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R7 — Compaction vs. slow watcher (I5).
// Watcher resumes with hwm just below a freshly advanced horizon.
// Defense: horizon advanced transactionally with the delete; boundary check on poll.
// Test: freeze a watcher, compact past its hwm, resume; assert 410 Gone.
func TestR7_CompactionVsSlowWatcher(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Write some resources, some as tombstones with old timestamps
	wr := newWriter(t, nil)
	for i := 0; i < 3; i++ {
		past := time.Now().Add(-48 * time.Hour)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("compact-victim-%d", i))
		req.DeletionTimestamp = &past
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Backdate updated_at so GREATEST(deletion_timestamp, updated_at) is old enough to compact
	backdateConn := freshConn(t)
	_, err := backdateConn.Exec(ctx, `UPDATE kubernetes_resources SET updated_at = deletion_timestamp WHERE name LIKE 'compact-victim-%'`)
	require.NoError(t, err)
	backdateConn.Close(context.Background())

	// Write a live resource
	_, err = wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"survivor"))
	require.NoError(t, err)

	// Compact — removes the 3 tombstones, advances horizon
	compactConn := freshConn(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.Deleted)

	// Get the compacted_xid to start watcher below it
	var compactedXid int64
	err = compactConn.QueryRow(ctx,
		`SELECT compacted_xid FROM compaction_horizon WHERE gvk = 'apps/v1/Deployment'`,
	).Scan(&compactedXid)
	require.NoError(t, err)
	require.Greater(t, compactedXid, int64(0))

	// Now start a watcher from hwm=1 (below the compaction horizon)
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		StartRV:          resourceversion.RV{Watermark: 1},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	// The watcher should return with a 410 Gone error
	select {
	case err := <-done:
		assert.ErrorIs(t, err, reader.ErrGone, "watcher must get 410 Gone when hwm < compacted_xid")
	case <-time.After(3 * time.Second):
		watchCancel()
		<-done
		t.Fatal("watcher did not return 410 Gone within 3s")
	}

	watchCancel()
	pollConn.Close(context.Background())
}

// Test the boundary case: hwm == horizon exactly must succeed
func TestR7_CompactionBoundary_Exact(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wr := newWriter(t, nil)
	past := time.Now().Add(-48 * time.Hour)
	req := makeWriteReq("apps/v1/Deployment", "default", "compact-exact")
	req.DeletionTimestamp = &past
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Backdate updated_at so GREATEST(deletion_timestamp, updated_at) is old enough to compact
	backdateConn2 := freshConn(t)
	_, err = backdateConn2.Exec(ctx, `UPDATE kubernetes_resources SET updated_at = deletion_timestamp WHERE name = 'compact-exact'`)
	require.NoError(t, err)
	backdateConn2.Close(context.Background())

	// Write live resource
	_, err = wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"exact-survivor"))
	require.NoError(t, err)

	// Compact: horizon at the tombstone's txid
	compactConn := freshConn(t)
	_, err = compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)

	// Get the compacted_xid (== horizon)
	var compactedXid int64
	err = compactConn.QueryRow(ctx,
		`SELECT compacted_xid FROM compaction_horizon WHERE gvk = 'apps/v1/Deployment'`,
	).Scan(&compactedXid)
	require.NoError(t, err)

	// Watcher with hwm == horizon exactly: must succeed, not 410
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		StartRV:          resourceversion.RV{Watermark: uint64(compactedXid)},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	// Should receive the survivor event, not 410
	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Greater(t, ev.Resource.TxidStamp, uint64(0))
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for event at hwm==horizon boundary")
	}
}
