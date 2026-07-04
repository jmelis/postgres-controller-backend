package race_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R14 — Debounced 410 swallowing (B2: I6/I7).
//
// debouncedPoll discards the return value of w.poll. If a doorbell-triggered
// poll detects an epoch mismatch (or sub-horizon hwm), the 410 Gone error is
// silently dropped and the watcher keeps running as if healthy.
//
// Defense (Phase 2): single-goroutine scheduler where every poll error is
// handled uniformly — ErrGone always terminates Run.
//
// Expected current failure: watcher keeps running after 410; test times out
// waiting for termination.
func TestR14_Debounced410(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Write a resource so the initial poll has work
	wr := newWriter(t, nil)
	_, err := wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"r14-test", 1, "holder-a", epoch))
	require.NoError(t, err)

	pollConn := connectManualShared(t)
	listenConn := connectManualShared(t)

	// Long baseline so only the doorbell triggers polls
	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 60 * time.Second,
		DebounceFloor:    50 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	runExited := false
	defer func() {
		watchCancel()
		if !runExited {
			<-done
		}
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	// Drain the initial event
	select {
	case <-w.Events():
	case <-time.After(3 * time.Second):
		t.Fatal("initial event not delivered")
	}

	// Bump the timeline epoch — next poll must detect mismatch
	adminConn := freshConn(t)
	_, err = adminConn.Exec(ctx, `UPDATE cluster_epoch SET timeline_id = timeline_id + 1`)
	require.NoError(t, err)
	adminConn.Close(context.Background())

	// Wait past DebounceFloor so the next doorbell triggers a leading-edge poll
	time.Sleep(200 * time.Millisecond)

	// Write another resource — the doorbell fires, debouncedPoll calls poll(),
	// which detects the epoch mismatch and returns ErrGone.
	// (The write itself succeeds because the fence checks bucket_leases (spec row),
	// not cluster_epoch.)
	wr2 := newWriter(t, nil)
	_, _ = wr2.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"r14-trigger", 1, "holder-a", epoch))

	// The watcher must terminate with ErrGone.
	// B2 bug: debouncedPoll discards the error; watcher keeps running.
	select {
	case err := <-done:
		runExited = true
		assert.ErrorIs(t, err, reader.ErrGone,
			"watcher must terminate with 410 Gone after epoch mismatch on doorbell-triggered poll")
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not terminate with 410 Gone within 3s " +
			"(B2: debouncedPoll swallowed the error)")
	}
}
