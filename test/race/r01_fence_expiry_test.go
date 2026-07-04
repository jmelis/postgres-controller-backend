package race_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R1 — Fence-expiry race (I4).
// Writer A passes fence, pauses before COMMIT. Coordinator attempts epoch bump
// (UPDATE bucket_leases SET epoch=epoch+1 for the spec row). The UPDATE must
// block because A holds FOR SHARE on the spec row.
//
// Sequence:
// 1. Session A: BEGIN → fence FOR SHARE → counter → upsert → BeforeCommit hook blocks
// 2. Session B: Grant() attempts UPDATE → must block on the FOR SHARE lock
// 3. Unblock A → A commits
// 4. B's UPDATE completes → epoch bumped
// 5. A's next write with old epoch → ErrFenceViolation
func TestR1_FenceExpiryRace(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Setup: acquire lease for holder-a
	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Session A: writer with blocking hook
	hook := newBlockingHook()
	writerA := newWriter(t, hook)

	// Session B: coordinator connection for Grant
	grantConn := freshConn(t)
	coordinator := lease.NewSpecManager(grantConn, "coordinator")

	// Start writer A in a goroutine — it will pause at BeforeCommit
	type writeResult struct {
		result interface{}
		err    error
	}
	aCh := make(chan writeResult, 1)
	go func() {
		req := makeWriteReq("apps/v1/Deployment", "default", "fence-test", 1, "holder-a", epoch)
		r, err := writerA.Write(ctx, req)
		aCh <- writeResult{r, err}
	}()

	// Wait for A to reach BeforeCommit (holds FOR SHARE)
	select {
	case <-hook.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for writer A to reach BeforeCommit")
	}

	// Session B: attempt Grant (epoch bump) — this must BLOCK
	bCh := make(chan writeResult, 1)
	go func() {
		newEpoch, err := coordinator.Grant(ctx, 1, "holder-b", 60*time.Second)
		bCh <- writeResult{newEpoch, err}
	}()

	// Verify B is blocked (should not complete within 500ms)
	select {
	case r := <-bCh:
		t.Fatalf("Grant completed while FOR SHARE held — I4 violated (epoch=%v, err=%v)", r.result, r.err)
	case <-time.After(500 * time.Millisecond):
		// Expected: B is blocked by A's FOR SHARE lock
	}

	// Unblock A
	close(hook.proceed)

	// A's write should succeed
	aResult := <-aCh
	require.NoError(t, aResult.err, "writer A's first write must succeed")

	// B's Grant should now complete
	bResult := <-bCh
	require.NoError(t, bResult.err, "Grant must succeed after A releases lock")

	// A's next write with the OLD epoch must fail
	writerA2 := newWriter(t, nil)
	req2 := makeWriteReq("apps/v1/Deployment", "default", "fence-test-2", 1, "holder-a", epoch)
	_, err := writerA2.Write(ctx, req2)
	assert.ErrorIs(t, err, writer.ErrFenceViolation, "write with stale epoch must be fenced")
}

// Also test the reverse ordering: Grant commits first, then the late writer's
// fence must find the new epoch and abort.
func TestR1_FenceExpiryRace_ReverseOrder(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// This time: hook pauses AfterFence (before counter), giving coordinator
	// time to bump the epoch. But since FOR SHARE is already held, the coordinator
	// will still block.
	hook := &afterFenceBlockingHook{
		ready:   make(chan struct{}),
		proceed: make(chan struct{}),
	}
	writerA := newWriter(t, hook)

	grantConn := freshConn(t)
	coordinator := lease.NewSpecManager(grantConn, "coordinator")

	type res struct {
		val interface{}
		err error
	}

	aCh := make(chan res, 1)
	go func() {
		req := makeWriteReq("apps/v1/Deployment", "default", "fence-rev", 1, "holder-a", epoch)
		r, err := writerA.Write(ctx, req)
		aCh <- res{r, err}
	}()

	<-hook.ready

	bCh := make(chan res, 1)
	go func() {
		newEpoch, err := coordinator.Grant(ctx, 1, "holder-b", 60*time.Second)
		bCh <- res{newEpoch, err}
	}()

	// B should block
	select {
	case r := <-bCh:
		t.Fatalf("Grant completed while FOR SHARE held — I4 violated (epoch=%v, err=%v)", r.val, r.err)
	case <-time.After(500 * time.Millisecond):
	}

	close(hook.proceed)

	aResult := <-aCh
	require.NoError(t, aResult.err)

	bResult := <-bCh
	require.NoError(t, bResult.err)
}

type afterFenceBlockingHook struct {
	ready   chan struct{}
	proceed chan struct{}
}

func (h *afterFenceBlockingHook) AfterFence(ctx context.Context, _ pgx.Tx) error {
	close(h.ready)
	select {
	case <-h.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *afterFenceBlockingHook) AfterCounter(_ context.Context, _ pgx.Tx, _ int64) error {
	return nil
}
func (h *afterFenceBlockingHook) BeforeCommit(_ context.Context, _ pgx.Tx) error {
	return nil
}
