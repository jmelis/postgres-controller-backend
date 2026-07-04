package race_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RB4a — Identical content write consumes no sequence number (I1/I5).
// Write an object, then write identical content. Assert: second write returns
// Changed=false, ObjectVersion unchanged, counter unchanged.
func TestRB4a_IdenticalWriteSuppressed(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	w := newWriter(t, nil)

	req := makeWriteReq("apps/v1/Deployment", "default", "noop-test", 1, "holder-a", epoch)
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r1.Changed, "first write must be changed")
	assert.Equal(t, int64(1), r1.Seq)

	conn := freshConn(t)
	var counterAfterFirst int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counterAfterFirst)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counterAfterFirst)

	// Write identical content — must be suppressed.
	req.ExpectedVersion = r1.ObjectVersion
	r2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.False(t, r2.Changed, "identical write must be suppressed")
	assert.Equal(t, r1.ObjectVersion, r2.ObjectVersion, "object version must not change")
	assert.Equal(t, r1.UID, r2.UID, "UID must be preserved")
	assert.Equal(t, int64(0), r2.Seq, "suppressed write must not consume a seq")

	// Counter must not have advanced.
	var counterAfterSecond int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counterAfterSecond)
	require.NoError(t, err)
	assert.Equal(t, counterAfterFirst, counterAfterSecond, "counter must not advance on suppressed write")
	conn.Close(ctx)
}

// RB4b — Real change after a no-op is correctly sequenced (I1/I2).
// Write A, write A again (no-op), then write B. Assert: B gets next seq,
// watcher sees exactly one event.
func TestRB4b_RealChangeAfterNoOp(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	w := newWriter(t, nil)

	// Write initial content.
	req := makeWriteReq("apps/v1/Deployment", "default", "noop-seq", 1, "holder-a", epoch)
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r1.Changed)

	// No-op write (identical content).
	req.ExpectedVersion = r1.ObjectVersion
	r2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.False(t, r2.Changed, "no-op must be suppressed")

	// Real change.
	req.Spec = json.RawMessage(`{"replicas":99}`)
	req.ExpectedVersion = r2.ObjectVersion
	r3, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r3.Changed, "real change must not be suppressed")
	assert.Equal(t, int64(2), r3.Seq, "real change must get the next seq after the first write")
	assert.Equal(t, r1.ObjectVersion+1, r3.ObjectVersion, "version must increment exactly once")
}

// RB4c — Watcher sees no event for a suppressed write (I5).
func TestRB4c_WatcherSeesNoEventForSuppressed(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	w := newWriter(t, nil)

	// Create initial object.
	req := makeWriteReq("apps/v1/Deployment", "default", "noop-watch", 1, "holder-a", epoch)
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Start watcher from current position.
	watchConn := freshConn(t)
	listenConn := freshConn(t)

	startRV := resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: r1.Seq}}
	watcher := reader.NewWatcher(watchConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          startRV,
		BaselineInterval: 100 * time.Millisecond,
	}, nil)

	watchCtx, cancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() { errCh <- watcher.Run(watchCtx) }()

	// No-op write.
	req.ExpectedVersion = r1.ObjectVersion
	r2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.False(t, r2.Changed)

	// Wait long enough that the watcher would have polled.
	time.Sleep(300 * time.Millisecond)

	// Now do a real change so we know the watcher is alive.
	req.Spec = json.RawMessage(`{"replicas":42}`)
	req.ExpectedVersion = r2.ObjectVersion
	r3, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r3.Changed)

	// The only event the watcher should see is the real change.
	select {
	case ev := <-watcher.Events():
		assert.Equal(t, reader.EventModified, ev.Type)
		assert.Equal(t, r3.Seq, ev.Resource.GVKBucketSeq, "event must be for the real change")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watcher event")
	}

	// Stop watcher before closing connections to avoid data race.
	cancel()
	<-errCh
	watchConn.Close(ctx)
	listenConn.Close(ctx)
}

// RB4d — Create-path suppression: replayed create with identical content (I1).
func TestRB4d_ReplayedCreateSuppressed(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	w := newWriter(t, nil)

	req := makeWriteReq("apps/v1/Deployment", "default", "replay-create", 1, "holder-a", epoch)
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r1.Changed)

	// Replay: same content, ExpectedVersion=0 (create attempt).
	req.ExpectedVersion = 0
	r2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.False(t, r2.Changed, "replayed create with identical content must be suppressed")
	assert.Equal(t, r1.ObjectVersion, r2.ObjectVersion)
	assert.Equal(t, r1.UID, r2.UID)

	conn := freshConn(t)
	var counter int64
	err = conn.QueryRow(ctx,
		`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&counter)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counter, "counter must not advance on replayed create")
	conn.Close(ctx)
}

// RB4e — WriteStatus suppression (I1/I5).
func TestRB4e_WriteStatusSuppressed(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	specEpoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	statusEpoch := setupStatusLease(t, 1, "status-holder", 60_000_000_000)
	w := newWriter(t, nil)

	// Create the object first via spec write.
	createReq := makeWriteReq("apps/v1/Deployment", "default", "status-noop", 1, "holder-a", specEpoch)
	r1, err := w.Write(ctx, createReq)
	require.NoError(t, err)
	assert.True(t, r1.Changed)

	// Status write.
	statusReq := model.StatusWriteRequest{
		GVK:             "apps/v1/Deployment",
		Namespace:       "default",
		Name:            "status-noop",
		BucketID:        1,
		Status:          json.RawMessage(`{"ready":true}`),
		ExpectedVersion: r1.ObjectVersion,
		LeaseHolder:     "status-holder",
		LeaseEpoch:      statusEpoch,
	}
	r2, err := w.WriteStatus(ctx, statusReq)
	require.NoError(t, err)
	assert.True(t, r2.Changed)

	// Identical status write — must be suppressed.
	statusReq.ExpectedVersion = r2.ObjectVersion
	r3, err := w.WriteStatus(ctx, statusReq)
	require.NoError(t, err)
	assert.False(t, r3.Changed, "identical status write must be suppressed")
	assert.Equal(t, r2.ObjectVersion, r3.ObjectVersion)
}

// RB4f — ForceWrite bypasses suppression.
func TestRB4f_ForceWriteBypassesSuppression(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)
	w := newWriter(t, nil)

	req := makeWriteReq("apps/v1/Deployment", "default", "force-test", 1, "holder-a", epoch)
	r1, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Force write with identical content.
	req.ExpectedVersion = r1.ObjectVersion
	req.ForceWrite = true
	r2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.True(t, r2.Changed, "ForceWrite must bypass suppression")
	assert.Equal(t, int64(2), r2.Seq, "forced write must consume a seq")
	assert.Equal(t, r1.ObjectVersion+1, r2.ObjectVersion)
}

// RB4g — No-op suppression under the fence: interleave a grant between
// suppression check and commit. The suppressed write holds FOR SHARE,
// so the grant must block until the suppressed txn commits.
func TestRB4g_SuppressionUnderFence(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Hook that blocks after suppression check.
	hook := &afterSuppressionBlockingHook{
		ready:   make(chan struct{}),
		proceed: make(chan struct{}),
	}
	writerA := writer.New(freshConn(t), hook)

	grantConn := freshConn(t)
	coordinator := lease.NewSpecManager(grantConn, "coordinator")

	// First write to create the object.
	plainWriter := newWriter(t, nil)
	req := makeWriteReq("apps/v1/Deployment", "default", "fence-suppress", 1, "holder-a", epoch)
	r1, err := plainWriter.Write(ctx, req)
	require.NoError(t, err)

	// Suppressed write in goroutine — will pause after suppression check.
	type result struct {
		res model.WriteResult
		err error
	}
	aCh := make(chan result, 1)
	go func() {
		req2 := makeWriteReq("apps/v1/Deployment", "default", "fence-suppress", 1, "holder-a", epoch)
		req2.ExpectedVersion = r1.ObjectVersion
		r, err := writerA.Write(ctx, req2)
		aCh <- result{r, err}
	}()

	// Wait for A to reach the hook (holds FOR SHARE).
	select {
	case <-hook.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for suppression hook")
	}

	// Grant must block because A holds FOR SHARE.
	bCh := make(chan error, 1)
	go func() {
		_, err := coordinator.Grant(ctx, 1, "holder-b", 60*time.Second)
		bCh <- err
	}()

	select {
	case err := <-bCh:
		t.Fatalf("Grant completed while FOR SHARE held — I4 violated (err=%v)", err)
	case <-time.After(500 * time.Millisecond):
		// Expected: blocked.
	}

	// Unblock A.
	close(hook.proceed)

	aResult := <-aCh
	require.NoError(t, aResult.err)
	assert.False(t, aResult.res.Changed, "write must still be suppressed")

	// Grant completes.
	require.NoError(t, <-bCh)
}

// afterSuppressionBlockingHook blocks at AfterSuppressionCheck.
type afterSuppressionBlockingHook struct {
	ready   chan struct{}
	proceed chan struct{}
}

func (h *afterSuppressionBlockingHook) AfterFence(_ context.Context, _ pgx.Tx) error { return nil }
func (h *afterSuppressionBlockingHook) AfterSuppressionCheck(ctx context.Context, _ pgx.Tx, _ bool) error {
	close(h.ready)
	select {
	case <-h.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *afterSuppressionBlockingHook) AfterCounter(_ context.Context, _ pgx.Tx, _ int64) error {
	return nil
}
func (h *afterSuppressionBlockingHook) BeforeCommit(_ context.Context, _ pgx.Tx) error { return nil }
