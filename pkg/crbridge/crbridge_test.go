package crbridge_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/pkg/crbridge"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sharedDB *testinfra.TestDB

func TestMain(m *testing.M) {
	sharedDB = testinfra.StartPostgresForTestMain()
	code := m.Run()
	sharedDB.Stop()
	os.Exit(code)
}

func connFactory(t *testing.T) func() (*pgx.Conn, error) {
	t.Helper()
	return func() (*pgx.Conn, error) {
		return pgx.Connect(context.Background(), sharedDB.ConnStr)
	}
}

func truncateAll(t *testing.T) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())
}

func setupLeases(t *testing.T, bucketID int, holder string) int64 {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	mgr := lease.NewBothManager(conn, holder)
	epochs, err := mgr.AcquireBoth(context.Background(), bucketID, 60*time.Second)
	require.NoError(t, err)
	require.Equal(t, epochs.Spec, epochs.Status)
	return epochs.Spec
}

func staticAssigner(bucketID int) crbridge.BucketAssigner {
	return func(_, _ string) int { return bucketID }
}

func TestClient_CreateGetUpdateDelete(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		ns       = "default"
		name     = "nginx"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	spec := json.RawMessage(`{"replicas":3}`)
	status := json.RawMessage(`{"available":0}`)
	meta := json.RawMessage(`{}`)

	// Create
	obj, err := c.Create(context.Background(), ns, name, spec, status, meta)
	require.NoError(t, err)
	assert.Equal(t, ns, obj.Namespace)
	assert.Equal(t, name, obj.Name)
	assert.Equal(t, "1", obj.ResourceVersion)
	assert.False(t, obj.Deleted)

	// Get
	got, err := c.Get(context.Background(), ns, name)
	require.NoError(t, err)
	assert.Equal(t, "1", got.ResourceVersion)
	assert.JSONEq(t, `{"replicas":3}`, string(got.Spec))
	assert.JSONEq(t, `{"available":0}`, string(got.Status))

	// Update
	got.Spec = json.RawMessage(`{"replicas":5}`)
	updated, err := c.Update(context.Background(), got)
	require.NoError(t, err)
	assert.Equal(t, "2", updated.ResourceVersion)

	got2, err := c.Get(context.Background(), ns, name)
	require.NoError(t, err)
	assert.JSONEq(t, `{"replicas":5}`, string(got2.Spec))

	// Delete
	err = c.Delete(context.Background(), got2)
	require.NoError(t, err)

	_, err = c.Get(context.Background(), ns, name)
	assert.True(t, errors.Is(err, crbridge.ErrNotFound))
}

func TestClient_CreateConflict(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		ns       = "default"
		name     = "dup"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	spec := json.RawMessage(`{"replicas":1}`)

	_, err := c.Create(context.Background(), ns, name, spec, json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.NoError(t, err)

	_, err = c.Create(context.Background(), ns, name, spec, json.RawMessage(`{}`), json.RawMessage(`{}`))
	assert.True(t, errors.Is(err, crbridge.ErrAlreadyExists))
}

func TestClient_UpdateConflict(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		ns       = "default"
		name     = "stale"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	spec := json.RawMessage(`{"v":1}`)
	_, err := c.Create(context.Background(), ns, name, spec, json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.NoError(t, err)

	obj, err := c.Get(context.Background(), ns, name)
	require.NoError(t, err)

	// First update succeeds
	obj.Spec = json.RawMessage(`{"v":2}`)
	_, err = c.Update(context.Background(), obj)
	require.NoError(t, err)

	// Same version again → stale
	obj.Spec = json.RawMessage(`{"v":3}`)
	_, err = c.Update(context.Background(), obj)
	assert.True(t, errors.Is(err, crbridge.ErrConflict))
}

func TestClient_StatusUpdate(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		ns       = "default"
		name     = "with-status"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	spec := json.RawMessage(`{"replicas":1}`)
	_, err := c.Create(context.Background(), ns, name, spec, json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.NoError(t, err)

	obj, err := c.Get(context.Background(), ns, name)
	require.NoError(t, err)

	newStatus := json.RawMessage(`{"available":1}`)
	updated, err := c.Status().Update(obj, newStatus)
	require.NoError(t, err)
	assert.Equal(t, "2", updated.ResourceVersion)

	got, err := c.Get(context.Background(), ns, name)
	require.NoError(t, err)
	assert.JSONEq(t, `{"available":1}`, string(got.Status))
	// Spec unchanged
	assert.JSONEq(t, `{"replicas":1}`, string(got.Spec))
}

func TestListerWatcher_ListAndWatch(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		ns       = "default"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	// Pre-populate
	for _, name := range []string{"a", "b", "c"} {
		_, err := c.Create(context.Background(), ns, name,
			json.RawMessage(`{"n":"`+name+`"}`), json.RawMessage(`{}`), json.RawMessage(`{}`))
		require.NoError(t, err)
	}

	lw := crbridge.NewListerWatcher(connFactory(t), gvk, []int{bucketID})

	// List
	result, err := lw.List(context.Background())
	require.NoError(t, err)
	assert.Len(t, result.Objects, 3)
	assert.NotEmpty(t, result.ResourceVersion)

	// Watch from list RV
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wi, err := lw.Watch(ctx, result.ResourceVersion)
	require.NoError(t, err)
	defer wi.Stop()

	// Write a new resource to trigger a watch event
	_, err = c.Create(context.Background(), ns, "d",
		json.RawMessage(`{"n":"d"}`), json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.NoError(t, err)

	select {
	case ev := <-wi.ResultChan():
		assert.Equal(t, crbridge.EventAdded, ev.Type)
		assert.Equal(t, "d", ev.Object.Name)
	case <-ctx.Done():
		t.Fatal("timed out waiting for watch event")
	}
}

func TestListerWatcher_WatchEpochMismatch_410(t *testing.T) {
	truncateAll(t)

	const (
		gvk      = "apps/v1/Deployment"
		bucketID = 1
		holder   = "test-holder"
	)

	epoch := setupLeases(t, bucketID, holder)
	c := crbridge.NewClient(connFactory(t), gvk, staticAssigner(bucketID), holder, epoch)

	// Create one resource so there's a counter
	_, err := c.Create(context.Background(), "ns", "obj",
		json.RawMessage(`{}`), json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.NoError(t, err)

	lw := crbridge.NewListerWatcher(connFactory(t), gvk, []int{bucketID})

	result, err := lw.List(context.Background())
	require.NoError(t, err)

	// Bump cluster epoch to force 410 on next poll
	adminConn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	defer adminConn.Close(context.Background())
	_, err = adminConn.Exec(context.Background(), `UPDATE cluster_epoch SET timeline_id = timeline_id + 1`)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wi, err := lw.Watch(ctx, result.ResourceVersion)
	if err != nil {
		// Watch may fail immediately on initial poll with epoch mismatch
		assert.Contains(t, err.Error(), "epoch mismatch")
		return
	}
	defer wi.Stop()

	// The watcher should terminate and close its channel on epoch mismatch.
	// Drain any buffered events until the channel closes.
	for {
		select {
		case _, ok := <-wi.ResultChan():
			if !ok {
				return // channel closed = watcher terminated (410)
			}
		case <-ctx.Done():
			t.Fatal("timed out — expected watcher to terminate on epoch mismatch")
		}
	}
}

func TestNoExposedInternalTypes(t *testing.T) {
	// Compile-time check: the crbridge public API must not expose pgx, model,
	// or resourceversion types. This test is a documentation assertion — if it
	// compiles, it passes.
	var _ crbridge.BucketAssigner = func(_, _ string) int { return 0 }
	var _ crbridge.WatchInterface
	var _ *crbridge.Object
	var _ *crbridge.ListResult
	var _ *crbridge.Client
	var _ *crbridge.ListerWatcher
	var _ *crbridge.StatusClient
	var _ crbridge.EventType
}
