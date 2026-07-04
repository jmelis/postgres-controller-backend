package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectManual creates a connection NOT managed by t.Cleanup — the caller
// must close it manually after the watcher goroutine has exited.
func connectManual(t *testing.T, db *testinfra.TestDB) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), db.ConnStr)
	require.NoError(t, err)
	return conn
}

// runWatcher starts the watcher in a goroutine and returns a done channel.
// The caller should cancel ctx, then wait on done before closing connections.
func runWatcher(w *reader.Watcher, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()
	return done
}

func TestWatchReceivesWrittenEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch, err := mgr.Acquire(ctx, 1, 60*time.Second)
	require.NoError(t, err)

	pollConn := connectManual(t, db)
	listenConn := connectManual(t, db)

	w := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		BucketIDs:        []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 500 * time.Millisecond,
		DebounceFloor:    50 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := runWatcher(w, watchCtx)
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	writerConn := db.Connect(t)
	wr := writer.New(writerConn, nil)
	for i := 0; i < 3; i++ {
		_, err := wr.Write(ctx, model.WriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default",
			Name: fmt.Sprintf("deploy-%d", i), BucketID: 1,
			Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch,
		})
		require.NoError(t, err)
	}

	var events []reader.Event
	deadline := time.After(5 * time.Second)
	for len(events) < 3 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout waiting for events, got %d", len(events))
		}
	}

	assert.Len(t, events, 3)
	for i, ev := range events {
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Equal(t, int64(i+1), ev.Resource.GVKBucketSeq)
	}
}

func TestWatchDetectsDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch, err := mgr.Acquire(ctx, 1, 60*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	wr := writer.New(writerConn, nil)

	result, err := wr.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "to-delete",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch,
	})
	require.NoError(t, err)

	now := time.Now()
	_, err = wr.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "to-delete",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), DeletionTimestamp: &now,
		ExpectedVersion: result.ObjectVersion,
		LeaseHolder:     "replica-1", LeaseEpoch: epoch,
	})
	require.NoError(t, err)

	pollConn := connectManual(t, db)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 500 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := runWatcher(w, watchCtx)
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	// PK is (gvk, namespace, name), so only ONE row exists — at seq=2 with deletion.
	// Watcher from hwm=0 sees seq=2 DELETED.
	var events []reader.Event
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
			if len(events) >= 2 {
				goto done2
			}
		case <-deadline:
			goto done2
		}
	}
done2:
	require.Len(t, events, 1)
	assert.Equal(t, reader.EventDeleted, events[0].Type)
	assert.Equal(t, int64(2), events[0].Resource.GVKBucketSeq)
}

func TestWatchBaselinePollDelivers(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch, err := mgr.Acquire(ctx, 1, 60*time.Second)
	require.NoError(t, err)

	pollConn := connectManual(t, db)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK: "apps/v1/Deployment", BucketIDs: []int{1},
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: map[int]int64{1: 0}},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := runWatcher(w, watchCtx)
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	writerConn := db.Connect(t)
	wr := writer.New(writerConn, nil)
	_, err = wr.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "baseline-test",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch,
	})
	require.NoError(t, err)

	select {
	case ev := <-w.Events():
		assert.Equal(t, reader.EventAdded, ev.Type)
		assert.Equal(t, "baseline-test", ev.Resource.Name)
	case <-time.After(2 * time.Second):
		t.Fatal("baseline poll did not deliver event within 2s")
	}
}
