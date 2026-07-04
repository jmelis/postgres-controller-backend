package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/reader"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)

	result, err := reader.List(context.Background(), conn, "apps/v1/Deployment", []int{1, 2})
	require.NoError(t, err)
	assert.Empty(t, result.Resources)
	assert.Equal(t, int64(1), result.ResourceVersion.Epoch)
	assert.Equal(t, int64(0), result.ResourceVersion.Buckets[1])
	assert.Equal(t, int64(0), result.ResourceVersion.Buckets[2])
}

func TestListReturnsLiveResources(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	for i := 0; i < 3; i++ {
		req := model.WriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default",
			Name: fmt.Sprintf("deploy-%d", i), BucketID: 1,
			Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch,
		}
		_, err := w.Write(ctx, req)
		require.NoError(t, err)
	}

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", []int{1})
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
	assert.Equal(t, int64(3), result.ResourceVersion.Buckets[1])

	// Resources come back ordered by seq
	for i, r := range result.Resources {
		assert.Equal(t, int64(i+1), r.GVKBucketSeq)
	}
}

func TestListExcludesTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	// Create a live resource
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "live",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch,
	})
	require.NoError(t, err)

	// Create a tombstone (deletion_timestamp set)
	now := time.Now()
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "deleted",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), DeletionTimestamp: &now,
		LeaseHolder: "replica-1", LeaseEpoch: epoch,
	})
	require.NoError(t, err)

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", []int{1})
	require.NoError(t, err)
	assert.Len(t, result.Resources, 1)
	assert.Equal(t, "live", result.Resources[0].Name)
	// RV reflects both writes even though tombstone is excluded from results
	assert.Equal(t, int64(2), result.ResourceVersion.Buckets[1])
}

func TestListMultipleBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "replica-1")
	epoch1, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	epoch2, err := mgr.Acquire(ctx, 2, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "ns1", Name: "a",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch1,
	})
	require.NoError(t, err)

	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "ns2", Name: "b",
		BucketID: 2, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), LeaseHolder: "replica-1", LeaseEpoch: epoch2,
	})
	require.NoError(t, err)

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", []int{1, 2})
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	assert.Equal(t, int64(1), result.ResourceVersion.Buckets[1])
	assert.Equal(t, int64(1), result.ResourceVersion.Buckets[2])
}
