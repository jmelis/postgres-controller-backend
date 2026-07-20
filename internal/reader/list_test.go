package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)

	result, err := reader.List(context.Background(), conn, "apps/v1/Deployment", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Resources)
}

func TestListReturnsLiveResources(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	for i := 0; i < 3; i++ {
		req := model.WriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default",
			Name:     fmt.Sprintf("deploy-%d", i),
			Spec:     json.RawMessage(`{}`),
			Status:   json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`),
		}
		_, err := w.Write(ctx, req)
		require.NoError(t, err)
	}

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
	assert.Greater(t, result.ResourceVersion.Watermark, uint64(0))

	// Resources come back ordered by txid_stamp; verify monotonically increasing
	for i := 1; i < len(result.Resources); i++ {
		assert.Greater(t, result.Resources[i].TxidStamp, result.Resources[i-1].TxidStamp)
	}
}

func TestListExcludesTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	// Create a live resource
	_, err := w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "live",
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	})
	require.NoError(t, err)

	// Create a tombstone (deletion_timestamp set, no finalizers)
	now := time.Now()
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "deleted",
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), DeletionTimestamp: &now,
	})
	require.NoError(t, err)

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 1)
	assert.Equal(t, "live", result.Resources[0].Name)
	assert.Greater(t, result.ResourceVersion.Watermark, uint64(0))
}

func TestListIncludesDyingWithFinalizers(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	// Create a live resource
	_, err := w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "live",
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	})
	require.NoError(t, err)

	// Create a dying resource (deletion_timestamp set, has finalizers)
	now := time.Now()
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dying",
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata:          json.RawMessage(`{"finalizers":["cleanup.example.com"]}`),
		DeletionTimestamp: &now,
	})
	require.NoError(t, err)

	// Create a tombstone (deletion_timestamp set, no finalizers)
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "tombstone",
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata:          json.RawMessage(`{}`),
		DeletionTimestamp: &now,
	})
	require.NoError(t, err)

	listConn := db.Connect(t)
	result, err := reader.List(ctx, listConn, "apps/v1/Deployment", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	names := []string{result.Resources[0].Name, result.Resources[1].Name}
	assert.Contains(t, names, "live")
	assert.Contains(t, names, "dying")
	assert.Greater(t, result.ResourceVersion.Watermark, uint64(0))
}
