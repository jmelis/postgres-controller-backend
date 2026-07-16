package compaction_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/pkg/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactDeletesExpiredTombstones(t *testing.T) {
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
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	})
	require.NoError(t, err)

	// Create a tombstone with deletion_timestamp in the past
	past := time.Now().Add(-2 * time.Hour)
	_, err = w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "old-tombstone",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), DeletionTimestamp: &past,
	})
	require.NoError(t, err)

	// Backdate updated_at so GREATEST(deletion_timestamp, updated_at) is old enough to compact
	backdateConn := db.Connect(t)
	_, err = backdateConn.Exec(ctx, `UPDATE kubernetes_resources SET updated_at = deletion_timestamp WHERE name = 'old-tombstone'`)
	require.NoError(t, err)
	backdateConn.Close(ctx)

	// Compact with 1h retention — the 2h-old tombstone should be deleted
	compactConn := db.Connect(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Deleted)

	// Verify: live resource still exists, tombstone gone
	verifyConn := db.Connect(t)
	var count int
	err = verifyConn.QueryRow(ctx,
		`SELECT count(*) FROM kubernetes_resources WHERE gvk = 'apps/v1/Deployment'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify compaction horizon was set
	var compactedSeq int64
	err = verifyConn.QueryRow(ctx,
		`SELECT compacted_seq FROM compaction_horizon WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
	).Scan(&compactedSeq)
	require.NoError(t, err)
	assert.Equal(t, int64(2), compactedSeq)
}

func TestCompactSkipsFreshTombstones(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	// Create a fresh tombstone (just now)
	now := time.Now()
	_, err := w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "fresh-tombstone",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), DeletionTimestamp: &now,
	})
	require.NoError(t, err)

	// Compact with 1h retention — fresh tombstone should survive
	compactConn := db.Connect(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Deleted)
}

func TestCompactSkipsDyingObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)

	// Create a dying object: deletion_timestamp set, but finalizers still present
	past := time.Now().Add(-2 * time.Hour)
	_, err := w.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dying",
		BucketID: 1, Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{"finalizers":["cleanup.example.com"]}`), DeletionTimestamp: &past,
	})
	require.NoError(t, err)

	// Backdate updated_at so age alone would qualify for compaction
	backdateConn := db.Connect(t)
	_, err = backdateConn.Exec(ctx, `UPDATE kubernetes_resources SET updated_at = deletion_timestamp WHERE name = 'dying'`)
	require.NoError(t, err)
	backdateConn.Close(ctx)

	// Compact with 1h retention — dying object must survive despite being 2h old
	compactConn := db.Connect(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Deleted)

	// Verify the row still exists
	verifyConn := db.Connect(t)
	var count int
	err = verifyConn.QueryRow(ctx,
		`SELECT count(*) FROM kubernetes_resources WHERE name = 'dying'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestCompactNoop(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}
	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	compactConn := db.Connect(t)
	result, err := compaction.Compact(ctx, compactConn, compaction.Config{Retention: 1 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.Deleted)
}

