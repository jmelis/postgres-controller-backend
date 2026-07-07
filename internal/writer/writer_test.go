package writer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupWriter(t *testing.T, db *testinfra.TestDB) *writer.Writer {
	t.Helper()
	writerConn := db.Connect(t)
	return writer.New(writerConn, nil)
}

func makeReq() model.WriteRequest {
	return model.WriteRequest{
		GVK:       "apps/v1/Deployment",
		Namespace: "default",
		Name:      "nginx",
		BucketID:  1,
		Spec:      json.RawMessage(`{"replicas":3}`),
		Status:    json.RawMessage(`{}`),
		Metadata:  json.RawMessage(`{}`),
	}
}

func TestCreateResource(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	result, err := w.Write(ctx, makeReq())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Seq)
	assert.Equal(t, int64(1), result.ObjectVersion)
	assert.NotEmpty(t, result.UID)
}

func TestUpdateResource(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	req := makeReq()
	result1, err := w.Write(ctx, req)
	require.NoError(t, err)

	req.Spec = json.RawMessage(`{"replicas":5}`)
	req.ExpectedVersion = result1.ObjectVersion
	result2, err := w.Write(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result2.Seq)
	assert.Equal(t, int64(2), result2.ObjectVersion)
	assert.Equal(t, result1.UID, result2.UID)
}

func TestConflictReturns409(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	req := makeReq()
	_, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Different content + stale version → 409 (suppression does not apply
	// because content differs).
	req.Spec = json.RawMessage(`{"replicas":99}`)
	req.ExpectedVersion = 999
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrConflict)
}

func TestWriteStatus_UpdatesOnlyStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	// Create resource via spec write
	w := setupWriter(t, db)
	createReq := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-test",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":3}`),
		Status: json.RawMessage(`{"ready":false}`), Metadata: json.RawMessage(`{}`),
	}
	createResult, err := w.Write(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), createResult.Seq)

	// Update status via WriteStatus
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-test",
		BucketID: 1, Status: json.RawMessage(`{"ready":true,"replicas":3}`),
		ExpectedVersion: createResult.ObjectVersion,
	}
	statusResult, err := w.WriteStatus(ctx, statusReq)
	require.NoError(t, err)
	assert.Equal(t, int64(2), statusResult.Seq)
	assert.Equal(t, int64(2), statusResult.ObjectVersion)
	assert.Equal(t, createResult.UID, statusResult.UID)

	// Verify: spec unchanged, status updated
	verifyConn := db.Connect(t)
	var spec, status json.RawMessage
	err = verifyConn.QueryRow(ctx,
		`SELECT spec, status FROM kubernetes_resources WHERE gvk = $1 AND namespace = $2 AND name = $3`,
		"apps/v1/Deployment", "default", "status-test").Scan(&spec, &status)
	require.NoError(t, err)
	assert.JSONEq(t, `{"replicas":3}`, string(spec), "spec must be unchanged")
	assert.JSONEq(t, `{"ready":true,"replicas":3}`, string(status), "status must be updated")
}

func TestWriteStatus_Conflict(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	// Create resource
	_, err := w.Write(ctx, makeReq())
	require.NoError(t, err)

	// WriteStatus with stale version
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "nginx",
		BucketID: 1, Status: json.RawMessage(`{"ready":true}`),
		ExpectedVersion: 999,
	}
	_, err = w.WriteStatus(ctx, statusReq)
	assert.ErrorIs(t, err, writer.ErrConflict)
}

func TestSequentialWritesAreContiguous(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		req := makeReq()
		req.Name = fmt.Sprintf("resource-%d", i)
		result, err := w.Write(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int64(i+1), result.Seq)
	}
}

func TestCreateRevivesTombstone(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	// Create → tombstone → re-create with same name
	req := makeReq()
	result1, err := w.Write(ctx, req)
	require.NoError(t, err)

	past := time.Now().Add(-10 * time.Minute)
	req.ExpectedVersion = result1.ObjectVersion
	req.DeletionTimestamp = &past
	req.Metadata = json.RawMessage(`{}`)
	_, err = w.Write(ctx, req)
	require.NoError(t, err)

	// Re-create: same (gvk, ns, name), ExpectedVersion=0
	req2 := makeReq()
	req2.Spec = json.RawMessage(`{"replicas":5}`)
	result2, err := w.Write(ctx, req2)
	require.NoError(t, err)

	assert.NotEqual(t, result1.UID, result2.UID, "revived resource must get a new UID")
	assert.Equal(t, int64(1), result2.ObjectVersion, "revived resource starts at version 1")

	// Verify deletion_timestamp is cleared
	verifyConn := db.Connect(t)
	var delTS *time.Time
	err = verifyConn.QueryRow(ctx,
		`SELECT deletion_timestamp FROM kubernetes_resources WHERE gvk = $1 AND namespace = $2 AND name = $3`,
		req2.GVK, req2.Namespace, req2.Name).Scan(&delTS)
	require.NoError(t, err)
	assert.Nil(t, delTS)
}

func TestCreateBlockedByDyingObject(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	// Create with finalizer
	req := makeReq()
	req.Metadata = json.RawMessage(`{"finalizers":["cleanup.example.com"]}`)
	result1, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Set deletion_timestamp but keep finalizers (dying, not tombstone)
	past := time.Now().Add(-10 * time.Minute)
	req.ExpectedVersion = result1.ObjectVersion
	req.DeletionTimestamp = &past
	_, err = w.Write(ctx, req)
	require.NoError(t, err)

	// Try to create again — must fail because object is dying, not fully deleted
	req2 := makeReq()
	_, err = w.Write(ctx, req2)
	assert.ErrorIs(t, err, writer.ErrAlreadyExists)
}

func TestCreateBlockedByLiveObject(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w := setupWriter(t, db)
	ctx := context.Background()

	req := makeReq()
	_, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Try to create again with different content — suppression check won't
	// fire, so the INSERT hits unique_violation, revival sees a live row,
	// and AlreadyExists is returned.
	req2 := makeReq()
	req2.Spec = json.RawMessage(`{"replicas":99}`)
	_, err = w.Write(ctx, req2)
	assert.ErrorIs(t, err, writer.ErrAlreadyExists)
}
