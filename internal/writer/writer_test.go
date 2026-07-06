package writer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupLeaseAndWriter(t *testing.T, db *testinfra.TestDB) (*writer.Writer, int64) {
	t.Helper()
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "test-replica")
	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)
	return w, epoch
}

func makeReq(epoch int64) model.WriteRequest {
	return model.WriteRequest{
		GVK:         "apps/v1/Deployment",
		Namespace:   "default",
		Name:        "nginx",
		BucketID:    1,
		Spec:        json.RawMessage(`{"replicas":3}`),
		Status:      json.RawMessage(`{}`),
		Metadata:    json.RawMessage(`{"labels":{}}`),
		LeaseHolder: "test-replica",
		LeaseEpoch:  epoch,
	}
}

func TestCreateResource(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	result, err := w.Write(ctx, makeReq(epoch))
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
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	req := makeReq(epoch)
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
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	req := makeReq(epoch)
	_, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Different content + stale version → 409 (suppression does not apply
	// because content differs).
	req.Spec = json.RawMessage(`{"replicas":99}`)
	req.ExpectedVersion = 999
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrConflict)
}

func TestFenceViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	writerConn := db.Connect(t)
	ctx := context.Background()

	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "test-replica")
	_, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	w := writer.New(writerConn, nil)
	req := makeReq(999) // wrong epoch
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrFenceViolation)
}

func TestWriteStatus_UpdatesOnlyStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	// Setup: spec lease (holder-a) + status lease (holder-b)
	specLeaseConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specLeaseConn, "holder-a")
	specEpoch, err := specMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	statusLeaseConn := db.Connect(t)
	statusMgr := lease.NewStatusManager(statusLeaseConn, "holder-b")
	statusEpoch, err := statusMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	// Create resource via spec write
	specConn := db.Connect(t)
	specWriter := writer.New(specConn, nil)
	createReq := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-test",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":3}`),
		Status: json.RawMessage(`{"ready":false}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "holder-a", LeaseEpoch: specEpoch,
	}
	createResult, err := specWriter.Write(ctx, createReq)
	require.NoError(t, err)
	assert.Equal(t, int64(1), createResult.Seq)

	// Update status via WriteStatus
	statusConn := db.Connect(t)
	statusWriter := writer.New(statusConn, nil)
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-test",
		BucketID: 1, Status: json.RawMessage(`{"ready":true,"replicas":3}`),
		LeaseHolder: "holder-b", LeaseEpoch: statusEpoch,
		ExpectedVersion: createResult.ObjectVersion,
	}
	statusResult, err := statusWriter.WriteStatus(ctx, statusReq)
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

func TestWriteStatus_FenceViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	// Create a spec lease and a resource
	specLeaseConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specLeaseConn, "test-replica")
	specEpoch, err := specMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	specConn := db.Connect(t)
	specWriter := writer.New(specConn, nil)
	createReq := makeReq(specEpoch)
	createResult, err := specWriter.Write(ctx, createReq)
	require.NoError(t, err)

	// No status lease acquired — WriteStatus must fail with fence violation
	statusConn := db.Connect(t)
	statusWriter := writer.New(statusConn, nil)
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "nginx",
		BucketID: 1, Status: json.RawMessage(`{"ready":true}`),
		LeaseHolder: "holder-b", LeaseEpoch: 999,
		ExpectedVersion: createResult.ObjectVersion,
	}
	_, err = statusWriter.WriteStatus(ctx, statusReq)
	assert.ErrorIs(t, err, writer.ErrFenceViolation)
}

func TestWriteStatus_Conflict(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	specLeaseConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specLeaseConn, "test-replica")
	specEpoch, err := specMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	statusLeaseConn := db.Connect(t)
	statusMgr := lease.NewStatusManager(statusLeaseConn, "holder-b")
	statusEpoch, err := statusMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	// Create resource
	specConn := db.Connect(t)
	specWriter := writer.New(specConn, nil)
	_, err = specWriter.Write(ctx, makeReq(specEpoch))
	require.NoError(t, err)

	// WriteStatus with stale version
	statusConn := db.Connect(t)
	statusWriter := writer.New(statusConn, nil)
	statusReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "nginx",
		BucketID: 1, Status: json.RawMessage(`{"ready":true}`),
		LeaseHolder: "holder-b", LeaseEpoch: statusEpoch,
		ExpectedVersion: 999,
	}
	_, err = statusWriter.WriteStatus(ctx, statusReq)
	assert.ErrorIs(t, err, writer.ErrConflict)
}

func TestWriteStatus_IndependentFromSpecLease(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	// holder-a holds spec lease, holder-b holds status lease
	specLeaseConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specLeaseConn, "holder-a")
	specEpoch, err := specMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	statusLeaseConn := db.Connect(t)
	statusMgr := lease.NewStatusManager(statusLeaseConn, "holder-b")
	statusEpoch, err := statusMgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	// holder-a creates resource via Write
	specConn := db.Connect(t)
	specWriter := writer.New(specConn, nil)
	createResult, err := specWriter.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "independent",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "holder-a", LeaseEpoch: specEpoch,
	})
	require.NoError(t, err)

	// holder-b updates status via WriteStatus (different holder, different lease table)
	statusConn := db.Connect(t)
	statusWriter := writer.New(statusConn, nil)
	statusResult, err := statusWriter.WriteStatus(ctx, model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "independent",
		BucketID: 1, Status: json.RawMessage(`{"ready":true}`),
		LeaseHolder: "holder-b", LeaseEpoch: statusEpoch,
		ExpectedVersion: createResult.ObjectVersion,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), statusResult.Seq)

	// holder-a can still write spec (using fresh object_version)
	specUpdateResult, err := specWriter.Write(ctx, model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "independent",
		BucketID: 1, Spec: json.RawMessage(`{"replicas":2}`),
		Status: json.RawMessage(`{"ready":true}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "holder-a", LeaseEpoch: specEpoch,
		ExpectedVersion: statusResult.ObjectVersion,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), specUpdateResult.Seq)
}

func TestSequentialWritesAreGapless(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		req := makeReq(epoch)
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
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	// Create → tombstone → re-create with same name
	req := makeReq(epoch)
	result1, err := w.Write(ctx, req)
	require.NoError(t, err)

	past := time.Now().Add(-10 * time.Minute)
	req.ExpectedVersion = result1.ObjectVersion
	req.DeletionTimestamp = &past
	req.Metadata = json.RawMessage(`{}`)
	_, err = w.Write(ctx, req)
	require.NoError(t, err)

	// Re-create: same (gvk, ns, name), ExpectedVersion=0
	req2 := makeReq(epoch)
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
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	// Create with finalizer
	req := makeReq(epoch)
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
	req2 := makeReq(epoch)
	_, err = w.Write(ctx, req2)
	assert.ErrorIs(t, err, writer.ErrAlreadyExists)
}

func TestCreateBlockedByLiveObject(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	w, epoch := setupLeaseAndWriter(t, db)
	ctx := context.Background()

	req := makeReq(epoch)
	_, err := w.Write(ctx, req)
	require.NoError(t, err)

	// Try to create again with different content — suppression check won't
	// fire, so the INSERT hits unique_violation, revival sees a live row,
	// and AlreadyExists is returned.
	req2 := makeReq(epoch)
	req2.Spec = json.RawMessage(`{"replicas":99}`)
	_, err = w.Write(ctx, req2)
	assert.ErrorIs(t, err, writer.ErrAlreadyExists)
}

func TestExpiredUnStolenLeaseRejectsWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	ctx := context.Background()

	// Acquire lease with a very short TTL (1 second)
	leaseConn := db.Connect(t)
	mgr := lease.NewSpecManager(leaseConn, "test-replica")
	epoch, err := mgr.Acquire(ctx, 1, 1*time.Second)
	require.NoError(t, err)

	// Wait for lease to expire naturally (no one steals it)
	time.Sleep(1500 * time.Millisecond)

	// Try to write with the same holder and epoch — should fail because expires_at < now()
	writerConn := db.Connect(t)
	w := writer.New(writerConn, nil)
	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "expired-test",
		BucketID: 1,
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
		LeaseHolder: "test-replica", LeaseEpoch: epoch,
	}
	_, err = w.Write(ctx, req)
	assert.ErrorIs(t, err, writer.ErrFenceViolation,
		"write must fail with fence violation when lease is expired but not stolen")
}
