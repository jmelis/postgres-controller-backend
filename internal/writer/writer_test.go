package writer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
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
	statusReq := model.WriteRequest{
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
	statusReq := model.WriteRequest{
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
	statusReq := model.WriteRequest{
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
	statusResult, err := statusWriter.WriteStatus(ctx, model.WriteRequest{
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
