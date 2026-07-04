package lease_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireAndRenew(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewSpecManager(conn, "replica-1")

	epoch, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(1), epoch)

	info, err := mgr.Get(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "replica-1", info.Holder)
	assert.Equal(t, int64(1), info.Epoch)

	err = mgr.Renew(ctx, 1, 60*time.Second)
	require.NoError(t, err)

	info, err = mgr.Get(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), info.Epoch)
}

func TestAcquireReacquireSameHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewSpecManager(conn, "replica-1")

	epoch1, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	epoch2, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, epoch1+1, epoch2)
}

func TestAcquireBlockedByActiveHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn1 := db.Connect(t)
	conn2 := db.Connect(t)
	ctx := context.Background()

	mgr1 := lease.NewSpecManager(conn1, "replica-1")
	mgr2 := lease.NewSpecManager(conn2, "replica-2")

	_, err := mgr1.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	_, err = mgr2.Acquire(ctx, 1, 30*time.Second)
	assert.Error(t, err)
}

func TestAcquireExpiredLease(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn1 := db.Connect(t)
	conn2 := db.Connect(t)
	ctx := context.Background()

	mgr1 := lease.NewSpecManager(conn1, "replica-1")
	mgr2 := lease.NewSpecManager(conn2, "replica-2")

	_, err := mgr1.Acquire(ctx, 1, 1*time.Second)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	epoch, err := mgr2.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(2), epoch)

	info, err := mgr2.Get(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "replica-2", info.Holder)
}

func TestRelease(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewSpecManager(conn, "replica-1")

	_, err := mgr.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	err = mgr.Release(ctx, 1)
	require.NoError(t, err)

	info, err := mgr.Get(ctx, 1)
	require.NoError(t, err)
	assert.Nil(t, info)
}

func TestGrantBumpsEpoch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn1 := db.Connect(t)
	conn2 := db.Connect(t)
	ctx := context.Background()

	mgr1 := lease.NewSpecManager(conn1, "replica-1")
	mgr2 := lease.NewSpecManager(conn2, "coordinator")

	epoch1, err := mgr1.Acquire(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	epoch2, err := mgr2.Grant(ctx, 1, "replica-2", 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, epoch1+1, epoch2)

	info, err := mgr2.Get(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "replica-2", info.Holder)
	assert.Equal(t, epoch2, info.Epoch)
}

func TestRenewNotHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewSpecManager(conn, "replica-1")
	err := mgr.Renew(ctx, 99, 30*time.Second)
	assert.ErrorIs(t, err, lease.ErrNotHolder)
}

func TestAcquireBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewBothManager(conn, "replica-1")
	epochs, err := mgr.AcquireBoth(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(1), epochs.Spec)
	assert.Equal(t, int64(1), epochs.Status)

	// Verify both leases exist via individual managers
	specConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specConn, "replica-1")
	specInfo, err := specMgr.Get(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, specInfo)
	assert.Equal(t, "replica-1", specInfo.Holder)

	statusConn := db.Connect(t)
	statusMgr := lease.NewStatusManager(statusConn, "replica-1")
	statusInfo, err := statusMgr.Get(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, statusInfo)
	assert.Equal(t, "replica-1", statusInfo.Holder)

	// Re-acquire bumps both epochs
	epochs2, err := mgr.AcquireBoth(ctx, 1, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, int64(2), epochs2.Spec)
	assert.Equal(t, int64(2), epochs2.Status)
}

func TestRenewBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewBothManager(conn, "replica-1")
	_, err := mgr.AcquireBoth(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	err = mgr.RenewBoth(ctx, 1, 60*time.Second)
	require.NoError(t, err)

	// Renew for non-held bucket fails
	err = mgr.RenewBoth(ctx, 99, 60*time.Second)
	assert.Error(t, err)
}

func TestReleaseBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	mgr := lease.NewBothManager(conn, "replica-1")
	_, err := mgr.AcquireBoth(ctx, 1, 30*time.Second)
	require.NoError(t, err)

	err = mgr.ReleaseBoth(ctx, 1)
	require.NoError(t, err)

	// Verify both leases are gone
	specConn := db.Connect(t)
	specMgr := lease.NewSpecManager(specConn, "replica-1")
	specInfo, err := specMgr.Get(ctx, 1)
	require.NoError(t, err)
	assert.Nil(t, specInfo)

	statusConn := db.Connect(t)
	statusMgr := lease.NewStatusManager(statusConn, "replica-1")
	statusInfo, err := statusMgr.Get(ctx, 1)
	require.NoError(t, err)
	assert.Nil(t, statusInfo)
}
