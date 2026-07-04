package lease_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
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

	mgr := lease.NewManager(conn, "replica-1")

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

	mgr := lease.NewManager(conn, "replica-1")

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

	mgr1 := lease.NewManager(conn1, "replica-1")
	mgr2 := lease.NewManager(conn2, "replica-2")

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

	mgr1 := lease.NewManager(conn1, "replica-1")
	mgr2 := lease.NewManager(conn2, "replica-2")

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

	mgr := lease.NewManager(conn, "replica-1")

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

	mgr1 := lease.NewManager(conn1, "replica-1")
	mgr2 := lease.NewManager(conn2, "coordinator")

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

	mgr := lease.NewManager(conn, "replica-1")
	err := mgr.Renew(ctx, 99, 30*time.Second)
	assert.ErrorIs(t, err, lease.ErrNotHolder)
}
