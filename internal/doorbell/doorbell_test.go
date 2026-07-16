package doorbell_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/doorbell"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
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

func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	return sharedDB.Connect(t)
}

func listenOn(t *testing.T, gvk string) *pgx.Conn {
	t.Helper()
	conn := connect(t)
	channel := model.DoorbellChannel(gvk)
	_, err := conn.Exec(context.Background(), "LISTEN "+pgx.Identifier{channel}.Sanitize())
	require.NoError(t, err)
	return conn
}

func drainNotifications(t *testing.T, conn *pgx.Conn, timeout time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	count := 0
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			break
		}
		if n != nil {
			count++
		}
	}
	return count
}

func TestDebouncer_SendsNotification(t *testing.T) {
	const gvk = "apps/v1/Deployment"
	listener := listenOn(t, gvk)

	dbConn := connect(t)
	d := doorbell.NewDebouncer(dbConn, 50*time.Millisecond)

	d.Ring(gvk)
	d.Close()

	count := drainNotifications(t, listener, 500*time.Millisecond)
	assert.GreaterOrEqual(t, count, 1, "at least one notification must arrive")
}

func TestDebouncer_CoalescesBurst(t *testing.T) {
	const gvk = "apps/v1/Deployment"
	listener := listenOn(t, gvk)

	dbConn := connect(t)
	d := doorbell.NewDebouncer(dbConn, 100*time.Millisecond)

	for range 100 {
		d.Ring(gvk)
	}

	// Wait for 2 windows to ensure flush fires
	time.Sleep(250 * time.Millisecond)
	d.Close()

	count := drainNotifications(t, listener, 500*time.Millisecond)
	// 100 rings in ~0ms should produce at most 2-3 notifications (one per tick window)
	assert.GreaterOrEqual(t, count, 1, "at least one notification must arrive")
	assert.LessOrEqual(t, count, 5, "burst of 100 rings should be coalesced, not produce 100 notifications")
	t.Logf("100 rings produced %d notifications (window=100ms)", count)
}

func TestDebouncer_MultiGVK(t *testing.T) {
	gvks := []string{
		"apps/v1/Deployment",
		"apps/v1/StatefulSet",
		"batch/v1/Job",
	}

	listeners := make([]*pgx.Conn, len(gvks))
	for i, gvk := range gvks {
		listeners[i] = listenOn(t, gvk)
	}

	dbConn := connect(t)
	d := doorbell.NewDebouncer(dbConn, 50*time.Millisecond)

	for _, gvk := range gvks {
		d.Ring(gvk)
	}
	d.Close()

	for i, gvk := range gvks {
		count := drainNotifications(t, listeners[i], 500*time.Millisecond)
		assert.GreaterOrEqual(t, count, 1, "GVK %s must receive at least one notification", gvk)
	}
}

func TestDebouncer_FlushOnClose(t *testing.T) {
	const gvk = "apps/v1/Deployment"
	listener := listenOn(t, gvk)

	dbConn := connect(t)
	d := doorbell.NewDebouncer(dbConn, 5*time.Second) // very long window

	d.Ring(gvk)
	// Close immediately — must flush pending without waiting for the 5s tick
	d.Close()

	count := drainNotifications(t, listener, 500*time.Millisecond)
	assert.GreaterOrEqual(t, count, 1, "Close() must flush pending notifications")
}

func TestDebouncer_NoNotificationWithoutRing(t *testing.T) {
	const gvk = "apps/v1/Deployment"
	listener := listenOn(t, gvk)

	dbConn := connect(t)
	d := doorbell.NewDebouncer(dbConn, 50*time.Millisecond)

	// Wait for 2 full windows without ringing
	time.Sleep(150 * time.Millisecond)
	d.Close()

	count := drainNotifications(t, listener, 200*time.Millisecond)
	assert.Equal(t, 0, count, "no notification should be sent without Ring()")
}
