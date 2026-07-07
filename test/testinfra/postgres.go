package testinfra

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/schema"
)

type TestDB struct {
	ConnStr   string
	container string
}

func StartPostgres(t testing.TB) *TestDB {
	t.Helper()

	port := freePort(t)
	container := fmt.Sprintf("pgctl-test-%d", port)

	args := []string{
		"run", "-d", "--rm",
		"--name", container,
		"-p", fmt.Sprintf("%d:5432", port),
		"-e", "POSTGRES_DB=pgctl_test",
		"-e", "POSTGRES_USER=test",
		"-e", "POSTGRES_PASSWORD=test",
		"docker.io/library/postgres:16-alpine",
	}

	out, err := exec.Command("podman", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("podman run: %v\n%s", err, out)
	}

	connStr := fmt.Sprintf("postgres://test:test@localhost:%d/pgctl_test?sslmode=disable", port)

	t.Cleanup(func() {
		exec.Command("podman", "stop", container).Run()
	})

	waitForPostgres(t, connStr)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect for migration: %v", err)
	}
	if err := schema.Migrate(ctx, conn); err != nil {
		conn.Close(ctx)
		t.Fatalf("migrate: %v", err)
	}
	conn.Close(ctx)

	return &TestDB{ConnStr: connStr, container: container}
}

func (db *TestDB) Connect(t testing.TB) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.ConnStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func (db *TestDB) TruncateAll(t testing.TB, conn *pgx.Conn) {
	t.Helper()
	tables := []string{
		"kubernetes_resources",
		"gvk_bucket_counters",
		"compaction_horizon",
	}
	ctx := context.Background()
	for _, tbl := range tables {
		if _, err := conn.Exec(ctx, "TRUNCATE "+tbl+" CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}
	if _, err := conn.Exec(ctx, "UPDATE cluster_epoch SET timeline_id = 1"); err != nil {
		t.Fatalf("reset cluster_epoch: %v", err)
	}
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForPostgres(t testing.TB, connStr string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		err = conn.Ping(ctx)
		conn.Close(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
	// show container logs on failure
	out, _ := exec.Command("podman", "logs", extractContainer(connStr)).CombinedOutput()
	t.Fatalf("postgres not ready after 30s: %v\nlogs:\n%s", lastErr, out)
}

// StartPostgresForTestMain is for use in TestMain where testing.TB is not available.
// The caller must call Stop() when done.
func StartPostgresForTestMain() *TestDB {
	port := freePortNoT()
	container := fmt.Sprintf("pgctl-test-%d", port)

	args := []string{
		"run", "-d", "--rm",
		"--name", container,
		"-p", fmt.Sprintf("%d:5432", port),
		"-e", "POSTGRES_DB=pgctl_test",
		"-e", "POSTGRES_USER=test",
		"-e", "POSTGRES_PASSWORD=test",
		"docker.io/library/postgres:16-alpine",
	}

	out, err := exec.Command("podman", args...).CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("podman run: %v\n%s", err, out))
	}

	connStr := fmt.Sprintf("postgres://test:test@localhost:%d/pgctl_test?sslmode=disable", port)

	waitForPostgresNoT(connStr)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		panic(fmt.Sprintf("connect for migration: %v", err))
	}
	if err := schema.Migrate(ctx, conn); err != nil {
		conn.Close(ctx)
		panic(fmt.Sprintf("migrate: %v", err))
	}
	conn.Close(ctx)

	return &TestDB{ConnStr: connStr, container: container}
}

func (db *TestDB) Stop() {
	exec.Command("podman", "stop", db.container).Run()
}

func freePortNoT() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("free port: %v", err))
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForPostgresNoT(connStr string) {
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		err = conn.Ping(ctx)
		conn.Close(ctx)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
	panic(fmt.Sprintf("postgres not ready after 30s: %v", lastErr))
}

func extractContainer(connStr string) string {
	parts := strings.Split(connStr, ":")
	if len(parts) >= 4 {
		portPart := strings.Split(parts[3], "/")[0]
		return "pgctl-test-" + portPart
	}
	return ""
}
