package pgruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTracedPool(t *testing.T, threshold time.Duration, logger *slog.Logger) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(sharedDB.ConnStr)
	require.NoError(t, err)
	config.ConnConfig.Tracer = pgruntime.NewSlowQueryTracer(threshold, logger)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestSlowQueryTracer_LogsSlowQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	pool := newTracedPool(t, 500*time.Millisecond, logger)

	ctx := context.Background()
	_, err := pool.Exec(ctx, "SELECT pg_sleep(1)")
	require.NoError(t, err)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.NotEmpty(t, lines, "expected at least one log line")

	var entry map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &entry))

	assert.Equal(t, "slow query", entry["msg"])
	assert.Contains(t, entry, "pg_pid")
	assert.Contains(t, entry, "duration")
	assert.Contains(t, entry, "sql")

	pid, ok := entry["pg_pid"].(float64)
	require.True(t, ok, "pg_pid should be a number")
	assert.Greater(t, pid, float64(0), "pg_pid should be positive")

	sql, ok := entry["sql"].(string)
	require.True(t, ok)
	assert.Contains(t, sql, "pg_sleep")
}

func TestSlowQueryTracer_NoLogBelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	pool := newTracedPool(t, 5*time.Second, logger)

	ctx := context.Background()
	_, err := pool.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	assert.Empty(t, buf.String(), "no log should be emitted for fast queries")
}

func TestSlowQueryTracer_PIDMatchesBackend(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	config, err := pgxpool.ParseConfig(sharedDB.ConnStr)
	require.NoError(t, err)
	config.MaxConns = 1
	config.ConnConfig.Tracer = pgruntime.NewSlowQueryTracer(500*time.Millisecond, logger)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	ctx := context.Background()

	var backendPID uint32
	err = pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID)
	require.NoError(t, err)

	buf.Reset()

	_, err = pool.Exec(ctx, "SELECT pg_sleep(1)")
	require.NoError(t, err)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.NotEmpty(t, lines)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &entry))

	loggedPID, ok := entry["pg_pid"].(float64)
	require.True(t, ok)
	assert.Equal(t, uint32(loggedPID), backendPID, "logged PID should match pg_backend_pid()")
}

func TestSlowQueryTracer_IncludesErrorOnFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	config, err := pgxpool.ParseConfig(sharedDB.ConnStr)
	require.NoError(t, err)
	config.ConnConfig.Tracer = pgruntime.NewSlowQueryTracer(500*time.Millisecond, logger)
	config.ConnConfig.RuntimeParams = map[string]string{
		"statement_timeout": "1000",
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	ctx := context.Background()
	_, err = pool.Exec(ctx, "SELECT pg_sleep(2)")
	require.Error(t, err)

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.NotEmpty(t, lines)

	var entry map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &entry))

	assert.Equal(t, "slow query", entry["msg"])
	assert.Contains(t, entry, "pg_pid")
	assert.Contains(t, entry, "err")
}

func TestSlowQueryTracer_Disabled(t *testing.T) {
	config, err := pgxpool.ParseConfig(sharedDB.ConnStr)
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	ctx := context.Background()
	_, err = pool.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
}
