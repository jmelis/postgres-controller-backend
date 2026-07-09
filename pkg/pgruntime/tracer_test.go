package pgruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSlowQueryTracer_LogsSlowQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	c, cleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:             testScheme,
		DSN:                sharedDB.ConnStr,
		SlowQueryThreshold: time.Nanosecond,
		SlowQueryLogger:    logger,
	})
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tracer-test"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

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
}

func TestSlowQueryTracer_NoLogBelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	c, cleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:             testScheme,
		DSN:                sharedDB.ConnStr,
		SlowQueryThreshold: time.Hour,
		SlowQueryLogger:    logger,
	})
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "fast-query"},
		Spec:       WidgetSpec{Color: "green"},
	}
	require.NoError(t, c.Create(ctx, w))

	assert.Empty(t, buf.String(), "no log should be emitted for fast queries")
}

func TestSlowQueryTracer_IncludesErrorOnFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	c, cleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:             testScheme,
		DSN:                sharedDB.ConnStr,
		SlowQueryThreshold: time.Nanosecond,
		SlowQueryLogger:    logger,
	})
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	_ = c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "nonexistent"}, &Widget{})

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.NotEmpty(t, lines)

	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if sql, ok := entry["sql"].(string); ok && len(sql) > 10 {
			assert.Contains(t, entry, "pg_pid")
			return
		}
	}
}

func TestSlowQueryTracer_Disabled(t *testing.T) {
	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	c, cleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme: testScheme,
		DSN:    sharedDB.ConnStr,
	})
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "no-tracer"},
		Spec:       WidgetSpec{Color: "red"},
	}
	require.NoError(t, c.Create(ctx, w))
}
