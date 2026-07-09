package pgruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

type traceContextKey struct{}

type traceData struct {
	start time.Time
	sql   string
}

type slowQueryTracer struct {
	threshold time.Duration
	logger    *slog.Logger
}

func (t *slowQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceContextKey{}, traceData{start: time.Now(), sql: data.SQL})
}

func (t *slowQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	td, ok := ctx.Value(traceContextKey{}).(traceData)
	if !ok {
		return
	}

	elapsed := time.Since(td.start)
	if elapsed < t.threshold {
		return
	}

	attrs := []slog.Attr{
		slog.Uint64("pg_pid", uint64(conn.PgConn().PID())),
		slog.Duration("duration", elapsed),
		slog.String("sql", td.sql),
	}
	if data.Err != nil {
		attrs = append(attrs, slog.Any("err", data.Err))
	}

	t.logger.LogAttrs(ctx, slog.LevelWarn, "slow query", attrs...)
}
