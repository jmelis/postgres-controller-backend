package compaction

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Config struct {
	Retention time.Duration // how long tombstones survive (default 24h)
}

type Result struct {
	Deleted int64
}

// Compact deletes fully-deleted tombstones (deletion_timestamp set, no active
// finalizers) older than retention and advances the compaction horizon
// atomically in a single CTE statement (I6: horizon never lags delete).
// Dying objects with active finalizers are preserved regardless of age.
func Compact(ctx context.Context, conn *pgx.Conn, cfg Config) (*Result, error) {
	if cfg.Retention == 0 {
		cfg.Retention = 24 * time.Hour
	}

	cutoff := fmt.Sprintf("%d seconds", int(cfg.Retention.Seconds()))

	var deleted int64
	err := conn.QueryRow(ctx, `
		WITH del AS (
			DELETE FROM kubernetes_resources
			WHERE deletion_timestamp IS NOT NULL
			  AND GREATEST(deletion_timestamp, updated_at) < now() - $1::interval
			  AND (metadata->'finalizers' IS NULL OR metadata->'finalizers' = '[]'::jsonb) -- tombstone filter: also in list.go, 001_initial.sql, writer.go
			RETURNING bucket_id, gvk, gvk_bucket_seq
		),
		horizon AS (
			INSERT INTO compaction_horizon (bucket_id, gvk, compacted_seq)
			SELECT bucket_id, gvk, max(gvk_bucket_seq) FROM del GROUP BY 1, 2
			ON CONFLICT (bucket_id, gvk)
			DO UPDATE SET compacted_seq = GREATEST(compaction_horizon.compacted_seq, EXCLUDED.compacted_seq)
		)
		SELECT count(*) FROM del`, cutoff).Scan(&deleted)
	if err != nil {
		return nil, fmt.Errorf("compact: %w", err)
	}

	return &Result{Deleted: deleted}, nil
}
