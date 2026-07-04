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

// Compact deletes tombstones older than retention and advances the compaction
// horizon in the same transaction (I7: horizon must never lag physical delete).
func Compact(ctx context.Context, conn *pgx.Conn, cfg Config) (*Result, error) {
	if cfg.Retention == 0 {
		cfg.Retention = 24 * time.Hour
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("compact begin: %w", err)
	}
	defer tx.Rollback(ctx)

	cutoff := fmt.Sprintf("%d seconds", int(cfg.Retention.Seconds()))

	// Find the max seq per (bucket, gvk) among tombstones being deleted.
	// This becomes the new compaction horizon.
	rows, err := tx.Query(ctx, `
		SELECT bucket_id, gvk, max(gvk_bucket_seq) AS max_seq
		FROM kubernetes_resources
		WHERE deletion_timestamp IS NOT NULL
		  AND deletion_timestamp < now() - $1::interval
		GROUP BY bucket_id, gvk`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("compact scan: %w", err)
	}

	type horizonEntry struct {
		BucketID int
		GVK      string
		MaxSeq   int64
	}
	var entries []horizonEntry
	for rows.Next() {
		var e horizonEntry
		if err := rows.Scan(&e.BucketID, &e.GVK, &e.MaxSeq); err != nil {
			rows.Close()
			return nil, fmt.Errorf("compact scan row: %w", err)
		}
		entries = append(entries, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compact scan rows: %w", err)
	}

	if len(entries) == 0 {
		tx.Rollback(ctx)
		return &Result{Deleted: 0}, nil
	}

	// Advance compaction horizon for each (bucket, gvk)
	for _, e := range entries {
		_, err := tx.Exec(ctx, `
			INSERT INTO compaction_horizon (bucket_id, gvk, compacted_seq)
			VALUES ($1, $2, $3)
			ON CONFLICT (bucket_id, gvk)
			DO UPDATE SET compacted_seq = GREATEST(compaction_horizon.compacted_seq, EXCLUDED.compacted_seq)`,
			e.BucketID, e.GVK, e.MaxSeq)
		if err != nil {
			return nil, fmt.Errorf("compact horizon update bucket=%d gvk=%s: %w", e.BucketID, e.GVK, err)
		}
	}

	// Delete the tombstones
	tag, err := tx.Exec(ctx, `
		DELETE FROM kubernetes_resources
		WHERE deletion_timestamp IS NOT NULL
		  AND deletion_timestamp < now() - $1::interval`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("compact delete: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("compact commit: %w", err)
	}

	return &Result{Deleted: tag.RowsAffected()}, nil
}
