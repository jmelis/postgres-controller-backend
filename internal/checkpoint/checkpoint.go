package checkpoint

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

type Checkpointer struct {
	conn *pgx.Conn
}

func New(conn *pgx.Conn) *Checkpointer {
	return &Checkpointer{conn: conn}
}

type SaveRequest struct {
	StreamARN   string
	ShardID     string
	LastSeqNum  string
	HolderID    string
	BucketID    int
	SpecEpoch   int64
	StatusEpoch int64
}

// Save upserts a checkpoint, fenced by FOR SHARE on both lease domain rows.
// Returns ErrFenceViolation if the caller no longer holds both domains.
func (c *Checkpointer) Save(ctx context.Context, req SaveRequest) error {
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var count int
	err = tx.QueryRow(ctx, `
		SELECT count(*) FROM bucket_leases
		WHERE bucket_id = $1
		  AND ((domain = 'spec'   AND epoch = $2)
		    OR (domain = 'status' AND epoch = $3))
		  AND holder = $4
		  AND expires_at > now()
		FOR SHARE`,
		req.BucketID, req.SpecEpoch, req.StatusEpoch, req.HolderID,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("fence: %w", err)
	}
	if count < 2 {
		return writer.ErrFenceViolation
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO stream_checkpoints (stream_arn, shard_id, last_seq_num, holder_id, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (stream_arn, shard_id)
		DO UPDATE SET last_seq_num = $3, holder_id = $4, updated_at = now()`,
		req.StreamARN, req.ShardID, req.LastSeqNum, req.HolderID,
	)
	if err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Load returns all checkpoints for a stream as a shard_id → last_seq_num map.
func (c *Checkpointer) Load(ctx context.Context, streamARN string) (map[string]string, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT shard_id, last_seq_num FROM stream_checkpoints
		WHERE stream_arn = $1`,
		streamARN,
	)
	if err != nil {
		return nil, fmt.Errorf("query checkpoints: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var shardID, seqNum string
		if err := rows.Scan(&shardID, &seqNum); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		result[shardID] = seqNum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}
	return result, nil
}

// Delete removes all checkpoints for a stream (used during stream ARN rotation).
func (c *Checkpointer) Delete(ctx context.Context, streamARN string) error {
	_, err := c.conn.Exec(ctx, `
		DELETE FROM stream_checkpoints WHERE stream_arn = $1`,
		streamARN,
	)
	if err != nil {
		return fmt.Errorf("delete checkpoints: %w", err)
	}
	return nil
}

// Ensure the import of writer is used (for ErrFenceViolation).
var _ = errors.Is
