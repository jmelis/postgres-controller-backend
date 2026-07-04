package writer

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
)

type Writer struct {
	conn  *pgx.Conn
	hooks TxHooks
}

func New(conn *pgx.Conn, hooks TxHooks) *Writer {
	return &Writer{conn: conn, hooks: hooks}
}

func (w *Writer) Write(ctx context.Context, req model.WriteRequest) (model.WriteResult, error) {
	tx, err := w.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// (a) FENCE — FOR SHARE held to COMMIT (I4)
	var fenceOK int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM bucket_leases
		WHERE bucket_id = $1 AND holder = $2
		  AND epoch = $3 AND expires_at > now()
		FOR SHARE`,
		req.BucketID, req.LeaseHolder, req.LeaseEpoch).Scan(&fenceOK)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.WriteResult{}, ErrFenceViolation
		}
		return model.WriteResult{}, fmt.Errorf("fence: %w", err)
	}

	if w.hooks != nil {
		if err := w.hooks.AfterFence(ctx, tx); err != nil {
			return model.WriteResult{}, err
		}
	}

	// (b) SEQUENCE — exclusive row lock serializes issuance (I1/I2)
	var seq int64
	err = tx.QueryRow(ctx, `
		INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
		VALUES ($1, $2, 1)
		ON CONFLICT (bucket_id, gvk)
		DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
		RETURNING current_seq`,
		req.BucketID, req.GVK).Scan(&seq)
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("counter: %w", err)
	}

	if w.hooks != nil {
		if err := w.hooks.AfterCounter(ctx, tx, seq); err != nil {
			return model.WriteResult{}, err
		}
	}

	// (c) UPSERT with optimistic concurrency (I8)
	var resultUID uuid.UUID
	var resultVersion int64

	if req.ExpectedVersion == 0 {
		// Create path
		err = tx.QueryRow(ctx, `
			INSERT INTO kubernetes_resources
				(gvk, namespace, name, bucket_id, gvk_bucket_seq,
				 object_version, spec, status, metadata, deletion_timestamp)
			VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9)
			RETURNING uid, object_version`,
			req.GVK, req.Namespace, req.Name, req.BucketID, seq,
			req.Spec, req.Status, req.Metadata, req.DeletionTimestamp,
		).Scan(&resultUID, &resultVersion)
		if err != nil {
			return model.WriteResult{}, fmt.Errorf("create resource: %w", err)
		}
	} else {
		// Update path
		err = tx.QueryRow(ctx, `
			UPDATE kubernetes_resources
			SET gvk_bucket_seq = $1,
			    object_version = object_version + 1,
			    spec = $2,
			    status = $3,
			    metadata = $4,
			    deletion_timestamp = $5,
			    updated_at = now()
			WHERE gvk = $6 AND namespace = $7 AND name = $8
			  AND object_version = $9
			RETURNING uid, object_version`,
			seq, req.Spec, req.Status, req.Metadata, req.DeletionTimestamp,
			req.GVK, req.Namespace, req.Name, req.ExpectedVersion,
		).Scan(&resultUID, &resultVersion)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return model.WriteResult{}, ErrConflict
			}
			return model.WriteResult{}, fmt.Errorf("update resource: %w", err)
		}
	}

	// (d) DOORBELL — latency optimization only, correctness from poll (I5)
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`,
		fmt.Sprintf("resource_changes_b%d", req.BucketID),
		fmt.Sprintf(`{"bucket_id":%d,"gvk":"%s","seq":%d}`, req.BucketID, req.GVK, seq))
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("doorbell: %w", err)
	}

	if w.hooks != nil {
		if err := w.hooks.BeforeCommit(ctx, tx); err != nil {
			return model.WriteResult{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return model.WriteResult{}, &AmbiguousCommitError{Cause: err, Req: req, Seq: seq}
	}

	return model.WriteResult{Seq: seq, ObjectVersion: resultVersion, UID: resultUID}, nil
}

// ReadBack resolves an ambiguous commit by checking if the write actually landed.
// Returns the resource if found at the expected seq, nil if not.
func (w *Writer) ReadBack(ctx context.Context, req model.WriteRequest, seq int64) (*model.Resource, error) {
	r := &model.Resource{}
	err := w.conn.QueryRow(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata, deletion_timestamp,
		       created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND namespace = $2 AND name = $3
		  AND gvk_bucket_seq = $4`,
		req.GVK, req.Namespace, req.Name, seq,
	).Scan(&r.GVK, &r.Namespace, &r.Name, &r.UID, &r.BucketID,
		&r.GVKBucketSeq, &r.ObjectVersion, &r.Spec, &r.Status,
		&r.Metadata, &r.DeletionTimestamp, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("read-back: %w", err)
	}
	return r, nil
}
