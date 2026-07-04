package writer

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
)

type Writer struct {
	conn  *pgx.Conn
	hooks TxHooks
}

func New(conn *pgx.Conn, hooks TxHooks) *Writer {
	if hooks == nil {
		hooks = noopHooks{}
	}
	return &Writer{conn: conn, hooks: hooks}
}

func (w *Writer) Write(ctx context.Context, req model.WriteRequest) (model.WriteResult, error) {
	p := writeParams{
		domain: "spec",
		gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID, holder: req.LeaseHolder, epoch: req.LeaseEpoch,
	}

	return w.execWrite(ctx, p, func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error) {
		var uid uuid.UUID
		var version int64

		if req.ExpectedVersion == 0 {
			err := tx.QueryRow(ctx, `
				INSERT INTO kubernetes_resources
					(gvk, namespace, name, bucket_id, gvk_bucket_seq,
					 object_version, spec, status, metadata, deletion_timestamp)
				VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9)
				RETURNING uid, object_version`,
				req.GVK, req.Namespace, req.Name, req.BucketID, seq,
				req.Spec, req.Status, req.Metadata, req.DeletionTimestamp,
			).Scan(&uid, &version)
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "23505" {
					return uuid.Nil, 0, ErrAlreadyExists
				}
				return uuid.Nil, 0, fmt.Errorf("create resource: %w", err)
			}
		} else {
			err := tx.QueryRow(ctx, `
				UPDATE kubernetes_resources
				SET gvk_bucket_seq = $1,
				    object_version = object_version + 1,
				    spec = $2, status = $3, metadata = $4,
				    deletion_timestamp = $5, updated_at = now()
				WHERE gvk = $6 AND namespace = $7 AND name = $8
				  AND object_version = $9
				RETURNING uid, object_version`,
				seq, req.Spec, req.Status, req.Metadata, req.DeletionTimestamp,
				req.GVK, req.Namespace, req.Name, req.ExpectedVersion,
			).Scan(&uid, &version)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return uuid.Nil, 0, ErrConflict
				}
				return uuid.Nil, 0, fmt.Errorf("update resource: %w", err)
			}
		}

		return uid, version, nil
	})
}

// WriteStatus updates only the status sub-resource, fencing against the
// status row of bucket_leases instead of the spec row. The object must already
// exist (ExpectedVersion > 0). Spec, metadata, and deletion_timestamp are not
// touched. The shared gvk_bucket_seq counter and object_version are bumped so
// watchers see status changes in the same ordered stream as spec changes.
func (w *Writer) WriteStatus(ctx context.Context, req model.StatusWriteRequest) (model.WriteResult, error) {
	if req.ExpectedVersion == 0 {
		return model.WriteResult{}, fmt.Errorf("WriteStatus requires ExpectedVersion > 0: object must exist")
	}

	p := writeParams{
		domain: "status",
		gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID, holder: req.LeaseHolder, epoch: req.LeaseEpoch,
	}

	return w.execWrite(ctx, p, func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error) {
		var uid uuid.UUID
		var version int64

		err := tx.QueryRow(ctx, `
			UPDATE kubernetes_resources
			SET gvk_bucket_seq = $1,
			    object_version = object_version + 1,
			    status = $2, updated_at = now()
			WHERE gvk = $3 AND namespace = $4 AND name = $5
			  AND object_version = $6
			RETURNING uid, object_version`,
			seq, req.Status,
			req.GVK, req.Namespace, req.Name, req.ExpectedVersion,
		).Scan(&uid, &version)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return uuid.Nil, 0, ErrConflict
			}
			return uuid.Nil, 0, fmt.Errorf("update status: %w", err)
		}

		return uid, version, nil
	})
}

// ReadBack resolves an ambiguous commit by checking if the write actually landed.
// Returns the resource if found at the expected seq, nil if not.
func (w *Writer) ReadBack(ctx context.Context, gvk, namespace, name string, seq int64) (*model.Resource, error) {
	r := &model.Resource{}
	err := w.conn.QueryRow(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata, deletion_timestamp,
		       created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND namespace = $2 AND name = $3
		  AND gvk_bucket_seq = $4`,
		gvk, namespace, name, seq,
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

type upsertFunc func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error)

type writeParams struct {
	domain    string
	gvk       string
	namespace  string
	name       string
	bucketID   int
	holder     string
	epoch      int64
}

func (w *Writer) execWrite(ctx context.Context, p writeParams, upsert upsertFunc) (model.WriteResult, error) {
	tx, err := w.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var fenceOK int
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM bucket_leases
		WHERE bucket_id = $1 AND domain = $2 AND holder = $3
		  AND epoch = $4 AND expires_at > now()
		FOR SHARE`,
		p.bucketID, p.domain, p.holder, p.epoch).Scan(&fenceOK)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.WriteResult{}, ErrFenceViolation
		}
		return model.WriteResult{}, fmt.Errorf("fence: %w", err)
	}

	if err := w.hooks.AfterFence(ctx, tx); err != nil {
		return model.WriteResult{}, err
	}

	var seq int64
	err = tx.QueryRow(ctx, `
		INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
		VALUES ($1, $2, 1)
		ON CONFLICT (bucket_id, gvk)
		DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
		RETURNING current_seq`,
		p.bucketID, p.gvk).Scan(&seq)
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("counter: %w", err)
	}

	if err := w.hooks.AfterCounter(ctx, tx, seq); err != nil {
		return model.WriteResult{}, err
	}

	uid, version, err := upsert(ctx, tx, seq)
	if err != nil {
		return model.WriteResult{}, err
	}

	_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`,
		fmt.Sprintf("resource_changes_b%d", p.bucketID))
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("doorbell: %w", err)
	}

	if err := w.hooks.BeforeCommit(ctx, tx); err != nil {
		return model.WriteResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return model.WriteResult{}, &AmbiguousCommitError{
			Cause:     err,
			GVK:       p.gvk,
			Namespace: p.namespace,
			Name:      p.name,
			Seq:       seq,
		}
	}

	return model.WriteResult{Seq: seq, ObjectVersion: version, UID: uid}, nil
}
