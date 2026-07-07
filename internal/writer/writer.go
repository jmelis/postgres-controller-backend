package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmelis/postgres-controller-backend/internal/metrics"
	"github.com/jmelis/postgres-controller-backend/internal/model"
)

type Writer struct {
	conn    *pgx.Conn
	hooks   TxHooks
	metrics *metrics.WriterMetrics
}

func New(conn *pgx.Conn, hooks TxHooks) *Writer {
	return &Writer{conn: conn, hooks: hooks}
}

// WithMetrics attaches Prometheus metrics to the writer.
func (w *Writer) WithMetrics(m *metrics.WriterMetrics) *Writer {
	w.metrics = m
	return w
}

func (w *Writer) Write(ctx context.Context, req model.WriteRequest) (model.WriteResult, error) {
	if w.hooks != nil {
		return w.writeMultiStatement(ctx, req)
	}
	return w.writeStoredProc(ctx, req)
}

// WriteStatus updates only the status sub-resource. The object must already
// exist (ExpectedVersion > 0). Spec, metadata, and deletion_timestamp are not
// touched. The shared gvk_bucket_seq counter and object_version are bumped so
// watchers see status changes in the same ordered stream as spec changes.
func (w *Writer) WriteStatus(ctx context.Context, req model.StatusWriteRequest) (model.WriteResult, error) {
	if req.ExpectedVersion == 0 {
		return model.WriteResult{}, fmt.Errorf("WriteStatus requires ExpectedVersion > 0: object must exist")
	}

	if w.hooks != nil {
		return w.writeStatusMultiStatement(ctx, req)
	}
	return w.writeStatusStoredProc(ctx, req)
}

// WriteObject updates spec, metadata, and deletion_timestamp without touching
// status. The object must already exist (ExpectedVersion > 0). The shared
// gvk_bucket_seq counter and object_version are bumped so watchers see changes
// in the ordered stream.
func (w *Writer) WriteObject(ctx context.Context, req model.ObjectWriteRequest) (model.WriteResult, error) {
	if req.ExpectedVersion == 0 {
		return model.WriteResult{}, fmt.Errorf("WriteObject requires ExpectedVersion > 0: object must exist")
	}

	if w.hooks != nil {
		return w.writeObjectMultiStatement(ctx, req)
	}
	return w.writeObjectStoredProc(ctx, req)
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

// --- Stored procedure path (production, hooks==nil) ---

const pgctlWriteSQL = `SELECT * FROM pgctl_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`

func (w *Writer) writeStoredProc(ctx context.Context, req model.WriteRequest) (model.WriteResult, error) {
	start := time.Now()

	result, err := w.callStoredProc(ctx, writeParams{
		statusOnly: false, gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		expectedVersion: req.ExpectedVersion, forceWrite: req.ForceWrite,
		spec: req.Spec, status: req.Status, metadata: req.Metadata,
		deletionTimestamp: req.DeletionTimestamp,
	})

	w.observeResult(start, req.GVK, req.BucketID, result, err)
	return result, err
}

func (w *Writer) writeStatusStoredProc(ctx context.Context, req model.StatusWriteRequest) (model.WriteResult, error) {
	start := time.Now()

	result, err := w.callStoredProc(ctx, writeParams{
		statusOnly: true, gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		expectedVersion: req.ExpectedVersion, forceWrite: req.ForceWrite,
		spec: nil, status: req.Status, metadata: nil,
		deletionTimestamp: nil,
	})

	w.observeResult(start, req.GVK, req.BucketID, result, err)
	return result, err
}

func (w *Writer) writeObjectStoredProc(ctx context.Context, req model.ObjectWriteRequest) (model.WriteResult, error) {
	start := time.Now()

	result, err := w.callStoredProc(ctx, writeParams{
		statusOnly: false, gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		expectedVersion: req.ExpectedVersion, forceWrite: req.ForceWrite,
		spec: req.Spec, status: nil, metadata: req.Metadata,
		deletionTimestamp: req.DeletionTimestamp,
	})

	w.observeResult(start, req.GVK, req.BucketID, result, err)
	return result, err
}

func (w *Writer) callStoredProc(ctx context.Context, p writeParams) (model.WriteResult, error) {
	t0 := time.Now()
	tx, err := w.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var uid uuid.UUID
	var version, seq int64
	var changed bool
	var suppressUs, counterUs, upsertUs int64

	err = tx.QueryRow(ctx, pgctlWriteSQL,
		p.statusOnly, p.gvk, p.namespace, p.name, p.bucketID,
		p.expectedVersion, p.forceWrite,
		p.spec, p.status, p.metadata, p.deletionTimestamp,
	).Scan(&uid, &version, &seq, &changed, &suppressUs, &counterUs, &upsertUs)
	w.observeStep("stored_proc", time.Since(t0))

	if err != nil {
		return model.WriteResult{}, mapStoredProcError(err)
	}

	w.observeStep("suppression_check", time.Duration(suppressUs)*time.Microsecond)
	w.observeStep("counter_increment", time.Duration(counterUs)*time.Microsecond)
	w.observeStep("upsert", time.Duration(upsertUs)*time.Microsecond)

	if !changed {
		t0 = time.Now()
		if err := tx.Commit(ctx); err != nil {
			return model.WriteResult{}, fmt.Errorf("commit (suppressed): %w", err)
		}
		w.observeStep("commit", time.Since(t0))
		return model.WriteResult{ObjectVersion: version, UID: uid, Changed: false}, nil
	}

	t0 = time.Now()
	if err := tx.Commit(ctx); err != nil {
		return model.WriteResult{}, &AmbiguousCommitError{
			Cause: err, GVK: p.gvk, Namespace: p.namespace, Name: p.name, Seq: seq,
		}
	}
	w.observeStep("commit", time.Since(t0))

	w.fireDoorbell(ctx, p.bucketID)

	return model.WriteResult{Seq: seq, ObjectVersion: version, UID: uid, Changed: true}, nil
}

func mapStoredProcError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "P0002":
			return ErrConflict
		case "P0003":
			return ErrAlreadyExists
		case "P0004":
			return fmt.Errorf("WriteStatus requires ExpectedVersion > 0: object must exist")
		}
	}
	return fmt.Errorf("stored proc: %w", err)
}

func (w *Writer) fireDoorbell(ctx context.Context, bucketID int) {
	channel := fmt.Sprintf("resource_changes_b%d", bucketID)
	t0 := time.Now()
	_, err := w.conn.Exec(ctx, `SELECT pg_notify($1, '')`, channel)
	w.observeStep("doorbell_external", time.Since(t0))
	if err != nil {
		log.Printf("doorbell send failed (non-fatal): %v", err)
		if w.metrics != nil {
			w.metrics.DoorbellErrorsTotal.Inc()
		}
	}
}

// --- Multi-statement path (test hooks, hooks!=nil) ---

func (w *Writer) writeMultiStatement(ctx context.Context, req model.WriteRequest) (model.WriteResult, error) {
	p := writeParams{
		statusOnly: false,
		gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		forceWrite: req.ForceWrite,
	}

	checker := func(existing *existingRow) bool {
		return JSONEqual(existing.spec, req.Spec) &&
			JSONEqual(existing.status, req.Status) &&
			JSONEqual(existing.metadata, req.Metadata) &&
			timeEqual(existing.deletionTimestamp, req.DeletionTimestamp)
	}

	return w.execWrite(ctx, p, checker, func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error) {
		var uid uuid.UUID
		var version int64

		if req.ExpectedVersion == 0 {
			if _, err := tx.Exec(ctx, "SAVEPOINT tombstone_revival"); err != nil {
				return uuid.Nil, 0, fmt.Errorf("savepoint: %w", err)
			}
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
					if _, err2 := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT tombstone_revival"); err2 != nil {
						return uuid.Nil, 0, fmt.Errorf("rollback savepoint: %w", err2)
					}
					err = tx.QueryRow(ctx, `
						UPDATE kubernetes_resources
						   SET uid                = gen_random_uuid(),
						       bucket_id          = $4,
						       gvk_bucket_seq     = $5,
						       object_version     = 1,
						       spec               = $6,
						       status             = COALESCE($7, '{}'::jsonb),
						       metadata           = $8,
						       deletion_timestamp = NULL,
						       created_at         = now(),
						       updated_at         = now()
						 WHERE gvk = $1 AND namespace = $2 AND name = $3
						   AND deletion_timestamp IS NOT NULL
						   AND (metadata->'finalizers' IS NULL OR metadata->'finalizers' = '[]'::jsonb) -- tombstone filter: also in list.go, compactor.go, 001_initial.sql
						RETURNING uid, object_version`,
						req.GVK, req.Namespace, req.Name, req.BucketID, seq,
						req.Spec, req.Status, req.Metadata,
					).Scan(&uid, &version)
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return uuid.Nil, 0, ErrAlreadyExists
						}
						return uuid.Nil, 0, fmt.Errorf("tombstone revival: %w", err)
					}
				} else {
					return uuid.Nil, 0, fmt.Errorf("create resource: %w", err)
				}
			}
		} else {
			err := tx.QueryRow(ctx, `
				UPDATE kubernetes_resources
				SET gvk_bucket_seq = $1,
				    object_version = object_version + 1,
				    spec = $2, status = COALESCE($3, status), metadata = $4,
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

func (w *Writer) writeStatusMultiStatement(ctx context.Context, req model.StatusWriteRequest) (model.WriteResult, error) {
	p := writeParams{
		statusOnly: true,
		gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		forceWrite: req.ForceWrite,
	}

	checker := func(existing *existingRow) bool {
		return JSONEqual(existing.status, req.Status)
	}

	return w.execWrite(ctx, p, checker, func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error) {
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

func (w *Writer) writeObjectMultiStatement(ctx context.Context, req model.ObjectWriteRequest) (model.WriteResult, error) {
	p := writeParams{
		statusOnly: false,
		gvk: req.GVK, namespace: req.Namespace, name: req.Name,
		bucketID: req.BucketID,
		forceWrite: req.ForceWrite,
	}

	checker := func(existing *existingRow) bool {
		return JSONEqual(existing.spec, req.Spec) &&
			JSONEqual(existing.metadata, req.Metadata) &&
			timeEqual(existing.deletionTimestamp, req.DeletionTimestamp)
	}

	return w.execWrite(ctx, p, checker, func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error) {
		var uid uuid.UUID
		var version int64

		err := tx.QueryRow(ctx, `
			UPDATE kubernetes_resources
			SET gvk_bucket_seq = $1,
			    object_version = object_version + 1,
			    spec = $2, metadata = $3, deletion_timestamp = $4,
			    updated_at = now()
			WHERE gvk = $5 AND namespace = $6 AND name = $7
			  AND object_version = $8
			RETURNING uid, object_version`,
			seq, req.Spec, req.Metadata, req.DeletionTimestamp,
			req.GVK, req.Namespace, req.Name, req.ExpectedVersion,
		).Scan(&uid, &version)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return uuid.Nil, 0, ErrConflict
			}
			return uuid.Nil, 0, fmt.Errorf("update spec: %w", err)
		}

		return uid, version, nil
	})
}

type upsertFunc func(ctx context.Context, tx pgx.Tx, seq int64) (uuid.UUID, int64, error)

// contentChecker returns true if the existing row's content matches the request
// (i.e., the write is a no-op). Only called when suppression is active and the
// row exists.
type contentChecker func(existing *existingRow) bool

type existingRow struct {
	uid               uuid.UUID
	objectVersion     int64
	spec              json.RawMessage
	status            json.RawMessage
	metadata          json.RawMessage
	deletionTimestamp *time.Time
}

type writeParams struct {
	statusOnly        bool
	gvk               string
	namespace         string
	name              string
	bucketID          int
	expectedVersion   int64
	forceWrite        bool
	spec              json.RawMessage
	status            json.RawMessage
	metadata          json.RawMessage
	deletionTimestamp *time.Time
}

func (w *Writer) execWrite(ctx context.Context, p writeParams, isContentEqual contentChecker, upsert upsertFunc) (model.WriteResult, error) {
	start := time.Now()
	result, err := w.execWriteInner(ctx, p, isContentEqual, upsert)

	w.observeResult(start, p.gvk, p.bucketID, result, err)
	return result, err
}

func (w *Writer) observeResult(start time.Time, gvk string, bucketID int, result model.WriteResult, err error) {
	if w.metrics == nil {
		return
	}
	dur := time.Since(start)
	bucketStr := strconv.Itoa(bucketID)
	var resultLabel string
	switch {
	case err == nil && !result.Changed:
		resultLabel = "noop"
		w.metrics.NoopSuppressionsTotal.Inc()
	case err == nil:
		resultLabel = "success"
	case errors.Is(err, ErrConflict):
		resultLabel = "conflict"
	case errors.Is(err, ErrAlreadyExists):
		resultLabel = "already_exists"
	default:
		var ambErr *AmbiguousCommitError
		if errors.As(err, &ambErr) {
			resultLabel = "ambiguous_commit"
		} else {
			resultLabel = "error"
		}
	}
	w.metrics.WriteDuration.WithLabelValues(gvk, bucketStr, resultLabel).Observe(dur.Seconds())
	w.metrics.WritesTotal.WithLabelValues(gvk, bucketStr, resultLabel).Inc()
}

func (w *Writer) observeStep(step string, d time.Duration) {
	if w.metrics != nil {
		w.metrics.WriteStepDuration.WithLabelValues(step).Observe(d.Seconds())
	}
}

func (w *Writer) execWriteInner(ctx context.Context, p writeParams, isContentEqual contentChecker, upsert upsertFunc) (model.WriteResult, error) {
	t0 := time.Now()
	tx, err := w.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("begin: %w", err)
	}
	w.observeStep("begin_tx", time.Since(t0))
	defer tx.Rollback(ctx) //nolint:errcheck

	t0 = time.Now()
	if !p.forceWrite {
		existing, err := readExisting(ctx, tx, p.gvk, p.namespace, p.name)
		w.observeStep("suppression_check", time.Since(t0))
		if err != nil {
			return model.WriteResult{}, err
		}

		suppressed := existing != nil && isContentEqual(existing)

		if err := w.hooks.AfterSuppressionCheck(ctx, tx, suppressed); err != nil {
			return model.WriteResult{}, err
		}

		if suppressed {
			t0 = time.Now()
			if err := tx.Commit(ctx); err != nil {
				return model.WriteResult{}, fmt.Errorf("commit (suppressed): %w", err)
			}
			w.observeStep("commit", time.Since(t0))
			return model.WriteResult{
				ObjectVersion: existing.objectVersion,
				UID:           existing.uid,
				Changed:       false,
			}, nil
		}
	} else {
		w.observeStep("suppression_check", time.Since(t0))
		if err := w.hooks.AfterSuppressionCheck(ctx, tx, false); err != nil {
			return model.WriteResult{}, err
		}
	}

	t0 = time.Now()
	var seq int64
	err = tx.QueryRow(ctx, `
		INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
		VALUES ($1, $2, 1)
		ON CONFLICT (bucket_id, gvk)
		DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
		RETURNING current_seq`,
		p.bucketID, p.gvk).Scan(&seq)
	w.observeStep("counter_increment", time.Since(t0))
	if err != nil {
		return model.WriteResult{}, fmt.Errorf("counter: %w", err)
	}

	if err := w.hooks.AfterCounter(ctx, tx, seq); err != nil {
		return model.WriteResult{}, err
	}

	t0 = time.Now()
	uid, version, err := upsert(ctx, tx, seq)
	w.observeStep("upsert", time.Since(t0))
	if err != nil {
		return model.WriteResult{}, err
	}

	if err := w.hooks.BeforeCommit(ctx, tx); err != nil {
		return model.WriteResult{}, err
	}

	t0 = time.Now()
	if err := tx.Commit(ctx); err != nil {
		return model.WriteResult{}, &AmbiguousCommitError{
			Cause:     err,
			GVK:       p.gvk,
			Namespace: p.namespace,
			Name:      p.name,
			Seq:       seq,
		}
	}
	w.observeStep("commit", time.Since(t0))

	w.fireDoorbell(ctx, p.bucketID)

	return model.WriteResult{Seq: seq, ObjectVersion: version, UID: uid, Changed: true}, nil
}

func readExisting(ctx context.Context, tx pgx.Tx, gvk, namespace, name string) (*existingRow, error) {
	row := &existingRow{}
	err := tx.QueryRow(ctx, `
		SELECT uid, object_version, spec, status, metadata, deletion_timestamp
		FROM kubernetes_resources
		WHERE gvk = $1 AND namespace = $2 AND name = $3`,
		gvk, namespace, name,
	).Scan(&row.uid, &row.objectVersion, &row.spec, &row.status, &row.metadata, &row.deletionTimestamp)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("read existing: %w", err)
	}
	return row, nil
}

func JSONEqual(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	ra, _ := json.Marshal(va)
	rb, _ := json.Marshal(vb)
	return string(ra) == string(rb)
}

func timeEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
