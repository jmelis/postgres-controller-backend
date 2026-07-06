package reader

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
)

type ListResult struct {
	Resources       []model.Resource
	ResourceVersion resourceversion.RV
}

// List performs a REPEATABLE READ snapshot read of all live and dying resources
// matching the given GVK across the specified buckets. Fully-deleted tombstones
// (deletion_timestamp set, no finalizers) are excluded by the query. Dying
// objects (deletion_timestamp set, has finalizers) are included so controllers
// can perform cleanup before removing their finalizers. The returned RV is
// built from cluster_epoch + per-bucket counters within the same snapshot,
// so there is no skew between the data and the version (I5/I6 handoff into Watch).
func List(ctx context.Context, conn *pgx.Conn, gvk string, bucketIDs []int) (*ListResult, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("list begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Read timeline epoch
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT timeline_id FROM cluster_epoch`).Scan(&epoch); err != nil {
		return nil, fmt.Errorf("list epoch: %w", err)
	}

	// Read per-bucket counters (build RV)
	rv := resourceversion.RV{Epoch: epoch, Buckets: make(map[int]int64, len(bucketIDs))}

	counterRows, err := tx.Query(ctx, `
		SELECT bucket_id, current_seq FROM gvk_bucket_counters
		WHERE gvk = $1 AND bucket_id = ANY($2)`, gvk, bucketIDs)
	if err != nil {
		return nil, fmt.Errorf("list counters: %w", err)
	}
	defer counterRows.Close()

	for counterRows.Next() {
		var bid int
		var seq int64
		if err := counterRows.Scan(&bid, &seq); err != nil {
			return nil, fmt.Errorf("list counter scan: %w", err)
		}
		rv.Buckets[bid] = seq
	}
	if err := counterRows.Err(); err != nil {
		return nil, fmt.Errorf("list counter rows: %w", err)
	}

	// Buckets with no counter row yet have hwm 0 (no writes ever)
	for _, bid := range bucketIDs {
		if _, ok := rv.Buckets[bid]; !ok {
			rv.Buckets[bid] = 0
		}
	}

	resourceRows, err := tx.Query(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND bucket_id = ANY($2)
		  AND (deletion_timestamp IS NULL OR metadata->'finalizers' != '[]'::jsonb) -- tombstone filter: also in compactor.go, 001_initial.sql, writer.go
		ORDER BY bucket_id, gvk_bucket_seq`, gvk, bucketIDs)
	if err != nil {
		return nil, fmt.Errorf("list resources: %w", err)
	}
	defer resourceRows.Close()

	var resources []model.Resource
	for resourceRows.Next() {
		var r model.Resource
		if err := resourceRows.Scan(
			&r.GVK, &r.Namespace, &r.Name, &r.UID, &r.BucketID,
			&r.GVKBucketSeq, &r.ObjectVersion, &r.Spec, &r.Status,
			&r.Metadata, &r.DeletionTimestamp, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("list resource scan: %w", err)
		}
		resources = append(resources, r)
	}
	if err := resourceRows.Err(); err != nil {
		return nil, fmt.Errorf("list resource rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("list commit: %w", err)
	}

	return &ListResult{Resources: resources, ResourceVersion: rv}, nil
}
