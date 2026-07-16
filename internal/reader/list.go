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

type ListFilter struct {
	WhereClauses []string
	WhereArgs    []interface{}
	Limit        int64
	Offset       int64
}

// List performs a REPEATABLE READ snapshot read of all live and dying resources
// matching the given GVK. Fully-deleted tombstones (deletion_timestamp set, no
// finalizers) are excluded by the query. Dying objects (deletion_timestamp set,
// has finalizers) are included so controllers can perform cleanup before
// removing their finalizers. The returned RV uses the xmin watermark from the
// same snapshot, so there is no skew between the data and the version.
func List(ctx context.Context, conn *pgx.Conn, gvk string, filter ...*ListFilter) (*ListResult, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("list begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var xmin uint64
	err = tx.QueryRow(ctx, `SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint`).Scan(&xmin)
	if err != nil {
		return nil, fmt.Errorf("list xmin: %w", err)
	}

	var watermark uint64
	if xmin > 0 {
		watermark = xmin - 1
	}
	rv := resourceversion.RV{Watermark: watermark}

	query := `
		SELECT gvk, namespace, name, uid, txid_stamp::text::bigint,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1
		  AND (deletion_timestamp IS NULL OR metadata->'finalizers' != '[]'::jsonb)` // tombstone filter: also in compactor.go, 001_initial.sql, writer.go
	args := []interface{}{gvk}

	var f *ListFilter
	if len(filter) > 0 && filter[0] != nil {
		f = filter[0]
	}
	if f != nil {
		for _, clause := range f.WhereClauses {
			query += " AND " + clause
		}
		args = append(args, f.WhereArgs...)
	}
	query += " ORDER BY txid_stamp"
	if f != nil && f.Limit > 0 {
		args = append(args, f.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
		if f.Offset > 0 {
			args = append(args, f.Offset)
			query += fmt.Sprintf(" OFFSET $%d", len(args))
		}
	}

	resourceRows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list resources: %w", err)
	}
	defer resourceRows.Close()

	var resources []model.Resource
	for resourceRows.Next() {
		var r model.Resource
		if err := resourceRows.Scan(
			&r.GVK, &r.Namespace, &r.Name, &r.UID,
			&r.TxidStamp, &r.ObjectVersion, &r.Spec, &r.Status,
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
