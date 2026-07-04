package crbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

// Client provides the reconciler-facing CRUD operations. It wraps the internal
// writer and raw pgx queries, mapping errors to the crbridge sentinel set.
type Client struct {
	connFactory func() (*pgx.Conn, error)
	gvk         string
	assign      BucketAssigner
	holder      string
	epoch       int64
}

// NewClient creates a Client.
//   - connFactory: called to obtain a connection for each operation.
//   - gvk: the GroupVersionKind this client operates on.
//   - assign: maps (namespace, name) → bucket ID.
//   - holder/epoch: lease identity for fencing.
func NewClient(
	connFactory func() (*pgx.Conn, error),
	gvk string,
	assign BucketAssigner,
	holder string,
	epoch int64,
) *Client {
	return &Client{
		connFactory: connFactory,
		gvk:         gvk,
		assign:      assign,
		holder:      holder,
		epoch:       epoch,
	}
}

// Get retrieves a single resource by namespace/name.
func (c *Client) Get(ctx context.Context, namespace, name string) (*Object, error) {
	conn, err := c.connFactory()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	var r model.Resource
	err = conn.QueryRow(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND namespace = $2 AND name = $3
		  AND deletion_timestamp IS NULL`,
		c.gvk, namespace, name,
	).Scan(
		&r.GVK, &r.Namespace, &r.Name, &r.UID, &r.BucketID,
		&r.GVKBucketSeq, &r.ObjectVersion, &r.Spec, &r.Status,
		&r.Metadata, &r.DeletionTimestamp, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get: %w", err)
	}

	return objectFromResource(r), nil
}

// Create inserts a new resource. Returns ErrAlreadyExists if the key exists.
func (c *Client) Create(ctx context.Context, namespace, name string, spec, status, metadata json.RawMessage) (*Object, error) {
	conn, err := c.connFactory()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	w := writer.New(conn, nil)
	result, err := w.Write(ctx, model.WriteRequest{
		GVK:             c.gvk,
		Namespace:       namespace,
		Name:            name,
		BucketID:        c.assign(namespace, name),
		Spec:            spec,
		Status:          status,
		Metadata:        metadata,
		ExpectedVersion: 0,
		LeaseHolder:     c.holder,
		LeaseEpoch:      c.epoch,
	})
	if err != nil {
		return nil, c.mapError(ctx, w, err)
	}

	return c.resultToObject(namespace, name, result), nil
}

// Update writes the spec (and metadata) for an existing resource.
// obj.ResourceVersion must match the current object_version (optimistic locking).
func (c *Client) Update(ctx context.Context, obj *Object) (*Object, error) {
	version, err := obj.ObjectVersion()
	if err != nil {
		return nil, fmt.Errorf("parse resource version: %w", err)
	}

	conn, err := c.connFactory()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	w := writer.New(conn, nil)
	result, err := w.Write(ctx, model.WriteRequest{
		GVK:             c.gvk,
		Namespace:       obj.Namespace,
		Name:            obj.Name,
		BucketID:        c.assign(obj.Namespace, obj.Name),
		Spec:            obj.Spec,
		Status:          obj.Status,
		Metadata:        obj.Metadata,
		ExpectedVersion: version,
		LeaseHolder:     c.holder,
		LeaseEpoch:      c.epoch,
	})
	if err != nil {
		return nil, c.mapError(ctx, w, err)
	}

	return c.resultToObject(obj.Namespace, obj.Name, result), nil
}

// Delete marks a resource as deleted by setting the deletion timestamp.
func (c *Client) Delete(ctx context.Context, obj *Object) error {
	version, err := obj.ObjectVersion()
	if err != nil {
		return fmt.Errorf("parse resource version: %w", err)
	}

	conn, err := c.connFactory()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	now := time.Now()
	w := writer.New(conn, nil)
	_, err = w.Write(ctx, model.WriteRequest{
		GVK:               c.gvk,
		Namespace:         obj.Namespace,
		Name:              obj.Name,
		BucketID:          c.assign(obj.Namespace, obj.Name),
		Spec:              obj.Spec,
		Status:            obj.Status,
		Metadata:          obj.Metadata,
		DeletionTimestamp: &now,
		ExpectedVersion:   version,
		LeaseHolder:       c.holder,
		LeaseEpoch:        c.epoch,
	})
	if err != nil {
		return c.mapError(ctx, w, err)
	}

	return nil
}

// Status returns a StatusClient for the Status().Update() pattern.
func (c *Client) Status() *StatusClient {
	return &StatusClient{c: c}
}

func (c *Client) updateStatus(obj *Object, status json.RawMessage) (*Object, error) {
	version, err := obj.ObjectVersion()
	if err != nil {
		return nil, fmt.Errorf("parse resource version: %w", err)
	}

	ctx := context.Background()

	conn, err := c.connFactory()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	w := writer.New(conn, nil)
	result, err := w.WriteStatus(ctx, model.StatusWriteRequest{
		GVK:             c.gvk,
		Namespace:       obj.Namespace,
		Name:            obj.Name,
		BucketID:        c.assign(obj.Namespace, obj.Name),
		Status:          status,
		ExpectedVersion: version,
		LeaseHolder:     c.holder,
		LeaseEpoch:      c.epoch,
	})
	if err != nil {
		return nil, c.mapError(ctx, w, err)
	}

	return c.resultToObject(obj.Namespace, obj.Name, result), nil
}

func (c *Client) mapError(ctx context.Context, w *writer.Writer, err error) error {
	if errors.Is(err, writer.ErrAlreadyExists) {
		return ErrAlreadyExists
	}
	if errors.Is(err, writer.ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, writer.ErrFenceViolation) {
		return ErrFenced
	}

	var ace *writer.AmbiguousCommitError
	if errors.As(err, &ace) {
		r, rbErr := w.ReadBack(ctx, ace.GVK, ace.Namespace, ace.Name, ace.Seq)
		if rbErr != nil {
			return fmt.Errorf("ambiguous commit + read-back failed: %w (original: %v)", rbErr, err)
		}
		if r == nil {
			return ErrConflict
		}
		return nil
	}

	return err
}

func (c *Client) resultToObject(namespace, name string, result model.WriteResult) *Object {
	return &Object{
		GVK:             c.gvk,
		Namespace:       namespace,
		Name:            name,
		UID:             result.UID,
		ResourceVersion: fmt.Sprintf("%d", result.ObjectVersion),
		BucketID:        c.assign(namespace, name),
	}
}
