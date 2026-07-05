package crbridge

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// TypedObject is a generic resource where the Spec field has type S and the
// Status field has type T. It carries the same metadata as Object (namespace,
// name, UID, resourceVersion, etc.) but eliminates the need for manual
// json.Unmarshal/Marshal in controller code.
type TypedObject[S any, T any] struct {
	GVK             string
	Namespace       string
	Name            string
	UID             uuid.UUID
	ResourceVersion string
	Spec            S
	Status          T
	Metadata        json.RawMessage
	Deleted         bool
	BucketID        int
}

// TypedListResult holds the result of a typed list operation.
type TypedListResult[S any, T any] struct {
	Objects         []*TypedObject[S, T]
	ResourceVersion string
}

// TypedClient wraps an untyped Client and ListerWatcher, providing type-safe
// CRUD operations for a single GVK. S is the spec type, T is the status type.
//
// It bundles both read and write operations into a single object per GVK,
// so controllers don't need to track separate Client and ListerWatcher
// instances.
type TypedClient[S any, T any] struct {
	client *Client
	lw     *ListerWatcher
}

// NewTypedClient creates a TypedClient wrapping an existing Client and
// ListerWatcher. Both must be configured for the same GVK.
func NewTypedClient[S any, T any](client *Client, lw *ListerWatcher) *TypedClient[S, T] {
	return &TypedClient[S, T]{client: client, lw: lw}
}

// Get retrieves a resource by namespace/name and returns a typed object.
// Returns ErrNotFound if the resource does not exist.
func (tc *TypedClient[S, T]) Get(ctx context.Context, namespace, name string) (*TypedObject[S, T], error) {
	obj, err := tc.client.Get(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return toTyped[S, T](obj)
}

// Create inserts a new resource with the given spec. Status defaults to the
// zero value of T. Returns ErrAlreadyExists if the key already exists.
func (tc *TypedClient[S, T]) Create(ctx context.Context, namespace, name string, spec S) (*TypedObject[S, T], error) {
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshal spec: %w", err)
	}
	var zeroStatus T
	statusJSON, err := json.Marshal(zeroStatus)
	if err != nil {
		return nil, fmt.Errorf("marshal status: %w", err)
	}

	_, err = tc.client.Create(ctx, namespace, name, specJSON, statusJSON, json.RawMessage(`{}`))
	if err != nil {
		return nil, err
	}
	// Re-read because resultToObject returns a partial Object (no spec/status).
	return tc.Get(ctx, namespace, name)
}

// Update writes the spec of a typed object back to storage. The caller
// mutates tobj.Spec then calls Update. Optimistic locking is enforced via
// ResourceVersion — returns ErrConflict on mismatch.
func (tc *TypedClient[S, T]) Update(ctx context.Context, tobj *TypedObject[S, T]) (*TypedObject[S, T], error) {
	obj, err := fromTyped(tobj)
	if err != nil {
		return nil, err
	}
	_, err = tc.client.Update(ctx, obj)
	if err != nil {
		return nil, err
	}
	// Re-read for the updated ResourceVersion and full object.
	return tc.Get(ctx, tobj.Namespace, tobj.Name)
}

// Delete marks a resource as deleted (soft delete).
func (tc *TypedClient[S, T]) Delete(ctx context.Context, tobj *TypedObject[S, T]) error {
	obj, err := fromTyped(tobj)
	if err != nil {
		return err
	}
	return tc.client.Delete(ctx, obj)
}

// List returns all live objects for the configured buckets with typed
// spec/status fields.
func (tc *TypedClient[S, T]) List(ctx context.Context) (*TypedListResult[S, T], error) {
	result, err := tc.lw.List(ctx)
	if err != nil {
		return nil, err
	}
	typed := make([]*TypedObject[S, T], 0, len(result.Objects))
	for _, obj := range result.Objects {
		tobj, err := toTyped[S, T](obj)
		if err != nil {
			return nil, err
		}
		typed = append(typed, tobj)
	}
	return &TypedListResult[S, T]{
		Objects:         typed,
		ResourceVersion: result.ResourceVersion,
	}, nil
}

// Status returns a TypedStatusClient for the Status().Update() pattern.
func (tc *TypedClient[S, T]) Status() *TypedStatusClient[S, T] {
	return &TypedStatusClient[S, T]{tc: tc}
}

// Untyped returns the underlying untyped Client, useful when interoperating
// with code that expects the raw Client (e.g. HTTP APIs serving raw JSON).
func (tc *TypedClient[S, T]) Untyped() *Client {
	return tc.client
}

// ListerWatcher returns the underlying ListerWatcher, useful when
// interoperating with code that expects the raw ListerWatcher.
func (tc *TypedClient[S, T]) ListerWatcher() *ListerWatcher {
	return tc.lw
}

// TypedStatusClient provides Status().Update() with typed status values,
// mirroring the controller-runtime pattern.
type TypedStatusClient[S any, T any] struct {
	tc *TypedClient[S, T]
}

// Update writes a new status to the resource. Takes a context (unlike the
// untyped StatusClient which uses context.Background()), the typed object
// for ResourceVersion-based optimistic locking, and the new status value.
func (sc *TypedStatusClient[S, T]) Update(ctx context.Context, tobj *TypedObject[S, T], status T) (*TypedObject[S, T], error) {
	statusJSON, err := json.Marshal(status)
	if err != nil {
		return nil, fmt.Errorf("marshal status: %w", err)
	}
	obj, err := fromTyped(tobj)
	if err != nil {
		return nil, err
	}
	_, err = sc.tc.client.Status().Update(obj, statusJSON)
	if err != nil {
		return nil, err
	}
	return sc.tc.Get(ctx, tobj.Namespace, tobj.Name)
}

// toTyped converts an untyped Object to a TypedObject by unmarshaling the
// JSON spec and status into the parameterized types.
func toTyped[S any, T any](obj *Object) (*TypedObject[S, T], error) {
	var spec S
	if len(obj.Spec) > 0 {
		if err := json.Unmarshal(obj.Spec, &spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec: %w", err)
		}
	}
	var status T
	if len(obj.Status) > 0 {
		if err := json.Unmarshal(obj.Status, &status); err != nil {
			return nil, fmt.Errorf("unmarshal status: %w", err)
		}
	}
	return &TypedObject[S, T]{
		GVK:             obj.GVK,
		Namespace:       obj.Namespace,
		Name:            obj.Name,
		UID:             obj.UID,
		ResourceVersion: obj.ResourceVersion,
		Spec:            spec,
		Status:          status,
		Metadata:        obj.Metadata,
		Deleted:         obj.Deleted,
		BucketID:        obj.BucketID,
	}, nil
}

// fromTyped converts a TypedObject back to an untyped Object by marshaling
// the spec and status to JSON.
func fromTyped[S any, T any](tobj *TypedObject[S, T]) (*Object, error) {
	specJSON, err := json.Marshal(tobj.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal spec: %w", err)
	}
	statusJSON, err := json.Marshal(tobj.Status)
	if err != nil {
		return nil, fmt.Errorf("marshal status: %w", err)
	}
	return &Object{
		GVK:             tobj.GVK,
		Namespace:       tobj.Namespace,
		Name:            tobj.Name,
		UID:             tobj.UID,
		ResourceVersion: tobj.ResourceVersion,
		Spec:            specJSON,
		Status:          statusJSON,
		Metadata:        tobj.Metadata,
		Deleted:         tobj.Deleted,
		BucketID:        tobj.BucketID,
	}, nil
}
