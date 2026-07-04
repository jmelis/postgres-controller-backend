// Package crbridge adapts the postgres-controller-backend storage layer to a
// controller-runtime-shaped interface. Reconcilers consume this package; the
// internal pgx types, model structs, and raw RV representations stay hidden.
//
// Key contract:
//   - Per-object metadata.resourceVersion = object_version (for Update conflict detection)
//   - List/Watch resourceVersion = composite RV string (e{epoch}|b{id}:{seq},...)
//   - These differing representations are consistent with Kubernetes semantics:
//     object RV and list RV are both opaque strings from different counters.
package crbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
)

// EventType mirrors watch.EventType.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
	EventBookmark EventType = "BOOKMARK"
	EventError    EventType = "ERROR"
)

// Event mirrors watch.Event.
type Event struct {
	Type   EventType
	Object *Object
}

// Object is the reconciler-facing representation of a stored resource.
// It mirrors unstructured.Unstructured: a map-based object with standard
// metadata fields populated from the storage columns.
type Object struct {
	GVK       string
	Namespace string
	Name      string
	UID       uuid.UUID

	// ResourceVersion is the per-object version (object_version), used for
	// Update conflict detection. NOT the list/watch composite RV.
	ResourceVersion string

	Spec     json.RawMessage
	Status   json.RawMessage
	Metadata json.RawMessage

	Deleted bool

	BucketID int
}

func objectFromResource(r model.Resource) *Object {
	return &Object{
		GVK:             r.GVK,
		Namespace:       r.Namespace,
		Name:            r.Name,
		UID:             r.UID,
		ResourceVersion: strconv.FormatInt(r.ObjectVersion, 10),
		Spec:            r.Spec,
		Status:          r.Status,
		Metadata:        r.Metadata,
		Deleted:         r.DeletionTimestamp != nil,
		BucketID:        r.BucketID,
	}
}

// ObjectVersion parses the ResourceVersion back to int64.
func (o *Object) ObjectVersion() (int64, error) {
	return strconv.ParseInt(o.ResourceVersion, 10, 64)
}

// BucketAssigner maps (namespace, name) → bucketID. Callers provide this as
// configuration, e.g., FNV hash mod bucket count.
type BucketAssigner func(namespace, name string) int

// Errors returned by the Client facade. Reconcilers check these with errors.Is.
var (
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict: object version mismatch (409)")
	ErrFenced        = errors.New("fenced: lease not held or epoch mismatch (409)")
	ErrGone          = errors.New("410 Gone: resource version too old")
	ErrNotFound      = errors.New("not found")
)

// StatusClient provides the Status().Update() pattern.
type StatusClient struct {
	c *Client
}

func (s *StatusClient) Update(obj *Object, status json.RawMessage) (*Object, error) {
	return s.c.updateStatus(obj, status)
}

// Key returns the namespaced key for an object (namespace/name).
func Key(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return fmt.Sprintf("%s/%s", namespace, name)
}
