package crbridge

import "context"

// Reconciler is the interface controllers implement. S is the spec type, T is
// the status type. The Manager calls Reconcile whenever the primary object
// changes or a cross-type watch triggers a requeue.
type Reconciler[S any, T any] interface {
	Reconcile(ctx context.Context, obj *TypedObject[S, T]) (Result, error)
}

// ReconcileFunc is a convenience adapter so a plain function can satisfy the
// Reconciler interface without defining a struct.
type ReconcileFunc[S any, T any] func(ctx context.Context, obj *TypedObject[S, T]) (Result, error)

func (f ReconcileFunc[S, T]) Reconcile(ctx context.Context, obj *TypedObject[S, T]) (Result, error) {
	return f(ctx, obj)
}

// ControllerBuilder provides a fluent API for configuring and registering a
// controller with a Manager.
//
// Usage:
//
//	crbridge.NewControllerFor[MySpec, MyStatus](mgr, gvk, reconciler).
//	    Watches(otherGVK, mapFunc).
//	    Complete()
type ControllerBuilder[S any, T any] struct {
	mgr        *Manager
	gvk        string
	reconciler Reconciler[S, T]
	watches    []watchSource
}

// NewControllerFor starts building a controller for the given GVK.
// The primary type is automatically watched with identity mapping — events
// for this GVK produce Requests with the object's own namespace/name.
//
// This is a top-level generic function (not a method on Manager) because Go
// does not support type parameters on methods.
func NewControllerFor[S any, T any](mgr *Manager, gvk string, reconciler Reconciler[S, T]) *ControllerBuilder[S, T] {
	return &ControllerBuilder[S, T]{
		mgr:        mgr,
		gvk:        gvk,
		reconciler: reconciler,
		watches: []watchSource{
			{gvk: gvk, mapFn: nil}, // primary watch — identity mapping
		},
	}
}

// Watches adds a cross-type watch. When an object of watchGVK changes, mapFn
// is called to determine which primary objects to requeue. For example, when
// a GreetingPolicy changes, mapFn might list all Greetings in that namespace
// and return a Request for each.
func (b *ControllerBuilder[S, T]) Watches(watchGVK string, mapFn MapFunc) *ControllerBuilder[S, T] {
	b.watches = append(b.watches, watchSource{gvk: watchGVK, mapFn: mapFn})
	return b
}

// Complete registers the controller with the Manager. Must be called before
// Manager.Start().
//
// Internally, Complete creates a TypedClient[S, T] for the primary GVK using
// the Manager's config (ConnFactory, HolderID, BucketAssigner, etc.) and
// registers a type-erased reconcile closure. The closure calls tc.Get() to
// fetch the typed object, then delegates to the user's Reconciler.
func (b *ControllerBuilder[S, T]) Complete() error {
	epochs := b.mgr.cfg.LeaseEpochs[b.gvk]
	client := NewClient(
		b.mgr.cfg.ConnFactory,
		b.gvk,
		b.mgr.cfg.BucketAssigner,
		b.mgr.cfg.HolderID,
		epochs.Spec,
	)
	lw := NewListerWatcher(b.mgr.cfg.ConnFactory, b.gvk, b.mgr.cfg.BucketIDs)
	tc := NewTypedClient[S, T](client, lw)

	// Type-erased reconcile closure: fetches the typed object then
	// delegates to the user's Reconciler.
	reconcileFn := func(ctx context.Context, namespace, name string) (Result, error) {
		obj, err := tc.Get(ctx, namespace, name)
		if err != nil {
			return Result{}, err
		}
		return b.reconciler.Reconcile(ctx, obj)
	}

	b.mgr.controllers = append(b.mgr.controllers, controllerRegistration{
		gvk:       b.gvk,
		reconcile: reconcileFn,
		watches:   b.watches,
	})
	return nil
}
