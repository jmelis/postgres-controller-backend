package crbridge

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Result is returned by Reconcile to control requeue behavior.
type Result struct {
	Requeue      bool
	RequeueAfter time.Duration
}

// Request identifies the object to reconcile by namespace and name.
type Request struct {
	Namespace string
	Name      string
}

// MapFunc maps a watched object to zero or more reconcile requests for the
// primary type. Used for cross-type watches — e.g., when a GreetingPolicy
// changes, return Requests for all Greetings in that namespace.
//
// The object is untyped because the Manager's watch loop operates on raw
// events from the ListerWatcher. In the common case (requeue by namespace)
// only obj.Namespace is needed.
type MapFunc func(ctx context.Context, obj *Object) []Request

// LeaseEpochs holds the spec and status lease epochs for a GVK.
type LeaseEpochs struct {
	Spec   int64
	Status int64
}

// ManagerConfig configures a Manager.
type ManagerConfig struct {
	// ConnFactory creates new pgx connections on demand. Called for each
	// Client/ListerWatcher operation and for watch connections.
	ConnFactory func() (*pgx.Conn, error)

	// HolderID identifies this controller instance for lease fencing.
	HolderID string

	// BucketAssigner maps (namespace, name) → bucket ID.
	BucketAssigner BucketAssigner

	// BucketIDs is the set of buckets this controller instance watches.
	BucketIDs []int

	// LeaseEpochs provides the spec and status epochs per GVK.
	// Key is the GVK string.
	LeaseEpochs map[string]LeaseEpochs

	// Logger for internal operations. If nil, a no-op logger is used.
	Logger *slog.Logger

	// QueueSize is the capacity of the reconcile work queue (default: 256).
	QueueSize int
}

// Manager manages controllers, watches, and the reconcile loop.
//
// Internal mechanics of Start():
//   - Deduplicates GVKs across all registered controllers' watch sources,
//     creates one ListerWatcher per unique GVK.
//   - For each watch source, starts a goroutine running:
//     list → enqueue all → watch → enqueue events → on channel close → relist.
//   - Primary watches use identity mapping (obj → Request{obj.Namespace, obj.Name});
//     cross-type watches call the user-provided MapFunc.
//   - A single reconcile goroutine reads from the shared work queue and
//     dispatches to the right controller's reconcile closure.
//   - Requeue on error with 1s backoff; honor Result.Requeue / Result.RequeueAfter.
//   - All goroutines coordinate via sync.WaitGroup, exit on context cancellation.
type Manager struct {
	cfg         ManagerConfig
	controllers []controllerRegistration
	logger      *slog.Logger
}

// controllerRegistration holds a type-erased controller and its watch sources.
type controllerRegistration struct {
	gvk       string
	reconcile func(ctx context.Context, namespace, name string) (Result, error)
	watches   []watchSource
}

// watchSource describes a GVK to watch and how to map its events to
// reconcile Requests.
type watchSource struct {
	gvk   string
	mapFn MapFunc // nil for the primary type (identity mapping)
}

// reconcileItem is an internal work queue entry pairing a Request with the
// reconcile function that should handle it.
type reconcileItem struct {
	request   Request
	reconcile func(ctx context.Context, namespace, name string) (Result, error)
}

// NewManager creates a Manager.
func NewManager(cfg ManagerConfig) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(nil, nil))
	}
	return &Manager{
		cfg:    cfg,
		logger: logger,
	}
}

// Start runs all registered controllers. It blocks until ctx is cancelled,
// then waits for all internal goroutines to finish.
//
// For each unique GVK across all controllers' watch sources, Start creates
// a ListerWatcher and launches a goroutine that runs a continuous
// list-watch-relist loop. Watch events are mapped to Requests and sent to
// a shared work queue. A single reconcile goroutine reads from the queue,
// calls the appropriate controller's Reconcile, and requeues on error.
func (m *Manager) Start(ctx context.Context) error {
	queueSize := m.cfg.QueueSize
	if queueSize == 0 {
		queueSize = 256
	}
	queue := make(chan reconcileItem, queueSize)

	// Deduplicate GVKs — only one ListerWatcher per GVK, even if multiple
	// controllers or watch sources reference it.
	gvkWatchers := map[string]*ListerWatcher{}
	for _, ctrl := range m.controllers {
		for _, ws := range ctrl.watches {
			if _, ok := gvkWatchers[ws.gvk]; !ok {
				gvkWatchers[ws.gvk] = NewListerWatcher(
					m.cfg.ConnFactory, ws.gvk, m.cfg.BucketIDs,
				)
			}
		}
	}

	var wg sync.WaitGroup

	// Start one watch goroutine per (controller, watch source) pair.
	// Each goroutine runs a list-watch-relist loop that maps events into
	// reconcile Requests and pushes them onto the shared queue.
	for _, ctrl := range m.controllers {
		for _, ws := range ctrl.watches {
			lw := gvkWatchers[ws.gvk]
			wg.Go(func() {
				m.watchLoop(ctx, lw, ws, ctrl, queue)
			})
		}
	}

	// Single reconcile goroutine — reads from the shared queue and
	// dispatches to the right controller's reconcile closure.
	wg.Go(func() {
		m.reconcileLoop(ctx, queue)
	})

	wg.Wait()
	return ctx.Err()
}

// watchLoop runs a continuous list → watch → relist cycle for a single
// watch source. On each event, it maps the object to Requests and enqueues
// them. If the watch channel closes (server-side timeout, connection loss),
// it relists and restarts the watch.
func (m *Manager) watchLoop(ctx context.Context, lw *ListerWatcher, ws watchSource,
	ctrl controllerRegistration, queue chan<- reconcileItem) {

	for ctx.Err() == nil {
		result, err := lw.List(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("list failed", "gvk", ws.gvk, "err", err)
			time.Sleep(time.Second)
			continue
		}

		// Enqueue all non-deleted objects from the list.
		for _, obj := range result.Objects {
			if obj.Deleted {
				continue
			}
			m.enqueueForWatch(ctx, ws, obj, ctrl, queue)
		}

		m.logger.Info("watching", "gvk", ws.gvk, "rv", result.ResourceVersion)
		wi, err := lw.Watch(ctx, result.ResourceVersion)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("watch failed", "gvk", ws.gvk, "err", err)
			continue
		}

		// Process events until the watch channel closes.
		for ev := range wi.ResultChan() {
			if ev.Object == nil {
				continue
			}
			if ev.Type == EventAdded || ev.Type == EventModified {
				m.enqueueForWatch(ctx, ws, ev.Object, ctrl, queue)
			}
		}
		m.logger.Info("watch closed, relisting", "gvk", ws.gvk)
	}
}

// enqueueForWatch maps an object to Requests and pushes them onto the queue.
// For primary watches (nil mapFn), it uses identity mapping. For cross-type
// watches, it calls the user-provided MapFunc.
func (m *Manager) enqueueForWatch(ctx context.Context, ws watchSource, obj *Object,
	ctrl controllerRegistration, queue chan<- reconcileItem) {

	if ws.mapFn == nil {
		// Primary watch: the object IS the thing to reconcile.
		select {
		case queue <- reconcileItem{
			request:   Request{Namespace: obj.Namespace, Name: obj.Name},
			reconcile: ctrl.reconcile,
		}:
		default:
			m.logger.Warn("queue full, dropping", "ns", obj.Namespace, "name", obj.Name)
		}
		return
	}

	// Cross-type watch: call MapFunc to determine which primary objects
	// need reconciliation.
	requests := ws.mapFn(ctx, obj)
	for _, req := range requests {
		select {
		case queue <- reconcileItem{request: req, reconcile: ctrl.reconcile}:
		default:
			m.logger.Warn("queue full, dropping", "ns", req.Namespace, "name", req.Name)
		}
	}
}

// reconcileLoop reads from the work queue and dispatches each item to the
// appropriate controller's Reconcile. On error, the item is requeued after
// a 1-second backoff. If Reconcile returns Result{Requeue: true}, the item
// is requeued after RequeueAfter (default 1s).
func (m *Manager) reconcileLoop(ctx context.Context, queue chan reconcileItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-queue:
			result, err := item.reconcile(ctx, item.request.Namespace, item.request.Name)
			if err != nil {
				m.logger.Error("reconcile failed",
					"ns", item.request.Namespace, "name", item.request.Name, "err", err)
				go m.requeueAfter(ctx, queue, item, time.Second)
				continue
			}
			if result.Requeue {
				delay := result.RequeueAfter
				if delay == 0 {
					delay = time.Second
				}
				go m.requeueAfter(ctx, queue, item, delay)
			}
		}
	}
}

// requeueAfter waits for the given delay then re-enqueues the item.
func (m *Manager) requeueAfter(ctx context.Context, queue chan<- reconcileItem, item reconcileItem, delay time.Duration) {
	select {
	case <-time.After(delay):
		select {
		case queue <- item:
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
}
