package pgruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
)

// watchSource is the subset of *reader.Watcher used by pgWatcher. It exists so
// tests can substitute a fake source. Run must close the Events channel before
// returning (as reader.Watcher does).
type watchSource interface {
	Events() <-chan reader.Event
	Run(ctx context.Context) error
	Stop()
}

// pgWatcher adapts a reader.Watcher to the k8s watch.Interface expected by the
// client-go Reflector. It converts reader.Event values into watch.Event values
// and forwards them on ResultChan.
type pgWatcher struct {
	result    chan watch.Event
	done      chan struct{}
	runErr    chan error
	currentRV resourceversion.RV
}

// newPgWatcher starts the source's Run loop and relays its events. cleanup is
// invoked once Run returns (e.g. to close hijacked connections).
func newPgWatcher(ctx context.Context, w watchSource, scheme *runtime.Scheme, startRV resourceversion.RV, cleanup func()) *pgWatcher {
	buckets := make(map[int]int64, len(startRV.Buckets))
	for k, v := range startRV.Buckets {
		buckets[k] = v
	}
	pw := &pgWatcher{
		result:    make(chan watch.Event),
		done:      make(chan struct{}),
		runErr:    make(chan error, 1),
		currentRV: resourceversion.RV{Buckets: buckets},
	}
	go func() {
		pw.runErr <- w.Run(ctx)
		if cleanup != nil {
			cleanup()
		}
	}()
	go pw.relay(ctx, w, scheme)
	return pw
}

func (pw *pgWatcher) relay(ctx context.Context, w watchSource, scheme *runtime.Scheme) {
	defer close(pw.result)
	for {
		select {
		case <-pw.done:
			w.Stop()
			return
		case <-ctx.Done():
			w.Stop()
			return
		case ev, ok := <-w.Events():
			if !ok {
				// The watcher's Run loop has returned. If it failed, surface
				// the error to the Reflector as a terminal watch.Error event:
				// a silently closed channel makes the Reflector reconnect
				// with the same RV, which for a 410 (compaction passed our
				// RV) would never recover. An Expired status makes it relist.
				pw.sendError(ctx, <-pw.runErr)
				return
			}
			obj, err := resourceToObject(ev.Resource, scheme)
			if err != nil {
				// A skipped event would leave the informer cache permanently
				// stale for this object; kill the watch so the Reflector
				// relists instead.
				w.Stop()
				pw.sendError(ctx, fmt.Errorf("convert watch event for %s/%s: %w",
					ev.Resource.Namespace, ev.Resource.Name, err))
				return
			}
			// Advance the composite RV and stamp it on the object so the
			// Reflector's lastSyncResourceVersion stays in composite format.
			// The object's own version rides along as the "o<n>;" prefix so
			// write paths can still do optimistic concurrency.
			pw.currentRV.Buckets[ev.Resource.BucketID] = ev.Resource.GVKBucketSeq
			pw.currentRV.ObjectVersion = ev.Resource.ObjectVersion
			obj.SetResourceVersion(pw.currentRV.String())

			var eventType watch.EventType
			switch ev.Type {
			case reader.EventAdded:
				eventType = watch.Added
			case reader.EventModified:
				eventType = watch.Modified
			case reader.EventDeleted:
				if hasFinalizers(ev.Resource) {
					eventType = watch.Modified
				} else {
					eventType = watch.Deleted
				}
			}
			select {
			case pw.result <- watch.Event{Type: eventType, Object: obj}:
			case <-pw.done:
				w.Stop()
				return
			case <-ctx.Done():
				w.Stop()
				return
			}
		}
	}
}

// sendError converts a terminal watcher error into a watch.Error event.
// ErrGone maps to a 410 Expired status (Reflector relists); anything else maps
// to an internal error status. Clean shutdowns (nil or context errors) emit
// nothing.
func (pw *pgWatcher) sendError(ctx context.Context, err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	var status *metav1.Status
	if errors.Is(err, reader.ErrGone) {
		status = &apierrors.NewResourceExpired(err.Error()).ErrStatus
	} else {
		status = &apierrors.NewInternalError(err).ErrStatus
	}

	select {
	case pw.result <- watch.Event{Type: watch.Error, Object: status}:
	case <-pw.done:
	case <-ctx.Done():
	}
}

func (pw *pgWatcher) Stop() {
	select {
	case <-pw.done:
	default:
		close(pw.done)
	}
}

func (pw *pgWatcher) ResultChan() <-chan watch.Event {
	return pw.result
}

// listWatchWithoutWatchListSemantics opts out of the WatchList protocol
// (client-go v0.35.1+ enables it by default). Our pgWatcher does not send
// bookmark events, so the Reflector must use legacy List+Watch.
type listWatchWithoutWatchListSemantics struct {
	*toolscache.ListWatch
}

func (listWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool { return true }

var _ watch.Interface = (*pgWatcher)(nil)
