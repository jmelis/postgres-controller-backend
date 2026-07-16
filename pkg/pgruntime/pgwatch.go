package pgruntime

import (
	"context"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
)

// pgWatcher adapts a reader.Watcher to the k8s watch.Interface expected by the
// client-go Reflector. It converts reader.Event values into watch.Event values
// and forwards them on ResultChan.
type pgWatcher struct {
	result    chan watch.Event
	done      chan struct{}
	currentRV resourceversion.RV
}

func newPgWatcher(ctx context.Context, w *reader.Watcher, scheme *runtime.Scheme, startRV resourceversion.RV) *pgWatcher {
	buckets := make(map[int]int64, len(startRV.Buckets))
	for k, v := range startRV.Buckets {
		buckets[k] = v
	}
	pw := &pgWatcher{
		result:    make(chan watch.Event),
		done:      make(chan struct{}),
		currentRV: resourceversion.RV{Buckets: buckets},
	}
	go pw.relay(ctx, w, scheme)
	return pw
}

func (pw *pgWatcher) relay(ctx context.Context, w *reader.Watcher, scheme *runtime.Scheme) {
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
				return
			}
			obj, err := resourceToObject(ev.Resource, scheme)
			if err != nil {
				continue
			}
			// Advance the composite RV and stamp it on the object so the
			// Reflector's lastSyncResourceVersion stays in composite format.
			pw.currentRV.Buckets[ev.Resource.BucketID] = ev.Resource.GVKBucketSeq
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
