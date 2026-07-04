package crbridge

import (
	"github.com/jmelis/postgres-controller-backend/internal/reader"
)

// WatchInterface mirrors watch.Interface from k8s.io/apimachinery.
type WatchInterface interface {
	Stop()
	ResultChan() <-chan Event
}

// watchAdapter wraps reader.Watcher into a WatchInterface.
type watchAdapter struct {
	watcher *reader.Watcher
	events  chan Event
	stopCh  chan struct{}
}

func newWatchAdapter(w *reader.Watcher) *watchAdapter {
	a := &watchAdapter{
		watcher: w,
		events:  make(chan Event, 256),
		stopCh:  make(chan struct{}),
	}
	go a.translate()
	return a
}

func (a *watchAdapter) ResultChan() <-chan Event {
	return a.events
}

func (a *watchAdapter) Stop() {
	a.watcher.Stop()
}

func (a *watchAdapter) translate() {
	defer close(a.events)
	for ev := range a.watcher.Events() {
		var evType EventType
		switch ev.Type {
		case reader.EventAdded:
			evType = EventAdded
		case reader.EventModified:
			evType = EventModified
		case reader.EventDeleted:
			evType = EventDeleted
		case reader.EventBookmark:
			evType = EventBookmark
		default:
			continue
		}
		a.events <- Event{
			Type:   evType,
			Object: objectFromResource(ev.Resource),
		}
	}
}
