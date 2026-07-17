package pgruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// fakeWatchSource scripts a watchSource: it emits the given events, then
// returns err from Run (or blocks until Stop/ctx cancellation when err is
// nil). Like reader.Watcher, Run closes the events channel on return.
type fakeWatchSource struct {
	emit   []reader.Event
	err    error
	events chan reader.Event
	stopCh chan struct{}
	once   sync.Once
}

func newFakeWatchSource(emit []reader.Event, err error) *fakeWatchSource {
	return &fakeWatchSource{
		emit:   emit,
		err:    err,
		events: make(chan reader.Event),
		stopCh: make(chan struct{}),
	}
}

func (f *fakeWatchSource) Events() <-chan reader.Event { return f.events }

func (f *fakeWatchSource) Run(ctx context.Context) error {
	defer close(f.events)
	for _, ev := range f.emit {
		select {
		case f.events <- ev:
		case <-ctx.Done():
			return ctx.Err()
		case <-f.stopCh:
			return nil
		}
	}
	if f.err != nil {
		return f.err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.stopCh:
		return nil
	}
}

func (f *fakeWatchSource) Stop() { f.once.Do(func() { close(f.stopCh) }) }

func collectUntilClose(t *testing.T, pw *pgWatcher, timeout time.Duration) []watch.Event {
	t.Helper()
	var events []watch.Event
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-pw.ResultChan():
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timed out waiting for ResultChan to close; got %d events", len(events))
		}
	}
}

// The Reflector relies on a terminal watch.Error event carrying a 410
// (Expired/Gone) status to know it must relist instead of reconnecting with
// its stored resourceVersion. If the pgWatcher just closes the channel, a
// watch that failed with ErrGone (compaction advanced past the informer's RV)
// is retried with the same stale RV forever and the informer never recovers.
func TestPgWatcher_ErrGoneEmitsExpiredError(t *testing.T) {
	src := newFakeWatchSource(nil, fmt.Errorf("%w (hwm=1 < compacted=5)", reader.ErrGone))
	pw := newPgWatcher(context.Background(), src, runtime.NewScheme(),
		resourceversion.RV{Watermark: 1}, nil)

	events := collectUntilClose(t, pw, 5*time.Second)
	require.Len(t, events, 1, "expected a terminal watch.Error event before close")
	assert.Equal(t, watch.Error, events[0].Type)

	statusErr := apierrors.FromObject(events[0].Object)
	assert.True(t, apierrors.IsResourceExpired(statusErr) || apierrors.IsGone(statusErr),
		"error event must map to 410 Expired/Gone so the Reflector relists, got: %v", statusErr)
}

// Non-Gone poll failures (e.g. the database went away) must also surface as a
// watch.Error event so the Reflector logs/backs off deterministically instead
// of depending on how quickly the failed watch closed its channel.
func TestPgWatcher_PollErrorEmitsInternalError(t *testing.T) {
	src := newFakeWatchSource(nil, errors.New("poll begin tx: connection refused"))
	pw := newPgWatcher(context.Background(), src, runtime.NewScheme(),
		resourceversion.RV{Watermark: 1}, nil)

	events := collectUntilClose(t, pw, 5*time.Second)
	require.Len(t, events, 1, "expected a terminal watch.Error event before close")
	assert.Equal(t, watch.Error, events[0].Type)

	statusErr := apierrors.FromObject(events[0].Object)
	assert.True(t, apierrors.IsInternalError(statusErr),
		"non-Gone failures should map to an internal error status, got: %v", statusErr)
}

// A deliberate Stop (Reflector rotating the watch) is a clean shutdown: the
// channel closes without any error event, and cleanup runs.
func TestPgWatcher_CleanStopClosesWithoutError(t *testing.T) {
	cleanedUp := make(chan struct{})
	src := newFakeWatchSource(nil, nil)
	pw := newPgWatcher(context.Background(), src, runtime.NewScheme(),
		resourceversion.RV{Watermark: 1},
		func() { close(cleanedUp) })

	pw.Stop()
	events := collectUntilClose(t, pw, 5*time.Second)
	assert.Empty(t, events, "clean stop must not emit error events")

	select {
	case <-cleanedUp:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup was not invoked after Stop")
	}
}

// Context cancellation (informer shutting down) is likewise not an error.
func TestPgWatcher_ContextCancelClosesWithoutError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := newFakeWatchSource(nil, nil)
	pw := newPgWatcher(ctx, src, runtime.NewScheme(),
		resourceversion.RV{Watermark: 1}, nil)

	cancel()
	events := collectUntilClose(t, pw, 5*time.Second)
	assert.Empty(t, events, "context cancellation must not emit error events")
}

// An event that cannot be converted (GVK missing from the scheme) must not be
// silently dropped — a skipped event leaves the informer cache permanently
// stale for that object. Terminating the watch with an error event forces the
// Reflector to relist.
func TestPgWatcher_ConversionErrorTerminatesWatch(t *testing.T) {
	ev := reader.Event{
		Type: reader.EventAdded,
		Resource: model.Resource{
			GVK:           "test.example.com/v1/Unregistered",
			Namespace:     "default",
			Name:          "x",
			TxidStamp:     2,
			ObjectVersion: 1,
		},
	}
	src := newFakeWatchSource([]reader.Event{ev}, nil)
	pw := newPgWatcher(context.Background(), src, runtime.NewScheme(),
		resourceversion.RV{Watermark: 1}, nil)

	events := collectUntilClose(t, pw, 5*time.Second)
	require.Len(t, events, 1, "expected a terminal watch.Error event for the unconvertible object")
	assert.Equal(t, watch.Error, events[0].Type)
}
