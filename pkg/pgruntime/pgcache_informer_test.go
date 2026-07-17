package pgruntime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// --- event recorder ---

type recordedEvent struct {
	Type          string // "add", "update", "delete"
	Obj           client.Object
	OldObj        client.Object
	IsInitialList bool
}

type eventRecorder struct {
	events chan recordedEvent
}

func newEventRecorder(size int) *eventRecorder {
	return &eventRecorder{events: make(chan recordedEvent, size)}
}

func (r *eventRecorder) OnAdd(obj any, isInInitialList bool) {
	r.events <- recordedEvent{
		Type:          "add",
		Obj:           obj.(client.Object),
		IsInitialList: isInInitialList,
	}
}

func (r *eventRecorder) OnUpdate(oldObj, newObj any) {
	r.events <- recordedEvent{
		Type:   "update",
		OldObj: oldObj.(client.Object),
		Obj:    newObj.(client.Object),
	}
}

func (r *eventRecorder) OnDelete(obj any) {
	o, ok := obj.(client.Object)
	if !ok {
		if d, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
			o = d.Obj.(client.Object)
		}
	}
	r.events <- recordedEvent{
		Type: "delete",
		Obj:  o,
	}
}

func (r *eventRecorder) waitFor(t *testing.T, n int, timeout time.Duration) []recordedEvent {
	t.Helper()
	var result []recordedEvent
	deadline := time.After(timeout)
	for len(result) < n {
		select {
		case ev := <-r.events:
			result = append(result, ev)
		case <-deadline:
			t.Fatalf("timed out waiting for %d events, got %d", n, len(result))
		}
	}
	return result
}

func (r *eventRecorder) waitForType(t *testing.T, typ string, timeout time.Duration) recordedEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-r.events:
			if ev.Type == typ {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q event", typ)
		}
	}
}

// --- helpers ---

func startCacheAndClient(t *testing.T) (cache.Cache, client.Client, context.Context, context.CancelFunc) {
	t.Helper()
	mgr := newManager(t)
	c := mgr.GetClient()
	ch := mgr.GetCache()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	require.Eventually(t, func() bool {
		return ch.WaitForCacheSync(ctx)
	}, 10*time.Second, 100*time.Millisecond, "cache never synced")

	t.Cleanup(func() { cancel() })
	return ch, c, ctx, cancel
}

func createWidget(t *testing.T, c client.Client, ctx context.Context, name, color string) {
	t.Helper()
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec:       WidgetSpec{Color: color},
	}
	require.NoError(t, c.Create(ctx, w))
}

// --- tests ---

func TestInformer_InitialListDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	mgr := newManager(t)
	c := mgr.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 3 {
		createWidget(t, c, ctx, fmt.Sprintf("pre-%d", i), "red")
	}

	rec := newEventRecorder(20)
	inf, err := mgr.GetCache().GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	events := rec.waitFor(t, 3, 10*time.Second)
	for _, ev := range events {
		assert.Equal(t, "add", ev.Type)
		assert.True(t, ev.IsInitialList, "initial list events should have isInitialList=true")
	}
}

func TestInformer_WatchAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	createWidget(t, c, ctx, "watch-add", "green")

	ev := rec.waitForType(t, "add", 10*time.Second)
	assert.Equal(t, "watch-add", ev.Obj.GetName())
	assert.False(t, ev.IsInitialList, "watch events should have isInitialList=false")
}

func TestInformer_WatchUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "watch-update"},
		Spec:       WidgetSpec{Color: "red"},
	}
	require.NoError(t, c.Create(ctx, w))
	rec.waitForType(t, "add", 10*time.Second)

	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(w), w))
	w.Spec.Color = "blue"
	require.NoError(t, c.Update(ctx, w))

	ev := rec.waitForType(t, "update", 10*time.Second)
	oldWidget := ev.OldObj.(*Widget)
	newWidget := ev.Obj.(*Widget)
	assert.Equal(t, "red", oldWidget.Spec.Color)
	assert.Equal(t, "blue", newWidget.Spec.Color)
}

func TestInformer_WatchDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "watch-delete"},
		Spec:       WidgetSpec{Color: "red"},
	}
	require.NoError(t, c.Create(ctx, w))
	rec.waitForType(t, "add", 10*time.Second)

	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(w), w))
	require.NoError(t, c.Delete(ctx, w))

	ev := rec.waitForType(t, "delete", 10*time.Second)
	assert.Equal(t, "watch-delete", ev.Obj.GetName())
}

func TestInformer_HasSynced(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	mgr := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inf, err := mgr.GetCache().GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	assert.False(t, inf.HasSynced(), "informer should not be synced before Start")

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	assert.Eventually(t, func() bool {
		return inf.HasSynced()
	}, 10*time.Second, 100*time.Millisecond, "informer should eventually sync")

	assert.True(t, mgr.GetCache().WaitForCacheSync(ctx))
}

func TestInformer_HandlerAfterSync(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	mgr := newManager(t)
	c := mgr.GetClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createWidget(t, c, ctx, "existing-1", "red")
	createWidget(t, c, ctx, "existing-2", "blue")

	// Register a dummy handler so the informer is created and watches Widgets
	inf, err := mgr.GetCache().GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	_, err = inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{})
	require.NoError(t, err)

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	require.Eventually(t, func() bool {
		return inf.HasSynced()
	}, 10*time.Second, 100*time.Millisecond)

	rec := newEventRecorder(20)
	reg, err := inf.AddEventHandler(rec)
	require.NoError(t, err)

	events := rec.waitFor(t, 2, 10*time.Second)
	for _, ev := range events {
		assert.Equal(t, "add", ev.Type)
		assert.True(t, ev.IsInitialList, "replay to late handler should have isInitialList=true")
	}

	assert.Eventually(t, func() bool { return reg.HasSynced() },
		5*time.Second, 50*time.Millisecond, "late handler registration should sync quickly")
}

func TestInformer_MultipleHandlers(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec1 := newEventRecorder(20)
	rec2 := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec1)
	require.NoError(t, err)
	_, err = inf.AddEventHandler(rec2)
	require.NoError(t, err)

	createWidget(t, c, ctx, "multi-handler", "purple")

	ev1 := rec1.waitForType(t, "add", 10*time.Second)
	ev2 := rec2.waitForType(t, "add", 10*time.Second)
	assert.Equal(t, "multi-handler", ev1.Obj.GetName())
	assert.Equal(t, "multi-handler", ev2.Obj.GetName())
}

func TestInformer_WatchEventResourceVersionFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	createWidget(t, c, ctx, "rv-format", "red")

	ev := rec.waitForType(t, "add", 10*time.Second)
	rv := ev.Obj.GetResourceVersion()

	// The Reflector stores each watch event's ResourceVersion as
	// lastSyncResourceVersion and passes it back to WatchFunc on reconnect.
	// If watch events carry a bare ObjectVersion (e.g. "3"), the next
	// WatchFunc call would fail to parse it as a composite RV, causing the
	// informer to error-loop on every watch reconnect.
	_, parseErr := resourceversion.Parse(rv)
	require.NoError(t, parseErr,
		"watch event ResourceVersion %q must be a valid composite RV; "+
			"a bare ObjectVersion would break Reflector reconnection", rv)
}

// WaitForCacheSync must honor its context: a cancelled context has to unblock
// the wait and return false, even if the cache was never started. Waiting on
// the Start stop-channel instead (nil before Start) hangs forever.
func TestCache_WaitForCacheSyncHonorsContext(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	mgr := newManager(t)
	_, err := mgr.GetCache().GetInformer(context.Background(), &Widget{})
	require.NoError(t, err)

	// The cache is never started, so the informer can never sync.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() { done <- mgr.GetCache().WaitForCacheSync(ctx) }()

	select {
	case synced := <-done:
		assert.False(t, synced, "WaitForCacheSync should report failure on cancelled context")
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCacheSync did not return after context cancellation")
	}
}

// Objects delivered by watch events carry a composite ResourceVersion (for
// Reflector reconnects). They must still be usable for writes: taking an
// object from an event handler (or the informer store) and passing it to
// client.Update/Delete is the standard controller pattern, and optimistic
// concurrency must keep working.
func TestInformer_EventObjectUsableForWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	createWidget(t, c, ctx, "event-write", "red")
	ev := rec.waitForType(t, "add", 10*time.Second)

	// Update using the watch-delivered object (composite RV).
	fromEvent := ev.Obj.DeepCopyObject().(*Widget)
	fromEvent.Spec.Color = "blue"
	require.NoError(t, c.Update(ctx, fromEvent),
		"object taken from a watch event must be usable for Update")

	// Optimistic concurrency must still hold: the event object is now stale.
	stale := ev.Obj.DeepCopyObject().(*Widget)
	stale.Spec.Color = "green"
	err = c.Update(ctx, stale)
	assert.True(t, apierrors.IsConflict(err),
		"stale event object should produce a Conflict, got: %v", err)

	// Delete using a watch-delivered object as well.
	upd := rec.waitForType(t, "update", 10*time.Second)
	fromUpdate := upd.Obj.DeepCopyObject().(*Widget)
	require.NoError(t, c.Delete(ctx, fromUpdate),
		"object taken from a watch event must be usable for Delete")
}

// Status().Update() on an object taken directly from a watch event.
func TestInformer_StatusUpdateFromEventObject(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	createWidget(t, c, ctx, "status-event", "red")
	ev := rec.waitForType(t, "add", 10*time.Second)

	fromEvent := ev.Obj.DeepCopyObject().(*Widget)
	fromEvent.Status.Phase = "Running"
	require.NoError(t, c.Status().Update(ctx, fromEvent),
		"Status().Update() must work with composite RV from watch event")

	upd := rec.waitForType(t, "update", 10*time.Second)
	assert.Equal(t, "Running", upd.Obj.(*Widget).Status.Phase)
	assert.Equal(t, "red", upd.Obj.(*Widget).Spec.Color, "spec must not be clobbered by status update")
}

// All watch event RVs are composite so the Reflector can reconnect with them.
func TestInformer_WatchReconnection(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(50)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	// Phase 1: create some objects and verify events.
	for i := range 3 {
		createWidget(t, c, ctx, fmt.Sprintf("reconnect-%d", i), "red")
	}
	phase1 := rec.waitFor(t, 3, 10*time.Second)
	for _, ev := range phase1 {
		rv := ev.Obj.GetResourceVersion()
		_, parseErr := resourceversion.Parse(rv)
		require.NoError(t, parseErr,
			"phase 1 event RV %q must be composite", rv)
	}

	// Phase 2: create more objects after the initial batch. If the Reflector
	// internally rotates the watch, it will use the last event's RV to
	// reconnect. The bug would cause a parse failure here.
	for i := range 3 {
		createWidget(t, c, ctx, fmt.Sprintf("reconnect-late-%d", i), "blue")
	}
	phase2 := rec.waitFor(t, 3, 15*time.Second)
	for _, ev := range phase2 {
		rv := ev.Obj.GetResourceVersion()
		_, parseErr := resourceversion.Parse(rv)
		require.NoError(t, parseErr,
			"phase 2 event RV %q must be composite", rv)
	}
}

// Delete then recreate with the same name produces a new UID.
func TestInformer_DeleteAndRecreate(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ch, c, ctx, _ := startCacheAndClient(t)

	inf, err := ch.GetInformer(ctx, &Widget{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return inf.HasSynced() },
		10*time.Second, 50*time.Millisecond, "widget informer never synced")

	rec := newEventRecorder(20)
	_, err = inf.AddEventHandler(rec)
	require.NoError(t, err)

	// Create
	createWidget(t, c, ctx, "phoenix", "red")
	addEv := rec.waitForType(t, "add", 10*time.Second)
	originalUID := addEv.Obj.GetUID()
	assert.NotEmpty(t, originalUID)

	// Delete
	w := &Widget{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "phoenix"}, w))
	require.NoError(t, c.Delete(ctx, w))
	rec.waitForType(t, "delete", 10*time.Second)

	// Recreate with same name
	createWidget(t, c, ctx, "phoenix", "blue")
	readdEv := rec.waitForType(t, "add", 10*time.Second)
	assert.NotEqual(t, originalUID, readdEv.Obj.GetUID(),
		"recreated object must have a different UID")
	assert.Equal(t, "blue", readdEv.Obj.(*Widget).Spec.Color)
}

// compile-time check
var _ toolscache.ResourceEventHandler = (*eventRecorder)(nil)
