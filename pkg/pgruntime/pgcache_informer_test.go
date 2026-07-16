package pgruntime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// compile-time check
var _ toolscache.ResourceEventHandler = (*eventRecorder)(nil)
