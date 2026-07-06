package pgruntime_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	runtimescheme "sigs.k8s.io/controller-runtime/pkg/scheme"
)

// --- Test types ---

var (
	testGV      = schema.GroupVersion{Group: "test.example.com", Version: "v1"}
	testBuilder = &runtimescheme.Builder{GroupVersion: testGV}
	testScheme  = runtime.NewScheme()
)

func init() {
	testBuilder.Register(&Widget{}, &WidgetList{})
	if err := testBuilder.AddToScheme(testScheme); err != nil {
		panic(err)
	}
}

type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec   `json:"spec,omitempty"`
	Status            WidgetStatus `json:"status,omitempty"`
}

type WidgetSpec struct {
	Color string `json:"color"`
}

type WidgetStatus struct {
	Phase string `json:"phase,omitempty"`
}

type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func (in *Widget) DeepCopyObject() runtime.Object {
	out := new(Widget)
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	return out
}

func (in *WidgetList) DeepCopyObject() runtime.Object {
	out := new(WidgetList)
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Widget, len(in.Items))
		for i := range in.Items {
			out.Items[i] = in.Items[i]
			in.Items[i].ObjectMeta.DeepCopyInto(&out.Items[i].ObjectMeta)
		}
	}
	return out
}

func (in *Widget) GetObjectKind() schema.ObjectKind     { return &in.TypeMeta }
func (in *WidgetList) GetObjectKind() schema.ObjectKind { return &in.TypeMeta }

// --- Test infrastructure ---

var sharedDB *testinfra.TestDB

func TestMain(m *testing.M) {
	sharedDB = testinfra.StartPostgresForTestMain()
	code := m.Run()
	sharedDB.Stop()
	os.Exit(code)
}

func newManager(t *testing.T) ctrl.Manager {
	t.Helper()
	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	mgr, err := pgruntime.NewManager(pgruntime.Options{
		Scheme:   testScheme,
		DSN:      sharedDB.ConnStr,
		HolderID: "test-" + t.Name(),
		Logger:   logr.Discard(),
	})
	require.NoError(t, err)
	return mgr
}

// --- Tests ---

func TestClient_CRUD(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "blue-widget"}

	// Create
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "blue-widget"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))
	assert.NotEmpty(t, w.GetUID())
	assert.Equal(t, "1", w.GetResourceVersion())

	// Get
	got := &Widget{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, "blue", got.Spec.Color)
	assert.Equal(t, "1", got.GetResourceVersion())

	// Update
	got.Spec.Color = "red"
	require.NoError(t, c.Update(ctx, got))
	assert.Equal(t, "2", got.GetResourceVersion())

	got2 := &Widget{}
	require.NoError(t, c.Get(ctx, key, got2))
	assert.Equal(t, "red", got2.Spec.Color)

	// Delete
	require.NoError(t, c.Delete(ctx, got2))

	err := c.Get(ctx, key, &Widget{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClient_NotFound(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "nope"}, &Widget{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestClient_AlreadyExists(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dup"},
		Spec:       WidgetSpec{Color: "green"},
	}
	require.NoError(t, c.Create(ctx, w))

	w2 := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dup"},
		Spec:       WidgetSpec{Color: "different"},
	}
	err := c.Create(ctx, w2)
	assert.True(t, apierrors.IsAlreadyExists(err), "expected AlreadyExists, got: %v", err)
}

func TestClient_Conflict(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "stale"},
		Spec:       WidgetSpec{Color: "v1"},
	}
	require.NoError(t, c.Create(ctx, w))

	// Get a copy, update the original to bump the version
	stale := &Widget{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "stale"}, stale))
	stale2 := stale.DeepCopyObject().(*Widget)

	stale.Spec.Color = "v2"
	require.NoError(t, c.Update(ctx, stale))

	// Now try to update with the stale copy
	stale2.Spec.Color = "v3"
	err := c.Update(ctx, stale2)
	assert.True(t, apierrors.IsConflict(err), "expected Conflict, got: %v", err)
}

func TestClient_StatusUpdate(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "with-status"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	got := &Widget{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "with-status"}, got))

	got.Status.Phase = "Ready"
	require.NoError(t, c.Status().Update(ctx, got))
	assert.Equal(t, "2", got.GetResourceVersion())

	got2 := &Widget{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "with-status"}, got2))
	assert.Equal(t, "Ready", got2.Status.Phase)
	assert.Equal(t, "blue", got2.Spec.Color)
}

func TestClient_List(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		w := &Widget{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec:       WidgetSpec{Color: name},
		}
		require.NoError(t, c.Create(ctx, w))
	}

	// Also create one in a different namespace
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "d"},
		Spec:       WidgetSpec{Color: "d"},
	}
	require.NoError(t, c.Create(ctx, w))

	// List all
	list := &WidgetList{}
	require.NoError(t, c.List(ctx, list))
	assert.Len(t, list.Items, 4)

	// List with namespace filter
	list2 := &WidgetList{}
	require.NoError(t, c.List(ctx, list2, client.InNamespace("default")))
	assert.Len(t, list2.Items, 3)
}

func TestClient_MetadataRoundTrip(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "labeled",
			Labels:      map[string]string{"app": "test", "env": "dev"},
			Annotations: map[string]string{"note": "hello"},
			Finalizers:  []string{"test.example.com/cleanup"},
		},
		Spec: WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	got := &Widget{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "labeled"}, got))
	assert.Equal(t, map[string]string{"app": "test", "env": "dev"}, got.GetLabels())
	assert.Equal(t, map[string]string{"note": "hello"}, got.GetAnnotations())
	assert.Equal(t, []string{"test.example.com/cleanup"}, got.GetFinalizers())
}

func TestClient_FinalizerLifecycle(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "with-finalizer"}

	// Create with finalizer
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "with-finalizer",
			Finalizers: []string{"test.example.com/cleanup"},
		},
		Spec: WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	// Delete sets deletionTimestamp but object stays visible (has finalizer)
	require.NoError(t, c.Delete(ctx, w))

	got := &Widget{}
	require.NoError(t, c.Get(ctx, key, got), "object with finalizer should still be visible after Delete")
	assert.NotNil(t, got.GetDeletionTimestamp(), "deletionTimestamp should be set")
	assert.Equal(t, []string{"test.example.com/cleanup"}, got.GetFinalizers())

	// Remove finalizer via Update
	got.SetFinalizers(nil)
	require.NoError(t, c.Update(ctx, got))

	// Now Get should return NotFound (fully deleted)
	err := c.Get(ctx, key, &Widget{})
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound after finalizer removal, got: %v", err)
}

func TestClient_FinalizerNotInList(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	// Create a live resource
	live := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "live"},
		Spec:       WidgetSpec{Color: "green"},
	}
	require.NoError(t, c.Create(ctx, live))

	// Create a resource with finalizer, then delete it
	dying := &Widget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "dying",
			Finalizers: []string{"test.example.com/cleanup"},
		},
		Spec: WidgetSpec{Color: "red"},
	}
	require.NoError(t, c.Create(ctx, dying))
	require.NoError(t, c.Delete(ctx, dying))

	// Create and fully delete a resource (no finalizer)
	gone := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gone"},
		Spec:       WidgetSpec{Color: "gray"},
	}
	require.NoError(t, c.Create(ctx, gone))
	require.NoError(t, c.Delete(ctx, gone))

	// List should include live + dying (has finalizer), but not gone (fully deleted)
	list := &WidgetList{}
	require.NoError(t, c.List(ctx, list))
	assert.Len(t, list.Items, 2, "should include live and dying-with-finalizer")

	names := map[string]bool{}
	for _, item := range list.Items {
		names[item.Name] = true
	}
	assert.True(t, names["live"])
	assert.True(t, names["dying"])
}

func TestClient_SubresourceIsolation(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "isolated"}

	// Create
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "isolated"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	// Set status
	got := &Widget{}
	require.NoError(t, c.Get(ctx, key, got))
	got.Status.Phase = "Ready"
	require.NoError(t, c.Status().Update(ctx, got))

	// Update spec — should NOT clobber status
	got2 := &Widget{}
	require.NoError(t, c.Get(ctx, key, got2))
	assert.Equal(t, "Ready", got2.Status.Phase, "status should be preserved before spec update")

	got2.Spec.Color = "red"
	require.NoError(t, c.Update(ctx, got2))

	// Verify status is preserved
	got3 := &Widget{}
	require.NoError(t, c.Get(ctx, key, got3))
	assert.Equal(t, "red", got3.Spec.Color)
	assert.Equal(t, "Ready", got3.Status.Phase, "status should be preserved after spec update")
}

func TestClient_Generation(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "gen-test"}

	// Create — generation should be 1
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gen-test"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))
	assert.Equal(t, int64(1), w.GetGeneration())

	got := &Widget{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, int64(1), got.GetGeneration())

	// Update spec — generation should increment
	got.Spec.Color = "red"
	require.NoError(t, c.Update(ctx, got))

	got2 := &Widget{}
	require.NoError(t, c.Get(ctx, key, got2))
	assert.Equal(t, int64(2), got2.GetGeneration())

	// Update with same spec (only labels change) — generation should NOT increment
	got2.SetLabels(map[string]string{"env": "test"})
	require.NoError(t, c.Update(ctx, got2))

	got3 := &Widget{}
	require.NoError(t, c.Get(ctx, key, got3))
	assert.Equal(t, int64(2), got3.GetGeneration(), "generation should not increment for metadata-only changes")
	assert.Equal(t, map[string]string{"env": "test"}, got3.GetLabels())
}

func TestClient_DeleteWithoutRV(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "default", Name: "no-rv"}

	// Create
	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "no-rv"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	// Delete using a bare object (no ResourceVersion)
	bare := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "no-rv"},
	}
	require.NoError(t, c.Delete(ctx, bare))

	// Should be gone
	err := c.Get(ctx, key, &Widget{})
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound after delete without RV, got: %v", err)
}

func TestClient_DeleteWithoutRV_NotFound(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()
	ctx := context.Background()

	bare := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "nonexistent"},
	}
	err := c.Delete(ctx, bare)
	assert.True(t, apierrors.IsNotFound(err), "delete of nonexistent object should return NotFound, got: %v", err)
}

func TestManager_FullReconcileLoop(t *testing.T) {
	mgr := newManager(t)
	c := mgr.GetClient()

	reconciled := make(chan reconcile.Request, 10)

	err := ctrl.NewControllerManagedBy(mgr).
		For(&Widget{}).
		Complete(reconcile.Func(func(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
			reconciled <- req
			return ctrl.Result{}, nil
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	// Give the manager a moment to start the cache
	time.Sleep(500 * time.Millisecond)

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "trigger"},
		Spec:       WidgetSpec{Color: "green"},
	}
	require.NoError(t, c.Create(ctx, w))

	select {
	case req := <-reconciled:
		assert.Equal(t, "default", req.Namespace)
		assert.Equal(t, "trigger", req.Name)
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconcile")
	}
}
