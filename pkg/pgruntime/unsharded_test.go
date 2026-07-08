package pgruntime_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
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

// Config is a cluster-scoped test type used for unsharded GVK tests.
type Config struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ConfigSpec   `json:"spec,omitempty"`
	Status            ConfigStatus `json:"status,omitempty"`
}

type ConfigSpec struct {
	Region string `json:"region"`
}

type ConfigStatus struct {
	Ready bool `json:"ready,omitempty"`
}

type ConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Config `json:"items"`
}

func (in *Config) DeepCopyObject() runtime.Object {
	out := new(Config)
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	return out
}

func (in *ConfigList) DeepCopyObject() runtime.Object {
	out := new(ConfigList)
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Config, len(in.Items))
		for i := range in.Items {
			out.Items[i] = in.Items[i]
			in.Items[i].ObjectMeta.DeepCopyInto(&out.Items[i].ObjectMeta)
		}
	}
	return out
}

func (in *Config) GetObjectKind() schema.ObjectKind     { return &in.TypeMeta }
func (in *ConfigList) GetObjectKind() schema.ObjectKind { return &in.TypeMeta }

var (
	configGV      = schema.GroupVersion{Group: "infra.example.com", Version: "v1"}
	configGVK     = configGV.WithKind("Config")
	configBuilder = &runtimescheme.Builder{GroupVersion: configGV}
)

func init() {
	configBuilder.Register(&Config{}, &ConfigList{})
}

func unshardedScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := configBuilder.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := testBuilder.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

func newUnshardedManager(t *testing.T, bucketIDs []int) ctrl.Manager {
	t.Helper()
	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	mgr, err := pgruntime.NewManager(pgruntime.Options{
		Scheme:         unshardedScheme(),
		DSN:            sharedDB.ConnStr,
		BucketIDs:      bucketIDs,
		BucketAssigner: func(ns, _ string) int { return 2 },
		UnshardedGVKs:  []schema.GroupVersionKind{configGVK},
		Logger:         logr.Discard(),
	})
	require.NoError(t, err)
	return mgr
}

func TestUnsharded_CRUD(t *testing.T) {
	mgr := newUnshardedManager(t, []int{2, 3})
	c := mgr.GetClient()
	ctx := context.Background()
	key := types.NamespacedName{Name: "global-cfg"}

	cfg := &Config{
		ObjectMeta: metav1.ObjectMeta{Name: "global-cfg"},
		Spec:       ConfigSpec{Region: "us-east-1"},
	}
	require.NoError(t, c.Create(ctx, cfg))
	assert.NotEmpty(t, cfg.GetUID())
	assert.Equal(t, "1", cfg.GetResourceVersion())

	got := &Config{}
	require.NoError(t, c.Get(ctx, key, got))
	assert.Equal(t, "us-east-1", got.Spec.Region)

	got.Spec.Region = "eu-west-1"
	require.NoError(t, c.Update(ctx, got))
	assert.Equal(t, "2", got.GetResourceVersion())

	got.Status.Ready = true
	require.NoError(t, c.Status().Update(ctx, got))

	got2 := &Config{}
	require.NoError(t, c.Get(ctx, key, got2))
	assert.True(t, got2.Status.Ready)

	require.NoError(t, c.Delete(ctx, got2))

	err := c.Get(ctx, key, &Config{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestUnsharded_ListVisibility(t *testing.T) {
	mgr := newUnshardedManager(t, []int{2, 3})
	c := mgr.GetClient()
	ctx := context.Background()

	for _, name := range []string{"cfg-a", "cfg-b", "cfg-c"} {
		cfg := &Config{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       ConfigSpec{Region: "us-east-1"},
		}
		require.NoError(t, c.Create(ctx, cfg))
	}

	list := &ConfigList{}
	require.NoError(t, c.List(ctx, list))
	assert.Len(t, list.Items, 3, "all unsharded resources should be visible regardless of pod bucket slice")
}

func TestUnsharded_ShardedAndUnshardedCoexist(t *testing.T) {
	mgr := newUnshardedManager(t, []int{2, 3})
	c := mgr.GetClient()
	ctx := context.Background()

	cfg := &Config{
		ObjectMeta: metav1.ObjectMeta{Name: "global"},
		Spec:       ConfigSpec{Region: "us-east-1"},
	}
	require.NoError(t, c.Create(ctx, cfg))

	w := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "sharded-widget"},
		Spec:       WidgetSpec{Color: "blue"},
	}
	require.NoError(t, c.Create(ctx, w))

	configs := &ConfigList{}
	require.NoError(t, c.List(ctx, configs))
	assert.Len(t, configs.Items, 1)
	assert.Equal(t, "global", configs.Items[0].Name)

	widgets := &WidgetList{}
	require.NoError(t, c.List(ctx, widgets))
	assert.Len(t, widgets.Items, 1)
	assert.Equal(t, "sharded-widget", widgets.Items[0].Name)
}

func TestUnsharded_IsClusterScoped(t *testing.T) {
	mgr := newUnshardedManager(t, []int{0})
	c := mgr.GetClient()

	namespaced, err := c.IsObjectNamespaced(&Config{})
	require.NoError(t, err)
	assert.False(t, namespaced, "Config should be cluster-scoped (unsharded)")

	namespaced, err = c.IsObjectNamespaced(&Widget{})
	require.NoError(t, err)
	assert.True(t, namespaced, "Widget should be namespace-scoped (sharded)")
}

func TestUnsharded_WatchDelivery(t *testing.T) {
	mgr := newUnshardedManager(t, []int{2, 3})
	c := mgr.GetClient()

	reconciled := make(chan reconcile.Request, 10)

	err := ctrl.NewControllerManagedBy(mgr).
		For(&Config{}).
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

	time.Sleep(500 * time.Millisecond)

	cfg := &Config{
		ObjectMeta: metav1.ObjectMeta{Name: "watch-test"},
		Spec:       ConfigSpec{Region: "us-west-2"},
	}
	require.NoError(t, c.Create(ctx, cfg))

	select {
	case req := <-reconciled:
		assert.Equal(t, "", req.Namespace, "unsharded resource should have empty namespace")
		assert.Equal(t, "watch-test", req.Name)
	case <-ctx.Done():
		t.Fatal("timed out waiting for unsharded reconcile event")
	}
}

func TestUnsharded_ClientCRUD(t *testing.T) {
	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	c, cleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:         unshardedScheme(),
		DSN:            sharedDB.ConnStr,
		BucketIDs:      []int{5},
		BucketAssigner: func(ns, _ string) int { return 5 },
		UnshardedGVKs:  []schema.GroupVersionKind{configGVK},
	})
	require.NoError(t, err)
	defer cleanup()

	ctx := context.Background()

	cfg := &Config{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone"},
		Spec:       ConfigSpec{Region: "ap-southeast-1"},
	}
	require.NoError(t, c.Create(ctx, cfg))

	got := &Config{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "standalone"}, got))
	assert.Equal(t, "ap-southeast-1", got.Spec.Region)

	list := &ConfigList{}
	require.NoError(t, c.List(ctx, list))
	assert.Len(t, list.Items, 1)
}

func TestUnsharded_DifferentBucketSliceSeesUnsharded(t *testing.T) {
	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	s := unshardedScheme()

	writer, writerCleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:         s,
		DSN:            sharedDB.ConnStr,
		BucketIDs:      []int{0},
		BucketAssigner: func(ns, _ string) int { return 0 },
		UnshardedGVKs:  []schema.GroupVersionKind{configGVK},
	})
	require.NoError(t, err)
	defer writerCleanup()

	reader, readerCleanup, err := pgruntime.NewClient(pgruntime.Options{
		Scheme:         s,
		DSN:            sharedDB.ConnStr,
		BucketIDs:      []int{7, 8, 9},
		BucketAssigner: func(ns, _ string) int { return 7 },
		UnshardedGVKs:  []schema.GroupVersionKind{configGVK},
	})
	require.NoError(t, err)
	defer readerCleanup()

	ctx := context.Background()

	cfg := &Config{
		ObjectMeta: metav1.ObjectMeta{Name: "cross-pod"},
		Spec:       ConfigSpec{Region: "us-east-1"},
	}
	require.NoError(t, writer.Create(ctx, cfg))

	got := &Config{}
	require.NoError(t, reader.Get(ctx, types.NamespacedName{Name: "cross-pod"}, got))
	assert.Equal(t, "us-east-1", got.Spec.Region)

	list := &ConfigList{}
	require.NoError(t, reader.List(ctx, list))
	assert.Len(t, list.Items, 1, "reader on different buckets should see unsharded resources")

	shardedWidget := &Widget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "bucket0-widget"},
		Spec:       WidgetSpec{Color: "red"},
	}
	require.NoError(t, writer.Create(ctx, shardedWidget))

	widgets := &WidgetList{}
	require.NoError(t, reader.List(ctx, widgets))
	assert.Len(t, widgets.Items, 0, "reader on buckets [7,8,9] should NOT see widget written to bucket 0")
}

func TestUnsharded_ListWithLabelSelector(t *testing.T) {
	mgr := newUnshardedManager(t, []int{0})
	c := mgr.GetClient()
	ctx := context.Background()

	cfgA := &Config{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "labeled",
			Labels: map[string]string{"env": "prod"},
		},
		Spec: ConfigSpec{Region: "us-east-1"},
	}
	cfgB := &Config{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "unlabeled",
			Labels: map[string]string{"env": "dev"},
		},
		Spec: ConfigSpec{Region: "eu-west-1"},
	}
	require.NoError(t, c.Create(ctx, cfgA))
	require.NoError(t, c.Create(ctx, cfgB))

	list := &ConfigList{}
	require.NoError(t, c.List(ctx, list, client.MatchingLabels{"env": "prod"}))
	assert.Len(t, list.Items, 1)
	assert.Equal(t, "labeled", list.Items[0].Name)
}
