package parity_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	sharedDB   *testinfra.TestDB
	testEnv    *envtest.Environment
	testEnvCfg *rest.Config
	backends   []backendFactory

	testScheme = runtime.NewScheme()

	controllerSeq atomic.Int64
)

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = greeting.SchemeBuilder.AddToScheme(testScheme)
}

type Backend struct {
	Name      string
	Client    client.Client
	Namespace string
	stop      context.CancelFunc
}

type backendFactory struct {
	name string
	fn   func(t *testing.T) *Backend
}

func TestMain(m *testing.M) {
	sharedDB = testinfra.StartPostgresForTestMain()
	backends = []backendFactory{{name: "postgres", fn: newPostgresBackend}}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../examples/crd"},
		Scheme:                testScheme,
		ErrorIfCRDPathMissing: true,
	}
	if dir := findEnvtestBinDir("../../.envtest/k8s"); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	var err error
	testEnvCfg, err = testEnv.Start()
	if err != nil {
		fmt.Printf("WARN: envtest start failed (%v), running postgres-only\n", err)
	} else {
		backends = append(backends, backendFactory{name: "etcd", fn: newEtcdBackend})
	}

	code := m.Run()

	if testEnvCfg != nil {
		testEnv.Stop()
	}
	sharedDB.Stop()
	os.Exit(code)
}

func uniqueControllerName() string {
	return fmt.Sprintf("greeting-%d", controllerSeq.Add(1))
}

func newPostgresBackend(t *testing.T) *Backend {
	t.Helper()

	conn := sharedDB.Connect(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())

	mgr, err := pgruntime.NewManager(pgruntime.Options{
		Scheme:   greeting.Scheme,
		DSN:      sharedDB.ConnStr,
		HolderID: "parity-" + sanitizeName(t.Name()),
		Logger:   logr.Discard(),
	})
	require.NoError(t, err)

	reconciler := &greeting.GreetingReconciler{Client: mgr.GetClient()}
	require.NoError(t, reconciler.SetupWithManagerNamed(mgr, uniqueControllerName()))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mgr.Start(ctx)
	time.Sleep(500 * time.Millisecond)

	return &Backend{
		Name:      "postgres",
		Client:    mgr.GetClient(),
		Namespace: "default",
		stop:      cancel,
	}
}

func newEtcdBackend(t *testing.T) *Backend {
	t.Helper()

	ns := sanitizeName(t.Name())

	directClient, err := client.New(testEnvCfg, client.Options{Scheme: testScheme})
	require.NoError(t, err)
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	require.NoError(t, directClient.Create(context.Background(), nsObj))

	mgr, err := ctrl.NewManager(testEnvCfg, ctrl.Options{
		Scheme: testScheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{ns: {}},
		},
	})
	require.NoError(t, err)

	reconciler := &greeting.GreetingReconciler{Client: mgr.GetClient()}
	require.NoError(t, reconciler.SetupWithManagerNamed(mgr, uniqueControllerName()))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mgr.Start(ctx)

	require.Eventually(t, func() bool {
		return mgr.GetCache().WaitForCacheSync(ctx)
	}, 10*time.Second, 100*time.Millisecond)

	return &Backend{
		Name:      "etcd",
		Client:    mgr.GetClient(),
		Namespace: ns,
		stop:      cancel,
	}
}

func runOnBothBackends(t *testing.T, testFn func(t *testing.T, b *Backend)) {
	if testing.Short() {
		t.Skip("skipping parity test (requires postgres + envtest)")
	}
	for _, bf := range backends {
		bf := bf
		t.Run(bf.name, func(t *testing.T) {
			b := bf.fn(t)
			testFn(t, b)
		})
	}
}

func eventuallyGet(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		return c.Get(context.Background(), key, obj) == nil
	}, timeout, 100*time.Millisecond, "object %s never appeared", key)
}

func eventuallyCondition(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object, timeout time.Duration, desc string, condFn func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		if err := c.Get(context.Background(), key, obj); err != nil {
			return false
		}
		return condFn()
	}, timeout, 100*time.Millisecond, "condition %q never met for %s", desc, key)
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)

func sanitizeName(s string) string {
	s = strings.ToLower(s)
	s = nonAlphaNum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 60 {
		s = s[:60]
	}
	return s
}

func findEnvtestBinDir(basePath string) string {
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
