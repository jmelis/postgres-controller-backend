package pgruntime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"
	pgschema "github.com/jmelis/postgres-controller-backend/internal/schema"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
)

// UnshardedBucket is the reserved bucket ID for GVKs that opt out of bucket
// sharding. Every pod watches this bucket regardless of its BucketIDs slice.
const UnshardedBucket = -1

// Options configures a postgres-backed controller-runtime Manager.
type Options struct {
	Scheme                 *runtime.Scheme
	DSN                    string
	BucketIDs              []int
	BucketAssigner         func(ns, name string) int
	UnshardedGVKs          []schema.GroupVersionKind
	Logger                 logr.Logger
	MaxPoolConns           int32
	MinPoolConns           int32
	HealthProbeBindAddress string
}

// NewManager creates a controller-runtime Manager backed by PostgreSQL.
// It connects to the database and runs schema migrations.
func NewManager(opts Options) (manager.Manager, error) {
	if opts.Scheme == nil {
		return nil, fmt.Errorf("pgruntime: Scheme is required")
	}
	if opts.DSN == "" {
		return nil, fmt.Errorf("pgruntime: DSN is required")
	}
	if len(opts.BucketIDs) == 0 {
		opts.BucketIDs = []int{0}
	}
	for _, id := range opts.BucketIDs {
		if id == UnshardedBucket {
			return nil, fmt.Errorf("pgruntime: BucketIDs must not contain %d (reserved for unsharded GVKs)", UnshardedBucket)
		}
	}
	if opts.BucketAssigner == nil {
		opts.BucketAssigner = func(_, _ string) int { return opts.BucketIDs[0] }
	}
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	ctx := context.Background()

	pool, err := createPool(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("pgruntime: create connection pool: %w", err)
	}

	migrationConn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgruntime: acquire conn for migration: %w", err)
	}
	if err := pgschema.Migrate(ctx, migrationConn.Conn()); err != nil {
		migrationConn.Release()
		pool.Close()
		return nil, fmt.Errorf("pgruntime: schema migration: %w", err)
	}
	migrationConn.Release()

	unshardedMap := buildUnshardedMap(opts.UnshardedGVKs)

	restMapper := buildRESTMapper(opts.Scheme)

	pgclient := &pgClient{
		scheme:     opts.Scheme,
		pool:       pool,
		assign:     opts.BucketAssigner,
		bucketIDs:  opts.BucketIDs,
		unsharded:  unshardedMap,
		restMapper: restMapper,
	}

	pgcache := &pgCache{
		scheme:     opts.Scheme,
		pool:       pool,
		bucketIDs:  opts.BucketIDs,
		unsharded:  unshardedMap,
		restMapper: restMapper,
		logger:     opts.Logger.WithName("cache"),
		informers:  make(map[schema.GroupVersionKind]*pgInformer),
	}

	elected := make(chan struct{})
	close(elected)

	return &pgManager{
		scheme:     opts.Scheme,
		client:     pgclient,
		cache:      pgcache,
		restMapper: restMapper,
		logger:     opts.Logger,
		opts:       opts,

		pool:    pool,
		elected: elected,

		healthzChecks: make(map[string]healthz.Checker),
		readyzChecks:  make(map[string]healthz.Checker),
	}, nil
}

type pgManager struct {
	scheme     *runtime.Scheme
	client     *pgClient
	cache      *pgCache
	restMapper meta.RESTMapper
	logger     logr.Logger
	opts       Options

	pool    *pgxpool.Pool
	elected chan struct{}

	mu        sync.Mutex
	runnables []manager.Runnable

	healthzChecks map[string]healthz.Checker
	readyzChecks  map[string]healthz.Checker
}

// --- cluster.Cluster ---

func (m *pgManager) GetHTTPClient() *http.Client          { return nil }
func (m *pgManager) GetConfig() *rest.Config               { return nil }
func (m *pgManager) GetCache() cache.Cache                 { return m.cache }
func (m *pgManager) GetScheme() *runtime.Scheme            { return m.scheme }
func (m *pgManager) GetClient() client.Client              { return m.client }
func (m *pgManager) GetFieldIndexer() client.FieldIndexer  { return m.cache }
func (m *pgManager) GetRESTMapper() meta.RESTMapper        { return m.restMapper }
func (m *pgManager) GetAPIReader() client.Reader            { return m.client }
func (m *pgManager) GetEventRecorderFor(name string) record.EventRecorder {
	return &noopEventRecorder{}
}

// GetEventRecorder is a no-op; postgres backend has no event infrastructure.
func (m *pgManager) GetEventRecorder(name string) events.EventRecorder {
	return &noopEventsRecorder{}
}

// --- manager.Manager ---

func (m *pgManager) Add(runnable manager.Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runnables = append(m.runnables, runnable)
	return nil
}

func (m *pgManager) Elected() <-chan struct{} { return m.elected }

func (m *pgManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	return nil
}

func (m *pgManager) AddHealthzCheck(name string, check healthz.Checker) error {
	m.healthzChecks[name] = check
	return nil
}

func (m *pgManager) AddReadyzCheck(name string, check healthz.Checker) error {
	m.readyzChecks[name] = check
	return nil
}

func (m *pgManager) Start(ctx context.Context) error {
	m.logger.Info("starting pgruntime manager")

	var wg sync.WaitGroup

	if addr := m.opts.HealthProbeBindAddress; addr != "" {
		srv := m.buildHealthProbeServer(addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.logger.Info("starting health probe server", "addr", addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				m.logger.Error(err, "health probe server failed")
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.cache.Start(ctx); err != nil {
			m.logger.Error(err, "cache start failed")
		}
	}()

	if !m.cache.WaitForCacheSync(ctx) {
		return fmt.Errorf("pgruntime: cache sync failed")
	}
	m.logger.Info("cache synced")

	m.mu.Lock()
	runnables := make([]manager.Runnable, len(m.runnables))
	copy(runnables, m.runnables)
	m.mu.Unlock()

	for _, r := range runnables {
		wg.Add(1)
		go func(r manager.Runnable) {
			defer wg.Done()
			if err := r.Start(ctx); err != nil {
				m.logger.Error(err, "runnable exited with error")
			}
		}(r)
	}

	<-ctx.Done()
	m.logger.Info("shutting down pgruntime manager")
	wg.Wait()
	m.pool.Close()
	return nil
}

func (m *pgManager) buildHealthProbeServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.runChecks(w, r, m.healthzChecks)
	}))
	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.runChecks(w, r, m.readyzChecks)
	}))
	return &http.Server{Addr: addr, Handler: mux}
}

func (m *pgManager) runChecks(w http.ResponseWriter, r *http.Request, checks map[string]healthz.Checker) {
	for name, check := range checks {
		if err := check(r); err != nil {
			http.Error(w, fmt.Sprintf("check %q failed: %v", name, err), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

func (m *pgManager) GetWebhookServer() webhook.Server {
	panic("pgruntime: webhooks not supported")
}

func (m *pgManager) GetLogger() logr.Logger { return m.logger }

func (m *pgManager) GetControllerOptions() config.Controller {
	return config.Controller{}
}

// GetConverterRegistry is a stub; CRD conversion webhooks are not supported.
func (m *pgManager) GetConverterRegistry() conversion.Registry {
	return conversion.NewRegistry()
}

func buildUnshardedMap(gvks []schema.GroupVersionKind) map[schema.GroupVersionKind]bool {
	m := make(map[schema.GroupVersionKind]bool, len(gvks))
	for _, gvk := range gvks {
		m[gvk] = true
	}
	return m
}

func buildRESTMapper(s *runtime.Scheme) meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper(s.PrioritizedVersionsAllGroups())
	for gvk := range s.AllKnownTypes() {
		if strings.HasSuffix(gvk.Kind, "List") || gvk.Kind == "" {
			continue
		}
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	return mapper
}

type noopEventRecorder struct{}

func (r *noopEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {}
func (r *noopEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (r *noopEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
}

type noopEventsRecorder struct{}

func (r *noopEventsRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
}

func createPool(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	if opts.MaxPoolConns > 0 || opts.MinPoolConns > 0 {
		config, err := pgxpool.ParseConfig(opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("parse DSN: %w", err)
		}
		if opts.MaxPoolConns > 0 {
			config.MaxConns = opts.MaxPoolConns
		}
		if opts.MinPoolConns > 0 {
			config.MinConns = opts.MinPoolConns
		}
		return pgxpool.NewWithConfig(ctx, config)
	}
	return pgxpool.New(ctx, opts.DSN)
}

// NewClient creates a standalone client.Client backed by PostgreSQL, without
// the manager/cache/watch infrastructure. Intended for stateless HTTP services
// that need CRUD access to the same kubernetes_resources table.
// The returned function closes the connection pool and must be called on shutdown.
func NewClient(opts Options) (client.Client, func(), error) {
	if opts.Scheme == nil {
		return nil, nil, fmt.Errorf("pgruntime: Scheme is required")
	}
	if opts.DSN == "" {
		return nil, nil, fmt.Errorf("pgruntime: DSN is required")
	}
	if len(opts.BucketIDs) == 0 {
		opts.BucketIDs = []int{0}
	}
	for _, id := range opts.BucketIDs {
		if id == UnshardedBucket {
			return nil, nil, fmt.Errorf("pgruntime: BucketIDs must not contain %d (reserved for unsharded GVKs)", UnshardedBucket)
		}
	}
	if opts.BucketAssigner == nil {
		opts.BucketAssigner = func(_, _ string) int { return opts.BucketIDs[0] }
	}

	ctx := context.Background()

	pool, err := createPool(ctx, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("pgruntime: create connection pool: %w", err)
	}

	migrationConn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("pgruntime: acquire conn for migration: %w", err)
	}
	if err := pgschema.Migrate(ctx, migrationConn.Conn()); err != nil {
		migrationConn.Release()
		pool.Close()
		return nil, nil, fmt.Errorf("pgruntime: schema migration: %w", err)
	}
	migrationConn.Release()

	unshardedMap := buildUnshardedMap(opts.UnshardedGVKs)

	c := &pgClient{
		scheme:     opts.Scheme,
		pool:       pool,
		assign:     opts.BucketAssigner,
		bucketIDs:  opts.BucketIDs,
		unsharded:  unshardedMap,
		restMapper: buildRESTMapper(opts.Scheme),
	}

	return c, pool.Close, nil
}

var _ manager.Manager = (*pgManager)(nil)
