package pgruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pgCache implements cache.Cache backed by postgres list/watch.
type pgCache struct {
	scheme     *runtime.Scheme
	pool       *pgxpool.Pool
	bucketIDs  []int
	unsharded  map[schema.GroupVersionKind]bool
	restMapper meta.RESTMapper
	logger     logr.Logger

	mu        sync.Mutex
	informers map[schema.GroupVersionKind]*pgInformer
	started   bool
	ctx       context.Context
}

// --- cache.Reader (Get/List delegate directly to postgres) ---

func (c *pgCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	gvkStr := gvkToString(gvk)

	poolConn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer poolConn.Release()
	conn := poolConn.Conn()

	var r model.Resource
	err = conn.QueryRow(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND namespace = $2 AND name = $3`,
		gvkStr, key.Namespace, key.Name,
	).Scan(
		&r.GVK, &r.Namespace, &r.Name, &r.UID, &r.BucketID,
		&r.GVKBucketSeq, &r.ObjectVersion, &r.Spec, &r.Status,
		&r.Metadata, &r.DeletionTimestamp, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return mapGetError(err, gvk, key.Name)
	}

	if isFullyDeleted(r) {
		return mapGetError(pgx.ErrNoRows, gvk, key.Name)
	}

	return populateObject(obj, r, gvk)
}

func (c *pgCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}

	listGVK, err := resolveGVK(c.scheme, list)
	if err != nil {
		return err
	}
	itemGVK := itemGVKFromListGVK(listGVK)
	gvkStr := gvkToString(itemGVK)

	poolConn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer poolConn.Release()

	buckets := c.bucketIDs
	if c.unsharded[itemGVK] {
		buckets = []int{UnshardedBucket}
	}

	filter, err := buildListFilter(listOpts)
	if err != nil {
		return err
	}

	result, err := reader.List(ctx, poolConn.Conn(), gvkStr, buckets, filter)
	if err != nil {
		return err
	}

	var items []client.Object
	for _, r := range result.Resources {
		obj, err := resourceToObject(r, c.scheme)
		if err != nil {
			return err
		}
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labelSet(obj.GetLabels())) {
			continue
		}
		items = append(items, obj)
	}

	if err := setListItems(list, items); err != nil {
		return err
	}
	list.SetResourceVersion(result.ResourceVersion.String())
	if listOpts.Limit > 0 && int64(len(result.Resources)) == listOpts.Limit {
		offset, _ := decodeContinue(listOpts.Continue)
		list.SetContinue(encodeContinue(offset + listOpts.Limit))
	}
	return nil
}

// --- cache.Informers ---

func (c *pgCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return nil, err
	}
	return c.getOrCreateInformer(gvk)
}

func (c *pgCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	return c.getOrCreateInformer(gvk)
}

func (c *pgCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	return nil
}

func (c *pgCache) Start(ctx context.Context) error {
	c.mu.Lock()
	c.started = true
	c.ctx = ctx
	informers := make([]*pgInformer, 0, len(c.informers))
	for _, inf := range c.informers {
		informers = append(informers, inf)
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, inf := range informers {
		wg.Add(1)
		go func(inf *pgInformer) {
			defer wg.Done()
			inf.run(ctx)
		}(inf)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (c *pgCache) WaitForCacheSync(ctx context.Context) bool {
	c.mu.Lock()
	informers := make([]*pgInformer, 0, len(c.informers))
	for _, inf := range c.informers {
		informers = append(informers, inf)
	}
	c.mu.Unlock()

	for {
		allSynced := true
		for _, inf := range informers {
			if !inf.HasSynced() {
				allSynced = false
				break
			}
		}
		if allSynced {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *pgCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

func (c *pgCache) getOrCreateInformer(gvk schema.GroupVersionKind) (*pgInformer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if inf, ok := c.informers[gvk]; ok {
		return inf, nil
	}

	buckets := c.bucketIDs
	if c.unsharded[gvk] {
		buckets = []int{UnshardedBucket}
	}

	inf := &pgInformer{
		gvk:         gvk,
		gvkStr:      gvkToString(gvk),
		scheme:      c.scheme,
		pool:      c.pool,
		bucketIDs: buckets,
		logger:      c.logger.WithValues("gvk", gvk.String()),
		store:       make(map[types.NamespacedName]storedObject),
	}
	c.informers[gvk] = inf

	if c.started && c.ctx != nil {
		go inf.run(c.ctx)
	}

	return inf, nil
}

// --- pgInformer implements cache.Informer ---

type storedObject struct {
	resourceVersion string
	obj             client.Object
}

type pgInformer struct {
	gvk       schema.GroupVersionKind
	gvkStr    string
	scheme    *runtime.Scheme
	pool      *pgxpool.Pool
	bucketIDs []int
	logger    logr.Logger

	handlerMu sync.RWMutex
	handlers  []handlerEntry
	synced    atomic.Bool
	stopped   atomic.Bool

	storeMu sync.RWMutex
	store   map[types.NamespacedName]storedObject
}

type handlerEntry struct {
	handler toolscache.ResourceEventHandler
	reg     *pgHandlerRegistration
}

type pgHandlerRegistration struct {
	synced atomic.Bool
}

func (r *pgHandlerRegistration) HasSynced() bool { return r.synced.Load() }
func (r *pgHandlerRegistration) HasSyncedChecker() toolscache.DoneChecker {
	return syncedDoneChecker("pgHandlerRegistration", &r.synced)
}

func (inf *pgInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return inf.AddEventHandlerWithResyncPeriod(handler, 0)
}

func (inf *pgInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, _ time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	reg := &pgHandlerRegistration{}

	inf.handlerMu.Lock()
	inf.handlers = append(inf.handlers, handlerEntry{handler: handler, reg: reg})
	alreadySynced := inf.synced.Load()
	inf.handlerMu.Unlock()

	if alreadySynced {
		reg.synced.Store(true)
		inf.storeMu.RLock()
		for _, so := range inf.store {
			handler.OnAdd(so.obj, true)
		}
		inf.storeMu.RUnlock()
	}

	return reg, nil
}

// AddEventHandlerWithOptions delegates to AddEventHandlerWithResyncPeriod; options are ignored.
func (inf *pgInformer) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, opts toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	return inf.AddEventHandlerWithResyncPeriod(handler, 0)
}

func (inf *pgInformer) RemoveEventHandler(handle toolscache.ResourceEventHandlerRegistration) error {
	return nil
}

func (inf *pgInformer) AddIndexers(indexers toolscache.Indexers) error {
	return nil
}

func (inf *pgInformer) HasSynced() bool {
	return inf.synced.Load()
}

func (inf *pgInformer) HasSyncedChecker() toolscache.DoneChecker {
	return syncedDoneChecker("pgInformer:"+inf.gvkStr, &inf.synced)
}

func (inf *pgInformer) IsStopped() bool {
	return inf.stopped.Load()
}

func (inf *pgInformer) run(ctx context.Context) {
	defer inf.stopped.Store(true)

	for ctx.Err() == nil {
		if err := inf.listAndWatch(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			inf.logger.Error(err, "list-watch cycle failed")
			time.Sleep(time.Second)
		}
	}
}

func (inf *pgInformer) listAndWatch(ctx context.Context) error {
	listConn, err := inf.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("list connect: %w", err)
	}

	result, err := reader.List(ctx, listConn.Conn(), inf.gvkStr, inf.bucketIDs)
	listConn.Release()
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}

	inf.processListResult(result.Resources, !inf.synced.Load())

	if !inf.synced.Load() {
		inf.synced.Store(true)
		inf.handlerMu.RLock()
		for _, h := range inf.handlers {
			h.reg.synced.Store(true)
		}
		inf.handlerMu.RUnlock()
	}

	inf.logger.V(1).Info("watching", "rv", result.ResourceVersion.String())

	pollPoolConn, err := inf.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("watch poll connect: %w", err)
	}
	pollConn := pollPoolConn.Hijack()

	listenPoolConn, err := inf.pool.Acquire(ctx)
	if err != nil {
		pollConn.Close(ctx)
		return fmt.Errorf("watch listen connect: %w", err)
	}
	listenConn := listenPoolConn.Hijack()

	watcher := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:       inf.gvkStr,
		BucketIDs: inf.bucketIDs,
		StartRV:   result.ResourceVersion,
	}, nil)

	go func() {
		_ = watcher.Run(ctx)
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	for ev := range watcher.Events() {
		inf.processEvent(ev)
	}

	return nil
}

func (inf *pgInformer) processListResult(resources []model.Resource, isInitialList bool) {
	inf.storeMu.Lock()
	defer inf.storeMu.Unlock()

	seen := make(map[types.NamespacedName]bool, len(resources))

	for _, r := range resources {
		key := types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
		seen[key] = true

		obj, err := resourceToObject(r, inf.scheme)
		if err != nil {
			inf.logger.Error(err, "convert resource", "ns", r.Namespace, "name", r.Name)
			continue
		}
		rv := obj.GetResourceVersion()

		existing, exists := inf.store[key]
		inf.store[key] = storedObject{resourceVersion: rv, obj: obj}

		if !exists {
			inf.dispatchAdd(obj, isInitialList)
		} else if existing.resourceVersion != rv {
			inf.dispatchUpdate(existing.obj, obj)
		}
	}

	for key, so := range inf.store {
		if !seen[key] {
			delete(inf.store, key)
			inf.dispatchDelete(so.obj)
		}
	}
}

func (inf *pgInformer) processEvent(ev reader.Event) {
	obj, err := resourceToObject(ev.Resource, inf.scheme)
	if err != nil {
		inf.logger.Error(err, "convert event resource")
		return
	}
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}

	inf.storeMu.Lock()
	defer inf.storeMu.Unlock()

	switch ev.Type {
	case reader.EventAdded:
		old, exists := inf.store[key]
		inf.store[key] = storedObject{resourceVersion: obj.GetResourceVersion(), obj: obj}
		if exists {
			inf.dispatchUpdate(old.obj, obj)
		} else {
			inf.dispatchAdd(obj, false)
		}
	case reader.EventModified:
		old, exists := inf.store[key]
		inf.store[key] = storedObject{resourceVersion: obj.GetResourceVersion(), obj: obj}
		if exists {
			inf.dispatchUpdate(old.obj, obj)
		} else {
			inf.dispatchAdd(obj, false)
		}
	case reader.EventDeleted:
		if hasFinalizers(ev.Resource) {
			old, exists := inf.store[key]
			inf.store[key] = storedObject{resourceVersion: obj.GetResourceVersion(), obj: obj}
			if exists {
				inf.dispatchUpdate(old.obj, obj)
			} else {
				inf.dispatchAdd(obj, false)
			}
		} else if so, exists := inf.store[key]; exists {
			delete(inf.store, key)
			inf.dispatchDelete(so.obj)
		}
	}
}

func (inf *pgInformer) dispatchAdd(obj client.Object, isInitialList bool) {
	inf.handlerMu.RLock()
	defer inf.handlerMu.RUnlock()
	for _, h := range inf.handlers {
		h.handler.OnAdd(obj, isInitialList)
	}
}

func (inf *pgInformer) dispatchUpdate(oldObj, newObj client.Object) {
	inf.handlerMu.RLock()
	defer inf.handlerMu.RUnlock()
	for _, h := range inf.handlers {
		h.handler.OnUpdate(oldObj, newObj)
	}
}

func (inf *pgInformer) dispatchDelete(obj client.Object) {
	inf.handlerMu.RLock()
	defer inf.handlerMu.RUnlock()
	for _, h := range inf.handlers {
		h.handler.OnDelete(obj)
	}
}

// syncedDoneChecker wraps an atomic.Bool as a toolscache.DoneChecker (snapshot, not live).
func syncedDoneChecker(name string, flag *atomic.Bool) toolscache.DoneChecker {
	return &doneChecker{n: name, flag: flag}
}

type doneChecker struct {
	n    string
	flag *atomic.Bool
}

func (d *doneChecker) Name() string { return d.n }
func (d *doneChecker) Done() <-chan struct{} {
	ch := make(chan struct{})
	if d.flag.Load() {
		close(ch)
	}
	return ch
}

// compile-time interface checks
var _ cache.Cache = (*pgCache)(nil)
var _ cache.Informer = (*pgInformer)(nil)
var _ toolscache.ResourceEventHandlerRegistration = (*pgHandlerRegistration)(nil)
