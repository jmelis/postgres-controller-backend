package crbridge

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/reader"
	"github.com/jmelisba/postgres-controller-backend/internal/resourceversion"
)

// ListResult holds the result of a List operation.
type ListResult struct {
	Objects         []*Object
	ResourceVersion string // composite RV string
}

// ListerWatcher provides the cache.ListerWatcher shape.
type ListerWatcher struct {
	connFactory func() (*pgx.Conn, error)
	gvk         string
	bucketIDs   []int
}

// NewListerWatcher creates a ListerWatcher. connFactory is called for each
// List and Watch to obtain a fresh connection (Watch needs dedicated conns for
// LISTEN and poll).
func NewListerWatcher(connFactory func() (*pgx.Conn, error), gvk string, bucketIDs []int) *ListerWatcher {
	return &ListerWatcher{
		connFactory: connFactory,
		gvk:         gvk,
		bucketIDs:   bucketIDs,
	}
}

// List performs a REPEATABLE READ snapshot list and returns all live objects
// with the composite RV set as ResourceVersion.
func (lw *ListerWatcher) List(ctx context.Context) (*ListResult, error) {
	conn, err := lw.connFactory()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	result, err := reader.List(ctx, conn, lw.gvk, lw.bucketIDs)
	if err != nil {
		return nil, err
	}

	objects := make([]*Object, len(result.Resources))
	for i, r := range result.Resources {
		objects[i] = objectFromResource(r)
	}

	return &ListResult{
		Objects:         objects,
		ResourceVersion: result.ResourceVersion.String(),
	}, nil
}

// Watch starts a watch from the given resourceVersion string. The caller must
// call Stop on the returned WatchInterface when done.
func (lw *ListerWatcher) Watch(ctx context.Context, rvString string) (WatchInterface, error) {
	startRV, err := resourceversion.Parse(rvString)
	if err != nil {
		return nil, err
	}

	pollConn, err := lw.connFactory()
	if err != nil {
		return nil, err
	}

	var listenConn *pgx.Conn
	listenConn, err = lw.connFactory()
	if err != nil {
		pollConn.Close(ctx)
		return nil, err
	}

	watcher := reader.NewWatcher(pollConn, listenConn, reader.WatcherConfig{
		GVK:       lw.gvk,
		BucketIDs: lw.bucketIDs,
		StartRV:   startRV,
	}, nil)

	go func() {
		_ = watcher.Run(ctx)
		pollConn.Close(context.Background())
		listenConn.Close(context.Background())
	}()

	return newWatchAdapter(watcher), nil
}
