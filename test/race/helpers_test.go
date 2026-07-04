package race_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/jmelisba/postgres-controller-backend/test/testinfra"
)

var sharedDB *testinfra.TestDB

func TestMain(m *testing.M) {
	sharedDB = testinfra.StartPostgresForTestMain()
	code := m.Run()
	sharedDB.Stop()
	os.Exit(code)
}

func freshConn(t *testing.T) *pgx.Conn {
	t.Helper()
	return sharedDB.Connect(t)
}

func truncateAll(t *testing.T) {
	t.Helper()
	conn := freshConn(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())
}

func setupLease(t *testing.T, bucketID int, holder string, ttl time.Duration) int64 {
	t.Helper()
	conn := freshConn(t)
	defer conn.Close(context.Background())
	mgr := lease.NewManager(conn, holder)
	epoch, err := mgr.Acquire(context.Background(), bucketID, ttl)
	if err != nil {
		t.Fatalf("setup lease: %v", err)
	}
	return epoch
}

func makeWriteReq(gvk, ns, name string, bucketID int, holder string, epoch int64) model.WriteRequest {
	return model.WriteRequest{
		GVK:         gvk,
		Namespace:   ns,
		Name:        name,
		BucketID:    bucketID,
		Spec:        json.RawMessage(`{"replicas":1}`),
		Status:      json.RawMessage(`{}`),
		Metadata:    json.RawMessage(`{}`),
		LeaseHolder: holder,
		LeaseEpoch:  epoch,
	}
}

func newWriter(t *testing.T, hooks writer.TxHooks) *writer.Writer {
	t.Helper()
	return writer.New(freshConn(t), hooks)
}

// blockingHook implements TxHooks to pause at BeforeCommit.
type blockingHook struct {
	ready   chan struct{} // closed when the hook is entered
	proceed chan struct{} // closed to let the hook return
}

func newBlockingHook() *blockingHook {
	return &blockingHook{
		ready:   make(chan struct{}),
		proceed: make(chan struct{}),
	}
}

func (h *blockingHook) AfterFence(_ context.Context, _ pgx.Tx) error   { return nil }
func (h *blockingHook) AfterCounter(_ context.Context, _ pgx.Tx, _ int64) error { return nil }

func (h *blockingHook) BeforeCommit(ctx context.Context, _ pgx.Tx) error {
	close(h.ready)
	select {
	case <-h.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
