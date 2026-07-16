package toxirace_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
)

var pdb *testinfra.ProxiedDB

func TestMain(m *testing.M) {
	pdb = testinfra.StartPostgresWithProxy()
	code := m.Run()
	pdb.Stop()
	os.Exit(code)
}

func directConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pdb.DirectConn(context.Background())
	if err != nil {
		t.Fatalf("direct conn: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func proxiedConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pdb.ProxiedConn(context.Background())
	if err != nil {
		t.Fatalf("proxied conn: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func truncateAll(t *testing.T) {
	t.Helper()
	conn := directConn(t)
	tables := []string{
		"kubernetes_resources",
		"compaction_horizon",
	}
	ctx := context.Background()
	for _, tbl := range tables {
		if _, err := conn.Exec(ctx, "TRUNCATE "+tbl+" CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", tbl, err)
		}
	}
}

func makeWriteReq(gvk, ns, name string) model.WriteRequest {
	return model.WriteRequest{
		GVK: gvk, Namespace: ns, Name: name,
		Spec: json.RawMessage(`{"replicas":1}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}
}

func directWriter(t *testing.T, hooks writer.TxHooks) *writer.Writer {
	t.Helper()
	return writer.New(directConn(t), hooks)
}
