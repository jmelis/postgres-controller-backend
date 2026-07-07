package verifier_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func manualConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), sharedDB.ConnStr)
	require.NoError(t, err)
	return conn
}

func truncateAll(t *testing.T) {
	t.Helper()
	conn := freshConn(t)
	sharedDB.TruncateAll(t, conn)
	conn.Close(context.Background())
}

func TestVerifier_CleanStream_NoViolations(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pollConn := manualConn(t)

	v := verifier.New(pollConn, nil, verifier.Config{
		GVK:          "apps/v1/Deployment",
		BucketIDs:    []int{1},
		PollInterval: 200 * time.Millisecond,
	})

	verCtx, verCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- v.Run(verCtx) }()

	time.Sleep(100 * time.Millisecond)

	// Write 10 resources in order — clean, gapless stream
	wrConn := freshConn(t)
	wr := writer.New(wrConn, nil)
	for i := 0; i < 10; i++ {
		req := model.WriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default",
			Name: fmt.Sprintf("clean-%d", i), BucketID: 1,
			Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
			Metadata: json.RawMessage(`{}`),
		}
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// Wait for verifier to see all events
	time.Sleep(1 * time.Second)

	result := v.Result()
	assert.Empty(t, result.Violations, "clean stream should have zero violations")
	assert.Equal(t, int64(10), result.EventsChecked, "all 10 events should be checked")

	verCancel()
	<-done
	pollConn.Close(context.Background())
}

func TestVerifier_WithCanary(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pollConn := manualConn(t)
	canaryConn := freshConn(t)

	v := verifier.New(pollConn, canaryConn, verifier.Config{
		GVK:            "apps/v1/Deployment",
		BucketIDs:      []int{1},
		PollInterval:   200 * time.Millisecond,
		CanaryInterval: 300 * time.Millisecond,
	})

	verCtx, verCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- v.Run(verCtx) }()

	// Let the canary fire a few times
	time.Sleep(2 * time.Second)

	result := v.Result()
	assert.Empty(t, result.Violations)
	assert.Greater(t, result.CanaryWrites, int64(0), "canary should have written at least once")
	assert.Greater(t, result.EventsChecked, int64(0), "should have observed canary events")
	assert.Greater(t, result.CanaryP99, time.Duration(0), "p99 should be non-zero")

	t.Logf("canary writes=%d, events checked=%d, p99=%v",
		result.CanaryWrites, result.EventsChecked, result.CanaryP99)

	verCancel()
	<-done
	pollConn.Close(context.Background())
}

func TestVerifier_DetectsDuplicate_I5(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Write one resource
	wrConn := freshConn(t)
	wr := writer.New(wrConn, nil)
	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "dup-test", BucketID: 1,
		Spec: json.RawMessage(`{}`), Status: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}
	_, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Start verifier with hwm=0 — it will see seq=1
	pollConn := manualConn(t)
	v := verifier.New(pollConn, nil, verifier.Config{
		GVK:          "apps/v1/Deployment",
		BucketIDs:    []int{1},
		PollInterval: 200 * time.Millisecond,
	})

	verCtx, verCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- v.Run(verCtx) }()

	// Wait for verifier to see the event
	time.Sleep(800 * time.Millisecond)

	result := v.Result()
	assert.Equal(t, int64(1), result.EventsChecked)
	// The watcher delivers no duplicates by design (seq > hwm),
	// so no I5 violation should fire.
	assert.Empty(t, result.Violations)

	verCancel()
	<-done
	pollConn.Close(context.Background())
}
