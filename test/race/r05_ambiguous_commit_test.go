package race_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R5 — Ambiguous commit (I1/I5).
// Connection drops during COMMIT; client doesn't know if the write landed.
// Defense: read-back protocol.
//
// We wrap the net.Conn to cut the response after COMMIT is sent to the server.
// The writer sees an error from tx.Commit() and must use ReadBack to determine
// whether the write actually landed.
func TestR5_AmbiguousCommit(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Create a connection through a FaultConn that we can cut mid-COMMIT
	var fault *faultConn
	var cutSignal atomic.Bool

	cfg, err := pgx.ParseConfig(sharedDB.ConnStr)
	require.NoError(t, err)

	origDialer := cfg.DialFunc
	if origDialer == nil {
		origDialer = (&net.Dialer{}).DialContext
	}
	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := origDialer(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		fault = &faultConn{Conn: c, cutSignal: &cutSignal}
		return fault, nil
	}

	faultyConn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { faultyConn.Close(context.Background()) })

	// Hook that cuts the response channel at BeforeCommit
	hook := &commitCutHook{cutSignal: &cutSignal}
	w := writer.New(faultyConn, hook)

	req := makeWriteReq("apps/v1/Deployment", "default", "ambiguous", 1)
	result, writeErr := w.Write(ctx, req)

	// The write might succeed (if COMMIT response made it through before the cut)
	// or fail with an ambiguous error. Either is valid.
	if writeErr != nil {
		// Ambiguous case: use a clean connection to read back
		t.Logf("write returned error (expected): %v", writeErr)

		cleanConn := freshConn(t)
		cleanWriter := writer.New(cleanConn, nil)

		// Read back: check if seq 1 landed
		resource, err := cleanWriter.ReadBack(ctx, req.GVK, req.Namespace, req.Name, 1)
		require.NoError(t, err)

		if resource != nil {
			// Write actually landed despite the error
			t.Log("read-back: write landed, treating as success")
			assert.Equal(t, int64(1), resource.GVKBucketSeq)
		} else {
			// Write did not land — retry
			t.Log("read-back: write did not land, retrying")
			retryResult, err := cleanWriter.Write(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, int64(1), retryResult.Seq)
		}

		// Verify: counter is exactly 1, no gap, no double
		var counterVal int64
		err = cleanConn.QueryRow(ctx,
			`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
		).Scan(&counterVal)
		require.NoError(t, err)
		assert.Equal(t, int64(1), counterVal, "exactly one seq must be issued")

	} else {
		// Clean success despite the fault (timing — cut happened after full response)
		t.Logf("write succeeded cleanly: seq=%d", result.Seq)
		assert.Equal(t, int64(1), result.Seq)
	}

	// Final invariant: exactly one resource row exists
	verifyConn := freshConn(t)
	var count int
	err = verifyConn.QueryRow(ctx,
		`SELECT count(*) FROM kubernetes_resources WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'ambiguous'`,
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one resource row must exist")
}

// faultConn wraps a net.Conn and can be signaled to drop read responses.
type faultConn struct {
	net.Conn
	cutSignal *atomic.Bool
}

func (c *faultConn) Read(b []byte) (int, error) {
	if c.cutSignal.Load() {
		c.Conn.Close()
		return 0, net.ErrClosed
	}
	return c.Conn.Read(b)
}

// commitCutHook signals the faultConn to cut reads just before COMMIT.
type commitCutHook struct {
	cutSignal *atomic.Bool
}

func (h *commitCutHook) AfterSuppressionCheck(_ context.Context, _ pgx.Tx, _ bool) error { return nil }
func (h *commitCutHook) AfterCounter(_ context.Context, _ pgx.Tx, _ int64) error         { return nil }

func (h *commitCutHook) BeforeCommit(_ context.Context, _ pgx.Tx) error {
	// Cut reads immediately — the COMMIT will be sent by pgx but the
	// response will be dropped. This makes the outcome truly ambiguous.
	h.cutSignal.Store(true)
	return nil
}
