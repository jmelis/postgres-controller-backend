package toxirace_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R5 Toxi — Ambiguous commit via network-level connection kill (I1/I5).
// The writer uses a proxied connection. At BeforeCommit, we add a reset_peer
// toxic that kills the connection. The COMMIT is sent but the response is lost
// (or the connection dies before the response arrives). The writer must detect
// the ambiguous state and the read-back protocol via a direct connection must
// resolve it.
func TestR5_Toxi_AmbiguousCommit_ResetPeer(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Writer uses PROXIED connection (will be killed)
	proxiedWriterConn, err := pdb.ProxiedConn(ctx)
	require.NoError(t, err)

	hook := &toxiCommitCutHook{t: t}
	w := writer.New(proxiedWriterConn, hook)

	req := makeWriteReq("apps/v1/Deployment", "default", "toxi-ambig", 1, "holder-a", epoch)
	result, writeErr := w.Write(ctx, req)

	if writeErr != nil {
		t.Logf("write returned error (expected for ambiguous path): %v", writeErr)

		// Read-back via DIRECT connection (bypasses proxy)
		directReadConn := directConn(t)
		cleanWriter := writer.New(directReadConn, nil)

		resource, err := cleanWriter.ReadBack(ctx, req.GVK, req.Namespace, req.Name, 1)
		require.NoError(t, err)

		if resource != nil {
			t.Log("read-back: write landed despite connection kill")
			assert.Equal(t, int64(1), resource.GVKBucketSeq)
		} else {
			t.Log("read-back: write did not land, retrying via direct conn")
			directWriteConn := directConn(t)
			retryWriter := writer.New(directWriteConn, nil)
			retryResult, err := retryWriter.Write(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, int64(1), retryResult.Seq)
		}

		// Verify counter state
		verifyConn := directConn(t)
		var counterVal int64
		err = verifyConn.QueryRow(ctx,
			`SELECT current_seq FROM gvk_bucket_counters WHERE bucket_id = 1 AND gvk = 'apps/v1/Deployment'`,
		).Scan(&counterVal)
		require.NoError(t, err)
		assert.Equal(t, int64(1), counterVal, "exactly one seq must be issued")
	} else {
		t.Logf("write succeeded cleanly despite proxy kill: seq=%d", result.Seq)
		assert.Equal(t, int64(1), result.Seq)
	}

	// Final invariant: exactly one resource row
	verifyConn := directConn(t)
	var count int
	err = verifyConn.QueryRow(ctx,
		`SELECT count(*) FROM kubernetes_resources WHERE gvk = 'apps/v1/Deployment' AND namespace = 'default' AND name = 'toxi-ambig'`,
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one resource row must exist")

	// Clean up: remove toxic if it's still there, re-enable proxy
	pdb.Proxy.RemoveToxic("commit-kill")
	pdb.Proxy.Enable()
	proxiedWriterConn.Close(ctx)
}

// toxiCommitCutHook activates a reset_peer toxic at BeforeCommit to kill the
// connection at the network level.
type toxiCommitCutHook struct {
	t *testing.T
}

func (h *toxiCommitCutHook) AfterFence(_ context.Context, _ pgx.Tx) error                    { return nil }
func (h *toxiCommitCutHook) AfterSuppressionCheck(_ context.Context, _ pgx.Tx, _ bool) error { return nil }
func (h *toxiCommitCutHook) AfterCounter(_ context.Context, _ pgx.Tx, _ int64) error         { return nil }

func (h *toxiCommitCutHook) BeforeCommit(_ context.Context, _ pgx.Tx) error {
	// Add reset_peer toxic — kills existing connections by sending a RST
	_, err := pdb.Proxy.AddToxic("commit-kill", "reset_peer", "downstream", 1.0, nil)
	if err != nil {
		h.t.Logf("warning: failed to add toxic: %v", err)
	}
	// Small delay for the toxic to take effect on the connection
	time.Sleep(50 * time.Millisecond)
	return nil
}
