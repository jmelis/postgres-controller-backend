package toxirace_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R5 Toxi — Ambiguous commit via network-level connection kill (I1/I4).
// The writer uses a proxied connection. At BeforeCommit, we add a reset_peer
// toxic that kills the connection. The COMMIT is sent but the response is lost
// (or the connection dies before the response arrives). The writer must detect
// the ambiguous state and the read-back protocol via a direct connection must
// resolve it.
func TestR5_Toxi_AmbiguousCommit_ResetPeer(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Writer uses PROXIED connection (will be killed)
	proxiedWriterConn, err := pdb.ProxiedConn(ctx)
	require.NoError(t, err)

	hook := &toxiCommitCutHook{t: t}
	w := writer.New(proxiedWriterConn, hook)

	req := makeWriteReq("apps/v1/Deployment", "default", "toxi-ambig")
	result, writeErr := w.Write(ctx, req)

	if writeErr != nil {
		t.Logf("write returned error (expected for ambiguous path): %v", writeErr)

		// Extract the AmbiguousCommitError to get the txid for read-back
		var ambErr *writer.AmbiguousCommitError
		require.True(t, errors.As(writeErr, &ambErr), "expected AmbiguousCommitError, got: %v", writeErr)

		// Read-back via DIRECT connection (bypasses proxy)
		directReadConn := directConn(t)
		cleanWriter := writer.New(directReadConn, nil)

		resource, err := cleanWriter.ReadBack(ctx, req.GVK, req.Namespace, req.Name, ambErr.Txid)
		require.NoError(t, err)

		if resource != nil {
			t.Log("read-back: write landed despite connection kill")
			assert.True(t, resource.TxidStamp > 0, "txid should be positive")
		} else {
			t.Log("read-back: write did not land, retrying via direct conn")
			directWriteConn := directConn(t)
			retryWriter := writer.New(directWriteConn, nil)
			retryResult, err := retryWriter.Write(ctx, req)
			require.NoError(t, err)
			assert.True(t, retryResult.Txid > 0, "retry txid should be positive")
		}
	} else {
		t.Logf("write succeeded cleanly despite proxy kill: txid=%d", result.Txid)
		assert.True(t, result.Txid > 0, "txid should be positive")
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

func (h *toxiCommitCutHook) AfterSuppressionCheck(_ context.Context, _ pgx.Tx, _ bool) error    { return nil }
func (h *toxiCommitCutHook) AfterTxidAcquire(_ context.Context, _ pgx.Tx, _ uint64) error      { return nil }

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
