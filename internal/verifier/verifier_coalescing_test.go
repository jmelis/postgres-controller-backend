package verifier_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/verifier"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B4 — Verifier false-positive under event coalescing.
//
// When two writes target the same key between polls (create then update),
// the row is updated in-place and only the latest txid survives. The watcher
// delivers the latest txid; the verifier sees a gap (expected previous, got
// later) and could flag a violation. This is a false positive: the gap is
// explained by legitimate coalescing, not data loss.
//
// Defense (Phase 4): recognise coalescing gaps — only flag when the gap is
// NOT explainable by a txid that was overwritten by a newer version of the same
// key before the poll snapshot.
//
// Expected current failure: verifier reports a violation because the gap
// is not explained by compaction_horizon.
func TestCoalescingFalsePositive(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wrConn := freshConn(t)
	wr := writer.New(wrConn, nil)

	// Create resource
	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "coalesce-test",
		Spec: json.RawMessage(`{"v":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
	}
	result, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Update same resource — the row now has a newer txid, old txid is gone
	req.ExpectedVersion = result.ObjectVersion
	req.Spec = json.RawMessage(`{"v":2}`)
	_, err = wr.Write(ctx, req)
	require.NoError(t, err)

	// Start verifier from hwm=0 — it will poll and only see the latest txid
	pollConn := manualConn(t)
	v := verifier.New(pollConn, nil, verifier.Config{
		GVK:          "apps/v1/Deployment",
		PollInterval: 200 * time.Millisecond,
	})

	verCtx, verCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- v.Run(verCtx) }()

	// Wait for verifier to see the event
	time.Sleep(1 * time.Second)

	res := v.Result()
	verCancel()
	<-done
	pollConn.Close(context.Background())

	assert.Equal(t, int64(1), res.EventsChecked,
		"should see exactly 1 event (latest txid, since earlier was overwritten)")
	assert.Empty(t, res.Violations,
		"B4: false-positive I1 under event coalescing: %v", res.Violations)
}
