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

// B4 — Verifier false-positive I1 under event coalescing.
//
// When two writes target the same key between polls (create at seq=1, update at
// seq=2), the row is updated in-place and only seq=2 survives. The watcher
// delivers seq=2; the verifier sees a gap (expected 1, got 2) and flags I1.
// This is a false positive: the gap is explained by legitimate coalescing, not
// data loss.
//
// Defense (Phase 4): recognise coalescing gaps — only flag I1 when the gap is
// NOT explainable by a seq that was overwritten by a newer version of the same
// key before the poll snapshot.
//
// Expected current failure: verifier reports an I1 violation because the gap
// from seq=1 to seq=2 is not explained by compaction_horizon.
func TestCoalescingFalsePositive(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	epoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	wrConn := freshConn(t)
	wr := writer.New(wrConn, nil)

	// Create resource at seq=1
	req := model.WriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "coalesce-test",
		BucketID: 1, Spec: json.RawMessage(`{"v":1}`),
		Status: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
		LeaseHolder: "holder-a", LeaseEpoch: epoch,
	}
	result, err := wr.Write(ctx, req)
	require.NoError(t, err)

	// Update same resource at seq=2 — the row now has seq=2, seq=1 is gone
	req.ExpectedVersion = result.ObjectVersion
	req.Spec = json.RawMessage(`{"v":2}`)
	_, err = wr.Write(ctx, req)
	require.NoError(t, err)

	// Start verifier from hwm=0 — it will poll and only see seq=2
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
	time.Sleep(1 * time.Second)

	res := v.Result()
	verCancel()
	<-done
	pollConn.Close(context.Background())

	assert.Equal(t, int64(1), res.EventsChecked,
		"should see exactly 1 event (seq=2, since seq=1 was overwritten)")
	assert.Empty(t, res.Violations,
		"B4: false-positive I1 under event coalescing: %v", res.Violations)
}
