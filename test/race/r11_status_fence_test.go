package race_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmelisba/postgres-controller-backend/internal/lease"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R11 — Status fence-expiry race (I4 for status sub-resource).
// Mirrors R1 but for the status write path: FOR SHARE on bucket_status_leases
// must block a coordinator's Grant (epoch bump UPDATE) while a status writer
// is mid-transaction.
func TestR11_StatusFenceExpiryRace(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	// Setup: spec lease for holder-a (to create the resource)
	specEpoch := setupLease(t, 1, "holder-a", 60_000_000_000)

	// Create the resource via spec write
	specWriter := newWriter(t, nil)
	createReq := makeWriteReq("apps/v1/Deployment", "default", "status-fence-test",
		1, "holder-a", specEpoch)
	createResult, err := specWriter.Write(ctx, createReq)
	require.NoError(t, err)

	// Setup: status lease for status-holder-a
	statusEpoch := setupStatusLease(t, 1, "status-holder-a", 60_000_000_000)

	// Session A: status writer with blocking hook — pauses at BeforeCommit
	hook := newBlockingHook()
	statusWriterA := writer.New(freshConn(t), hook)

	type writeResult struct {
		result interface{}
		err    error
	}
	aCh := make(chan writeResult, 1)
	go func() {
		req := model.StatusWriteRequest{
			GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-fence-test",
			BucketID: 1, Status: json.RawMessage(`{"ready":true}`),
			LeaseHolder: "status-holder-a", LeaseEpoch: statusEpoch,
			ExpectedVersion: createResult.ObjectVersion,
		}
		r, err := statusWriterA.WriteStatus(ctx, req)
		aCh <- writeResult{r, err}
	}()

	// Wait for A to reach BeforeCommit (holds FOR SHARE on bucket_status_leases)
	select {
	case <-hook.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for status writer A to reach BeforeCommit")
	}

	// Session B: coordinator grants status lease to status-holder-b
	grantConn := freshConn(t)
	coordinator := lease.NewStatusManager(grantConn, "coordinator")

	bCh := make(chan writeResult, 1)
	go func() {
		newEpoch, err := coordinator.Grant(ctx, 1, "status-holder-b", 60*time.Second)
		bCh <- writeResult{newEpoch, err}
	}()

	// Verify B is blocked (should not complete within 500ms)
	select {
	case r := <-bCh:
		t.Fatalf("Grant completed while FOR SHARE held — I4 violated (epoch=%v, err=%v)", r.result, r.err)
	case <-time.After(500 * time.Millisecond):
	}

	// Unblock A
	close(hook.proceed)

	// A's write should succeed
	aResult := <-aCh
	require.NoError(t, aResult.err, "status writer A's write must succeed")

	// B's Grant should now complete
	bResult := <-bCh
	require.NoError(t, bResult.err, "Grant must succeed after A releases lock")

	// A's next WriteStatus with the OLD epoch must fail
	statusWriterA2 := writer.New(freshConn(t), nil)
	staleReq := model.StatusWriteRequest{
		GVK: "apps/v1/Deployment", Namespace: "default", Name: "status-fence-test",
		BucketID: 1, Status: json.RawMessage(`{"ready":false}`),
		LeaseHolder: "status-holder-a", LeaseEpoch: statusEpoch,
		ExpectedVersion: 2,
	}
	_, err = statusWriterA2.WriteStatus(ctx, staleReq)
	assert.ErrorIs(t, err, writer.ErrFenceViolation, "WriteStatus with stale epoch must be fenced")
}
