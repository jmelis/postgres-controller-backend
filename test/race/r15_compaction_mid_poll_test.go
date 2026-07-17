package race_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/pkg/compaction"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
)

// R15 — Compaction mid-poll (B3: I6 direct violation).
//
// The poll cycle checks the compaction horizon and then queries rows in two separate
// statements with no shared snapshot. A compactor committing between them deletes
// tombstones that the watcher's hwm says it should have seen — the watcher
// silently skips Deleted events and never receives 410.
//
// Defense (Phase 2): one REPEATABLE READ read-only transaction per poll cycle.
// Horizon checks and row queries share the same snapshot; mid-poll compaction
// is invisible.
//
// Expected current failure: watcher delivers only the live resource (seq=4),
// silently skipping the 3 compacted tombstones, and does NOT return ErrGone.
func TestR15_CompactionMidPoll(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wr := newWriter(t, nil)

	// Write 3 tombstones with old deletion_timestamp (eligible for compaction)
	for i := 0; i < 3; i++ {
		past := time.Now().Add(-48 * time.Hour)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("r15-victim-%d", i))
		req.DeletionTimestamp = &past
		_, err := wr.Write(ctx, req)
		if err != nil {
			t.Fatalf("setup tombstone %d: %v", i, err)
		}
	}

	// Write 1 live resource
	_, err := wr.Write(ctx, makeWriteReq("apps/v1/Deployment", "default",
		"r15-survivor"))
	if err != nil {
		t.Fatalf("setup survivor: %v", err)
	}

	// Hook: after the horizon check (finds nothing compacted yet), run
	// compaction on a separate connection. This deletes the 3 tombstones and
	// advances the horizon. The subsequent row query (on the same
	// conn, no transaction) sees the post-compaction state.
	compacted := make(chan struct{})
	hook := &compactionMidPollHook{
		t:         t,
		once:      sync.Once{},
		compacted: compacted,
	}

	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		StartRV:          resourceversion.RV{Watermark: 0},
		BaselineInterval: 300 * time.Millisecond,
	}, hook)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()

	// Wait for mid-poll compaction to complete
	select {
	case <-compacted:
	case <-time.After(10 * time.Second):
		watchCancel()
		<-done
		pollConn.Close(context.Background())
		t.Fatal("compaction hook did not fire")
	}

	// Collect events. Correct behavior is one of:
	// (a) All 4 events delivered (3 Deleted + 1 Added) — snapshot predates compaction
	// (b) ErrGone — horizon (3) is above our hwm (0)
	var events []reader.Event
	var watchErr error

	collectTimer := time.NewTimer(3 * time.Second)
	defer collectTimer.Stop()

collectLoop:
	for {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				watchErr = <-done
				break collectLoop
			}
			events = append(events, ev)
		case err := <-done:
			watchErr = err
			break collectLoop
		case <-collectTimer.C:
			break collectLoop
		}
	}

	watchCancel()
	if watchErr == nil {
		<-done
	}
	pollConn.Close(context.Background())

	if watchErr != nil && errors.Is(watchErr, reader.ErrGone) {
		return // correct: detected compaction past hwm
	}
	if len(events) >= 4 {
		return // correct: snapshot delivered all events
	}

	// B3 bug: only the survivor was delivered, tombstones silently skipped
	t.Fatalf("B3: watcher silently skipped compacted tombstones: "+
		"got %d events (expected 4 or ErrGone), err=%v", len(events), watchErr)
}

// compactionMidPollHook implements WatchHooks + WatchHooksWithHorizon.
// AfterHorizonCheck runs compaction on a fresh connection, then returns.
type compactionMidPollHook struct {
	t         *testing.T
	once      sync.Once
	compacted chan struct{}
}

func (h *compactionMidPollHook) BeforePoll()                {}
func (h *compactionMidPollHook) AfterPoll(_ []reader.Event) {}

func (h *compactionMidPollHook) AfterHorizonCheck() {
	h.once.Do(func() {
		conn := freshConn(h.t)
		defer conn.Close(context.Background())
		result, err := compaction.Compact(context.Background(), conn,
			compaction.Config{Retention: 1 * time.Hour})
		if err != nil {
			h.t.Logf("compaction in hook: %v", err)
		} else {
			h.t.Logf("compaction in hook: deleted %d tombstones", result.Deleted)
		}
		close(h.compacted)
	})
}
