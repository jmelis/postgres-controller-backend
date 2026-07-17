package race_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R3 — Doorbell loss (I3).
// LISTEN connection drops silently; notifications lost.
// Defense: poll-primary — baseline poll delivers regardless.
// Test: watcher with NO listen connection (simulating total doorbell loss);
// assert every event still delivered within baseline interval.
func TestR3_DoorbellLoss(t *testing.T) {
	truncateAll(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// NO listen connection — doorbell is completely absent
	pollConn := connectManualShared(t)
	w := reader.NewWatcher(pollConn, nil, reader.WatcherConfig{
		GVK:              "apps/v1/Deployment",
		StartRV:          resourceversion.RV{Watermark: 0},
		BaselineInterval: 300 * time.Millisecond,
	}, nil)

	watchCtx, watchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(watchCtx) }()
	defer func() {
		watchCancel()
		<-done
		pollConn.Close(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	// Write 5 resources
	for i := 0; i < 5; i++ {
		wr := newWriter(t, nil)
		req := makeWriteReq("apps/v1/Deployment", "default",
			fmt.Sprintf("doorbell-loss-%d", i))
		_, err := wr.Write(ctx, req)
		require.NoError(t, err)
	}

	// All events must arrive within 2 baseline intervals (600ms + margin)
	var events []reader.Event
	deadline := time.After(3 * time.Second)
	for len(events) < 5 {
		select {
		case ev := <-w.Events():
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timeout: got %d/5 events without doorbell", len(events))
		}
	}

	assert.Len(t, events, 5)
	// Verify no duplicates
	txids := make(map[uint64]bool)
	for _, ev := range events {
		assert.False(t, txids[ev.Resource.TxidStamp], "duplicate txid %d", ev.Resource.TxidStamp)
		txids[ev.Resource.TxidStamp] = true
	}
}
