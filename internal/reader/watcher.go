package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
)

var ErrGone = errors.New("410 Gone: resource version too old")

type WatcherConfig struct {
	GVK              string
	BucketIDs        []int
	StartRV          resourceversion.RV
	BaselineInterval time.Duration // default 5s
	DebounceFloor    time.Duration // default 100ms
}

type Watcher struct {
	cfg        WatcherConfig
	pollConn   *pgx.Conn
	listenConn *pgx.Conn
	events     chan Event
	hwm        map[int]int64 // per-bucket high-water mark; only touched from the Run goroutine
	stopCh     chan struct{}
	hooks      WatchHooks
}

// WatchHooks allows tests to observe or inject behavior during poll cycles.
type WatchHooks interface {
	BeforePoll()
	AfterPoll(events []Event)
}

// WatchHooksWithHorizon is an optional extension for testing compaction races.
// If the hooks value implements this interface, AfterHorizonCheck is called
// between the compaction horizon check and the row query in pollBucket.
type WatchHooksWithHorizon interface {
	AfterHorizonCheck(bucketID int)
}

func NewWatcher(pollConn, listenConn *pgx.Conn, cfg WatcherConfig, hooks WatchHooks) *Watcher {
	if cfg.BaselineInterval == 0 {
		cfg.BaselineInterval = 5 * time.Second
	}
	if cfg.DebounceFloor == 0 {
		cfg.DebounceFloor = 100 * time.Millisecond
	}

	hwm := make(map[int]int64, len(cfg.BucketIDs))
	for _, bid := range cfg.BucketIDs {
		if seq, ok := cfg.StartRV.Buckets[bid]; ok {
			hwm[bid] = seq
		}
	}

	return &Watcher{
		cfg:        cfg,
		pollConn:   pollConn,
		listenConn: listenConn,
		events:     make(chan Event, 256),
		hwm:        hwm,
		stopCh:     make(chan struct{}),
		hooks:      hooks,
	}
}

// Events returns the channel on which watch events are delivered.
func (w *Watcher) Events() <-chan Event {
	return w.events
}

// Run starts the watch loop. Blocks until ctx is cancelled or Stop is called.
//
// Single-goroutine scheduler: one loop owns all polling and one timer. The
// listen goroutine (if present) only forwards notifications into a 1-buffered
// channel. hwm is never accessed concurrently.
func (w *Watcher) Run(ctx context.Context) error {
	defer close(w.events)

	if w.listenConn != nil {
		for _, bid := range w.cfg.BucketIDs {
			channel := fmt.Sprintf("resource_changes_b%d", bid)
			if _, err := w.listenConn.Exec(ctx, "LISTEN "+channel); err != nil {
				return fmt.Errorf("listen %s: %w", channel, err)
			}
		}
	}

	// Initial poll
	if err := w.poll(ctx); err != nil {
		return err
	}

	lastPoll := time.Now()
	doorbellPending := false

	timer := time.NewTimer(w.cfg.BaselineInterval)
	defer timer.Stop()

	// Doorbell listener goroutine — uses a child context so we can cancel it
	// when the main loop exits (e.g. on poll error), without waiting for the
	// caller's context to expire.
	listenCtx, listenCancel := context.WithCancel(ctx)
	defer listenCancel()

	doorbellCh := make(chan struct{}, 1)
	var listenWg sync.WaitGroup
	if w.listenConn != nil {
		listenWg.Add(1)
		go func() {
			defer listenWg.Done()
			w.listenLoop(listenCtx, doorbellCh)
		}()
	}

	var retErr error
	for {
		select {
		case <-ctx.Done():
			retErr = ctx.Err()
			goto shutdown
		case <-w.stopCh:
			goto shutdown

		case <-doorbellCh:
			sinceLastPoll := time.Since(lastPoll)
			if sinceLastPoll >= w.cfg.DebounceFloor {
				// Leading edge: poll immediately
				if err := w.poll(ctx); err != nil {
					retErr = err
					goto shutdown
				}
				lastPoll = time.Now()
				doorbellPending = false
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(w.cfg.BaselineInterval)
			} else {
				// Trailing edge: schedule poll at lastPoll + DebounceFloor
				doorbellPending = true
				remaining := w.cfg.DebounceFloor - sinceLastPoll
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(remaining)
			}

		case <-timer.C:
			if err := w.poll(ctx); err != nil {
				retErr = err
				goto shutdown
			}
			lastPoll = time.Now()
			doorbellPending = false
			_ = doorbellPending // used on next iteration
			timer.Reset(w.cfg.BaselineInterval)
		}
	}

shutdown:
	listenCancel()
	listenWg.Wait()
	return retErr
}

// Stop signals the watcher to shut down.
func (w *Watcher) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
}

func (w *Watcher) listenLoop(ctx context.Context, notify chan<- struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}

		_, err := w.listenConn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		select {
		case notify <- struct{}{}:
		default:
		}
	}
}

// poll runs one poll cycle inside a REPEATABLE READ read-only transaction.
// Epoch check, per-bucket horizon checks, and row queries all share the same
// snapshot — mid-poll compaction is invisible (B3 fix).
func (w *Watcher) poll(ctx context.Context) error {
	if w.hooks != nil {
		w.hooks.BeforePoll()
	}

	tx, err := w.pollConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("poll begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Epoch check (I6/R9 defense)
	var currentEpoch int64
	if err := tx.QueryRow(ctx, `SELECT timeline_id FROM cluster_epoch`).Scan(&currentEpoch); err != nil {
		return fmt.Errorf("poll epoch check: %w", err)
	}
	if w.cfg.StartRV.Epoch != 0 && currentEpoch != w.cfg.StartRV.Epoch {
		return fmt.Errorf("epoch mismatch (have=%d, db=%d): %w",
			w.cfg.StartRV.Epoch, currentEpoch, ErrGone)
	}

	var allEvents []Event

	for _, bid := range w.cfg.BucketIDs {
		events, err := w.pollBucket(ctx, tx, bid)
		if err != nil {
			return err
		}
		allEvents = append(allEvents, events...)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("poll commit: %w", err)
	}

	for _, ev := range allEvents {
		select {
		case w.events <- ev:
		case <-ctx.Done():
			return ctx.Err()
		case <-w.stopCh:
			return nil
		}
	}

	if w.hooks != nil {
		w.hooks.AfterPoll(allEvents)
	}

	return nil
}

func (w *Watcher) pollBucket(ctx context.Context, tx pgx.Tx, bucketID int) ([]Event, error) {
	hwm := w.hwm[bucketID]

	var compactedSeq *int64
	err := tx.QueryRow(ctx, `
		SELECT compacted_seq FROM compaction_horizon
		WHERE bucket_id = $1 AND gvk = $2`,
		bucketID, w.cfg.GVK).Scan(&compactedSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("poll compaction check: %w", err)
	}
	if compactedSeq != nil && hwm < *compactedSeq {
		return nil, fmt.Errorf("bucket %d: %w (hwm=%d < compacted=%d)",
			bucketID, ErrGone, hwm, *compactedSeq)
	}

	if w.hooks != nil {
		if h, ok := w.hooks.(WatchHooksWithHorizon); ok {
			h.AfterHorizonCheck(bucketID)
		}
	}

	rows, err := tx.Query(ctx, `
		SELECT gvk, namespace, name, uid, bucket_id, gvk_bucket_seq,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND bucket_id = $2 AND gvk_bucket_seq > $3
		ORDER BY gvk_bucket_seq ASC`,
		w.cfg.GVK, bucketID, hwm)
	if err != nil {
		return nil, fmt.Errorf("poll bucket %d: %w", bucketID, err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var r model.Resource
		if err := rows.Scan(
			&r.GVK, &r.Namespace, &r.Name, &r.UID, &r.BucketID,
			&r.GVKBucketSeq, &r.ObjectVersion, &r.Spec, &r.Status,
			&r.Metadata, &r.DeletionTimestamp, &r.CreatedAt, &r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("poll scan: %w", err)
		}

		var evType EventType
		if r.DeletionTimestamp != nil {
			evType = EventDeleted
		} else if r.ObjectVersion == 1 {
			evType = EventAdded
		} else {
			evType = EventModified
		}

		events = append(events, Event{Type: evType, Resource: r})
		w.hwm[bucketID] = r.GVKBucketSeq
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("poll rows: %w", err)
	}

	return events, nil
}

// HWM returns the current high-water marks for testing/inspection.
func (w *Watcher) HWM() map[int]int64 {
	out := make(map[int]int64, len(w.hwm))
	for k, v := range w.hwm {
		out[k] = v
	}
	return out
}
