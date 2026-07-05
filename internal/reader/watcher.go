package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/metrics"
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

	// ListenConnFactory, when set, lets the watcher replace a failed LISTEN
	// connection. Called after backoff; the watcher re-LISTENs all bucket
	// channels on the new connection and requests one immediate catch-up poll.
	// When nil, a failed listen connection degrades to baseline-poll-only.
	ListenConnFactory func(ctx context.Context) (*pgx.Conn, error)
}

// WatchStats holds doorbell health counters. Readable via Stats() from any
// goroutine; incremented via sync/atomic.
type WatchStats struct {
	ListenErrors    int64 // WaitForNotification failures
	Reconnects      int64 // successful ListenConnFactory reconnects
	DoorbellPolls   int64 // polls triggered by a doorbell (leading or trailing)
	BaselinePolls   int64 // polls triggered by the baseline timer
	BaselineCatches int64 // baseline polls that delivered events while LISTEN was configured
}

type Watcher struct {
	cfg        WatcherConfig
	pollConn   *pgx.Conn
	listenConn *pgx.Conn
	events     chan Event
	hwm        map[int]int64 // per-bucket high-water mark; only touched from the Run goroutine
	stopCh     chan struct{}
	hooks      WatchHooks
	stats      WatchStats
	metrics    *metrics.WatcherMetrics
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

// WithMetrics attaches Prometheus metrics to the watcher.
func (w *Watcher) WithMetrics(m *metrics.WatcherMetrics) *Watcher {
	w.metrics = m
	return w
}

// Events returns the channel on which watch events are delivered.
func (w *Watcher) Events() <-chan Event {
	return w.events
}

// Stats returns a snapshot of the doorbell health counters.
func (w *Watcher) Stats() WatchStats {
	return WatchStats{
		ListenErrors:    atomic.LoadInt64(&w.stats.ListenErrors),
		Reconnects:      atomic.LoadInt64(&w.stats.Reconnects),
		DoorbellPolls:   atomic.LoadInt64(&w.stats.DoorbellPolls),
		BaselinePolls:   atomic.LoadInt64(&w.stats.BaselinePolls),
		BaselineCatches: atomic.LoadInt64(&w.stats.BaselineCatches),
	}
}

// Run starts the watch loop. Blocks until ctx is cancelled or Stop is called.
//
// Single-goroutine scheduler: one loop owns all polling and one timer. The
// listen goroutine (if present) only forwards notifications into a 1-buffered
// channel. hwm is never accessed concurrently.
func (w *Watcher) Run(ctx context.Context) error {
	defer close(w.events)

	// Initial poll
	if _, err := w.poll(ctx); err != nil {
		return err
	}

	lastPoll := time.Now()
	doorbellPending := false
	listenConfigured := w.listenConn != nil || w.cfg.ListenConnFactory != nil

	timer := time.NewTimer(w.cfg.BaselineInterval)
	defer timer.Stop()

	// Doorbell listener goroutine — uses a child context so we can cancel it
	// when the main loop exits (e.g. on poll error), without waiting for the
	// caller's context to expire.
	listenCtx, listenCancel := context.WithCancel(ctx)
	defer listenCancel()

	doorbellCh := make(chan struct{}, 1)
	var listenWg sync.WaitGroup
	if listenConfigured {
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
				atomic.AddInt64(&w.stats.DoorbellPolls, 1)
				if w.metrics != nil {
					w.metrics.DoorbellPollsTotal.Inc()
				}
				start := time.Now()
				n, err := w.poll(ctx)
				if w.metrics != nil {
					w.metrics.PollDuration.WithLabelValues(w.cfg.GVK).Observe(time.Since(start).Seconds())
					w.metrics.PollEventsDelivered.WithLabelValues(w.cfg.GVK).Observe(float64(n))
				}
				if err != nil {
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
			wasDoorbell := doorbellPending
			start := time.Now()
			n, err := w.poll(ctx)
			if w.metrics != nil {
				w.metrics.PollDuration.WithLabelValues(w.cfg.GVK).Observe(time.Since(start).Seconds())
				w.metrics.PollEventsDelivered.WithLabelValues(w.cfg.GVK).Observe(float64(n))
			}
			if err != nil {
				retErr = err
				goto shutdown
			}
			if wasDoorbell {
				atomic.AddInt64(&w.stats.DoorbellPolls, 1)
				if w.metrics != nil {
					w.metrics.DoorbellPollsTotal.Inc()
				}
			} else {
				atomic.AddInt64(&w.stats.BaselinePolls, 1)
				if w.metrics != nil {
					w.metrics.BaselinePollsTotal.Inc()
				}
				if n > 0 && listenConfigured {
					atomic.AddInt64(&w.stats.BaselineCatches, 1)
					if w.metrics != nil {
						w.metrics.BaselineCatchesTotal.Inc()
					}
				}
			}
			lastPoll = time.Now()
			doorbellPending = false
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
	conn := w.listenConn
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	// If we start with a non-nil conn, LISTEN on it.
	if conn != nil {
		if err := w.listenAll(ctx, conn); err != nil {
			conn.Close(context.Background())
			conn = nil
			atomic.AddInt64(&w.stats.ListenErrors, 1)
			if w.metrics != nil {
				w.metrics.ListenErrorsTotal.Inc()
			}
		}
	}

	for {
		if conn == nil {
			if w.cfg.ListenConnFactory == nil {
				return
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			}

			newConn, err := w.cfg.ListenConnFactory(ctx)
			if err != nil {
				atomic.AddInt64(&w.stats.ListenErrors, 1)
				if w.metrics != nil {
					w.metrics.ListenErrorsTotal.Inc()
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			}
			if err := w.listenAll(ctx, newConn); err != nil {
				newConn.Close(context.Background())
				atomic.AddInt64(&w.stats.ListenErrors, 1)
				if w.metrics != nil {
					w.metrics.ListenErrorsTotal.Inc()
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			}
			conn = newConn
			atomic.AddInt64(&w.stats.Reconnects, 1)
			if w.metrics != nil {
				w.metrics.ReconnectsTotal.Inc()
			}
			backoff = 100 * time.Millisecond
			// Nudge the main loop to catch up on missed notifications
			select {
			case notify <- struct{}{}:
			default:
			}
			continue
		}

		_, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-w.stopCh:
				return
			default:
			}
			atomic.AddInt64(&w.stats.ListenErrors, 1)
			if w.metrics != nil {
				w.metrics.ListenErrorsTotal.Inc()
			}
			conn.Close(context.Background())
			conn = nil
			continue
		}

		backoff = 100 * time.Millisecond

		select {
		case notify <- struct{}{}:
		default:
		}
	}
}

// listenAll issues LISTEN for all configured bucket channels on the given conn.
func (w *Watcher) listenAll(ctx context.Context, conn *pgx.Conn) error {
	for _, bid := range w.cfg.BucketIDs {
		channel := fmt.Sprintf("resource_changes_b%d", bid)
		if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
			return fmt.Errorf("listen %s: %w", channel, err)
		}
	}
	return nil
}

// poll runs one poll cycle inside a REPEATABLE READ read-only transaction.
// Epoch check, per-bucket horizon checks, and row queries all share the same
// snapshot — mid-poll compaction is invisible (B3 fix).
// Returns the number of events delivered.
func (w *Watcher) poll(ctx context.Context) (int, error) {
	if w.hooks != nil {
		w.hooks.BeforePoll()
	}

	tx, err := w.pollConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("poll begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Epoch check (I6/R9 defense)
	var currentEpoch int64
	if err := tx.QueryRow(ctx, `SELECT timeline_id FROM cluster_epoch`).Scan(&currentEpoch); err != nil {
		return 0, fmt.Errorf("poll epoch check: %w", err)
	}
	if w.cfg.StartRV.Epoch != 0 && currentEpoch != w.cfg.StartRV.Epoch {
		return 0, fmt.Errorf("epoch mismatch (have=%d, db=%d): %w",
			w.cfg.StartRV.Epoch, currentEpoch, ErrGone)
	}

	var allEvents []Event

	for _, bid := range w.cfg.BucketIDs {
		events, err := w.pollBucket(ctx, tx, bid)
		if err != nil {
			return 0, err
		}
		allEvents = append(allEvents, events...)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("poll commit: %w", err)
	}

	for _, ev := range allEvents {
		select {
		case w.events <- ev:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-w.stopCh:
			return 0, nil
		}
	}

	if w.hooks != nil {
		w.hooks.AfterPoll(allEvents)
	}

	return len(allEvents), nil
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
