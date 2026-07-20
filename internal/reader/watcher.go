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
	StartRV          resourceversion.RV
	BaselineInterval time.Duration // default 5s
	DebounceFloor    time.Duration // default 100ms
	Shard            *ShardSpec    // nil = unsharded (all rows)

	// ListenConnFactory, when set, lets the watcher replace a failed LISTEN
	// connection. Called after backoff; the watcher re-LISTENs the GVK
	// channel on the new connection and requests one immediate catch-up poll.
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
	hwm        uint64 // high-water mark; only touched from the Run goroutine
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
// between the compaction horizon check and the row query in pollAll.
type WatchHooksWithHorizon interface {
	AfterHorizonCheck()
}

func NewWatcher(pollConn, listenConn *pgx.Conn, cfg WatcherConfig, hooks WatchHooks) *Watcher {
	if cfg.BaselineInterval == 0 {
		cfg.BaselineInterval = 5 * time.Second
	}
	if cfg.DebounceFloor == 0 {
		cfg.DebounceFloor = 100 * time.Millisecond
	}

	return &Watcher{
		cfg:        cfg,
		pollConn:   pollConn,
		listenConn: listenConn,
		events:     make(chan Event, 256),
		hwm:        cfg.StartRV.Watermark,
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
func (w *Watcher) Run(ctx context.Context) error {
	defer close(w.events)

	if _, err := w.poll(ctx); err != nil {
		return err
	}

	lastPoll := time.Now()
	doorbellPending := false
	listenConfigured := w.listenConn != nil || w.cfg.ListenConnFactory != nil

	timer := time.NewTimer(w.cfg.BaselineInterval)
	defer timer.Stop()

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

// listenAll issues LISTEN for the GVK channel on the given conn.
func (w *Watcher) listenAll(ctx context.Context, conn *pgx.Conn) error {
	channel := model.DoorbellChannel(w.cfg.GVK)
	if _, err := conn.Exec(ctx, fmt.Sprintf(`LISTEN "%s"`, channel)); err != nil {
		return fmt.Errorf("listen %s: %w", channel, err)
	}
	return nil
}

// poll runs one poll cycle inside a REPEATABLE READ read-only transaction.
// The xmin watermark and row query share the same snapshot — mid-poll
// compaction is invisible.
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

	events, err := w.pollAll(ctx, tx)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("poll commit: %w", err)
	}

	for _, ev := range events {
		select {
		case w.events <- ev:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-w.stopCh:
			return 0, nil
		}
	}

	if w.hooks != nil {
		w.hooks.AfterPoll(events)
	}

	return len(events), nil
}

func (w *Watcher) pollAll(ctx context.Context, tx pgx.Tx) ([]Event, error) {
	// Get xmin watermark from the same snapshot
	var xmin uint64
	err := tx.QueryRow(ctx, `SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint`).Scan(&xmin)
	if err != nil {
		return nil, fmt.Errorf("poll xmin: %w", err)
	}

	// Compaction horizon check
	var compactedXid *int64
	err = tx.QueryRow(ctx, `
		SELECT compacted_xid FROM compaction_horizon
		WHERE gvk = $1`, w.cfg.GVK).Scan(&compactedXid)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("poll compaction check: %w", err)
	}
	if compactedXid != nil && w.hwm < uint64(*compactedXid) {
		return nil, fmt.Errorf("%w (hwm=%d < compacted=%d)",
			ErrGone, w.hwm, *compactedXid)
	}

	if w.hooks != nil {
		if h, ok := w.hooks.(WatchHooksWithHorizon); ok {
			h.AfterHorizonCheck()
		}
	}

	query := `
		SELECT gvk, namespace, name, uid, txid_stamp::text::bigint,
		       object_version, spec, status, metadata,
		       deletion_timestamp, created_at, updated_at
		FROM kubernetes_resources
		WHERE gvk = $1 AND txid_stamp::text::bigint > $2`
	args := []any{w.cfg.GVK, w.hwm}
	if w.cfg.Shard != nil {
		query, args = w.cfg.Shard.AppendQuery(query, args)
	}
	query += " ORDER BY txid_stamp ASC"

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("poll: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var r model.Resource
		if err := rows.Scan(
			&r.GVK, &r.Namespace, &r.Name, &r.UID,
			&r.TxidStamp, &r.ObjectVersion, &r.Spec, &r.Status,
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
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("poll rows: %w", err)
	}

	// Advance HWM to xmin-1 (safe watermark), NOT to max seen txid.
	// Everything <= xmin-1 is committed; rows between hwm and xmin-1
	// may be re-scanned on the next poll but are deduped by the informer cache.
	if xmin > 0 && xmin-1 > w.hwm {
		w.hwm = xmin - 1
	}

	return events, nil
}

// HWM returns the current high-water mark for testing/inspection.
func (w *Watcher) HWM() uint64 {
	return w.hwm
}
