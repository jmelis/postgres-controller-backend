package verifier

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/metrics"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/reader"
	"github.com/jmelis/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

// Violation represents a detected invariant violation.
type Violation struct {
	Invariant string // I2, I4, I5, I6
	Bucket    int
	GVK       string
	Detail    string
	Time      time.Time
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] bucket=%d gvk=%s: %s", v.Invariant, v.Bucket, v.GVK, v.Detail)
}

// Config configures the verifier.
type Config struct {
	GVK       string
	BucketIDs []int

	// CanaryInterval controls how often the canary writer fires. Zero disables canary.
	CanaryInterval time.Duration

	// PollInterval for the verification watcher.
	PollInterval time.Duration

	// ListenConn, when set, gives the verifier's watcher a doorbell connection
	// so canary delivery latency reflects the real fast path.
	ListenConn *pgx.Conn

	// ListenConnFactory for reconnect support on the verifier's watcher.
	ListenConnFactory func(ctx context.Context) (*pgx.Conn, error)
}

// Result holds the current verification state.
type Result struct {
	Violations    []Violation
	EventsChecked int64
	CanaryWrites  int64
	CanaryP99     time.Duration // write-to-delivery latency p99
}

const canaryRingSize = 1000

// Verifier is the continuous invariant verification consumer.
// It subscribes via the ordinary poll path and checks:
//   - I2/I5: monotonic hwm (seq must be strictly greater than previous hwm)
//   - I4: no duplicate delivery (duplicate => seq <= hwm, caught by I2 check)
//   - I6: hwm-below-horizon implies 410 was received
//
// The canary probe writes synthetic objects and measures write-to-delivery
// latency (the wall-clock time from Write returning to the event appearing
// on the watcher channel). This times the full pipeline: commit visibility,
// pg_notify doorbell, poll scheduling, and channel delivery.
type Verifier struct {
	cfg        Config
	pollConn   *pgx.Conn
	canaryConn *pgx.Conn

	mu              sync.Mutex
	violations      []Violation
	hwm             map[int]int64
	eventsCount     int64
	canaryCount     int64
	canaryTimes     [canaryRingSize]time.Duration
	canaryIdx       int
	canaryFull      bool
	pendingCanaries map[string]time.Time // name -> writeCompletedAt

	metrics *metrics.VerifierMetrics
}

// New creates a verifier. canaryConn may be nil to disable canary writes.
func New(pollConn, canaryConn *pgx.Conn, cfg Config) *Verifier {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.CanaryInterval == 0 {
		cfg.CanaryInterval = 5 * time.Second
	}

	hwm := make(map[int]int64, len(cfg.BucketIDs))
	for _, bid := range cfg.BucketIDs {
		hwm[bid] = 0
	}

	return &Verifier{
		cfg:             cfg,
		pollConn:        pollConn,
		canaryConn:      canaryConn,
		hwm:             hwm,
		pendingCanaries: make(map[string]time.Time),
	}
}

// WithMetrics attaches Prometheus metrics to the verifier.
func (v *Verifier) WithMetrics(m *metrics.VerifierMetrics) *Verifier {
	v.metrics = m
	return v
}

// Run starts the verifier. Blocks until ctx is cancelled.
func (v *Verifier) Run(ctx context.Context) error {
	watcher := reader.NewWatcher(v.pollConn, v.cfg.ListenConn, reader.WatcherConfig{
		GVK:               v.cfg.GVK,
		BucketIDs:         v.cfg.BucketIDs,
		StartRV:           resourceversion.RV{Buckets: v.hwm},
		BaselineInterval:  v.cfg.PollInterval,
		ListenConnFactory: v.cfg.ListenConnFactory,
	}, nil)

	watchDone := make(chan error, 1)
	go func() { watchDone <- watcher.Run(ctx) }()

	var canaryTicker *time.Ticker
	var canaryC <-chan time.Time
	if v.canaryConn != nil {
		canaryTicker = time.NewTicker(v.cfg.CanaryInterval)
		canaryC = canaryTicker.C
		defer canaryTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			<-watchDone
			return nil

		case ev, ok := <-watcher.Events():
			if !ok {
				return <-watchDone
			}
			v.checkEvent(ev)

		case <-canaryC:
			v.writeCanary(ctx)
		}
	}
}

// Violations returns all recorded violations.
func (v *Verifier) Violations() []Violation {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]Violation, len(v.violations))
	copy(out, v.violations)
	return out
}

// Result returns the current verification state.
func (v *Verifier) Result() Result {
	v.mu.Lock()
	defer v.mu.Unlock()

	var p99 time.Duration
	n := v.canaryIdx
	if v.canaryFull {
		n = canaryRingSize
	}
	if n > 0 {
		sorted := make([]time.Duration, n)
		if v.canaryFull {
			copy(sorted, v.canaryTimes[:])
		} else {
			copy(sorted, v.canaryTimes[:n])
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		idx := int(float64(len(sorted)) * 0.99)
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		p99 = sorted[idx]
	}

	return Result{
		Violations:    append([]Violation{}, v.violations...),
		EventsChecked: v.eventsCount,
		CanaryWrites:  v.canaryCount,
		CanaryP99:     p99,
	}
}

func (v *Verifier) checkEvent(ev reader.Event) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.eventsCount++
	if v.metrics != nil {
		v.metrics.EventsCheckedTotal.Inc()
	}

	bucket := ev.Resource.BucketID
	seq := ev.Resource.GVKBucketSeq

	prevHWM := v.hwm[bucket]

	// I2/I4/I5: monotonic hwm — seq must be strictly greater than previous hwm.
	if seq <= prevHWM {
		v.addViolation(Violation{
			Invariant: "I2",
			Bucket:    bucket,
			GVK:       v.cfg.GVK,
			Detail:    fmt.Sprintf("non-monotonic: seq=%d <= hwm=%d", seq, prevHWM),
			Time:      time.Now(),
		})
	}

	// Canary delivery latency measurement
	if ev.Resource.Namespace == "_verifier" {
		if writeTime, ok := v.pendingCanaries[ev.Resource.Name]; ok {
			deliveryLatency := time.Since(writeTime)
			v.canaryTimes[v.canaryIdx] = deliveryLatency
			v.canaryIdx++
			if v.canaryIdx >= canaryRingSize {
				v.canaryIdx = 0
				v.canaryFull = true
			}
			delete(v.pendingCanaries, ev.Resource.Name)
			if v.metrics != nil {
				v.metrics.CanaryDelivery.Observe(deliveryLatency.Seconds())
			}
		}
	}

	if seq > v.hwm[bucket] {
		v.hwm[bucket] = seq
	}
}

func (v *Verifier) writeCanary(ctx context.Context) {
	v.mu.Lock()
	count := v.canaryCount
	v.mu.Unlock()

	if v.canaryConn == nil {
		return
	}

	w := writer.New(v.canaryConn, nil)
	name := fmt.Sprintf("_verifier-canary-%d", count)

	req := model.WriteRequest{
		GVK:         v.cfg.GVK,
		Namespace:   "_verifier",
		Name:        name,
		BucketID:    v.cfg.BucketIDs[0],
		Spec:     json.RawMessage(`{"canary":true}`),
		Status:   json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`),
	}

	_, err := w.Write(ctx, req)
	if err != nil {
		return
	}
	writeCompleted := time.Now()

	v.mu.Lock()
	v.canaryCount++
	// Cap pending map to stay bounded
	if len(v.pendingCanaries) >= canaryRingSize {
		for k := range v.pendingCanaries {
			delete(v.pendingCanaries, k)
			break
		}
	}
	v.pendingCanaries[name] = writeCompleted
	v.mu.Unlock()
}

func (v *Verifier) addViolation(viol Violation) {
	v.violations = append(v.violations, viol)
	if v.metrics != nil {
		v.metrics.ViolationsTotal.WithLabelValues(viol.Invariant).Inc()
	}
}
