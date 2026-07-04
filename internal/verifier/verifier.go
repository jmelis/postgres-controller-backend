package verifier

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/model"
	"github.com/jmelisba/postgres-controller-backend/internal/reader"
	"github.com/jmelisba/postgres-controller-backend/internal/resourceversion"
	"github.com/jmelisba/postgres-controller-backend/internal/writer"
)

// Violation represents a detected invariant violation.
type Violation struct {
	Invariant string // I1, I3, I4, I5, I6, I7
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

	// CanaryHolder and CanaryEpoch for the canary write's lease.
	CanaryHolder string
	CanaryEpoch  int64

	// PollInterval for the verification watcher.
	PollInterval time.Duration
}

// Result holds the current verification state.
type Result struct {
	Violations    []Violation
	EventsChecked int64
	CanaryWrites  int64
	CanaryP99     time.Duration
}

// Verifier is the continuous invariant verification consumer from §6.
// It subscribes via the ordinary poll path and checks:
//   - I1: per-(GVK, bucket) seq contiguity
//   - I3/I6: monotonic hwm
//   - I5: no duplicate (object, seq) deliveries
//   - I7: all gaps explained by compaction horizon
//
// Optionally writes a synthetic canary object to measure write→delivery latency.
type Verifier struct {
	cfg        Config
	pollConn   *pgx.Conn
	canaryConn *pgx.Conn

	mu          sync.Mutex
	violations  []Violation
	seenKeys    map[string]bool // "bucket:seq" → seen
	hwm         map[int]int64
	eventsCount int64
	canaryCount int64
	canaryTimes []time.Duration
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
		cfg:        cfg,
		pollConn:   pollConn,
		canaryConn: canaryConn,
		seenKeys:   make(map[string]bool),
		hwm:        hwm,
	}
}

// Run starts the verifier. Blocks until ctx is cancelled.
// Returns nil on clean shutdown (context cancelled), or the first fatal error.
func (v *Verifier) Run(ctx context.Context) error {
	watcher := reader.NewWatcher(v.pollConn, nil, reader.WatcherConfig{
		GVK:              v.cfg.GVK,
		BucketIDs:        v.cfg.BucketIDs,
		StartRV:          resourceversion.RV{Epoch: 1, Buckets: v.hwm},
		BaselineInterval: v.cfg.PollInterval,
	}, nil)

	watchDone := make(chan error, 1)
	go func() { watchDone <- watcher.Run(ctx) }()

	var canaryTicker *time.Ticker
	var canaryC <-chan time.Time
	if v.canaryConn != nil && v.cfg.CanaryHolder != "" {
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
	if len(v.canaryTimes) > 0 {
		sorted := make([]time.Duration, len(v.canaryTimes))
		copy(sorted, v.canaryTimes)
		sortDurations(sorted)
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
	bucket := ev.Resource.BucketID
	seq := ev.Resource.GVKBucketSeq

	// I5: no duplicate (object, seq)
	key := fmt.Sprintf("%d:%d", bucket, seq)
	if v.seenKeys[key] {
		v.addViolation(Violation{
			Invariant: "I5",
			Bucket:    bucket,
			GVK:       v.cfg.GVK,
			Detail:    fmt.Sprintf("duplicate delivery: seq=%d", seq),
			Time:      time.Now(),
		})
		return
	}
	v.seenKeys[key] = true

	// I1: seq contiguity — seq must be exactly hwm+1 or explainable by compaction
	prevHWM := v.hwm[bucket]
	if seq != prevHWM+1 && seq > prevHWM+1 {
		// Gap detected — check if compaction explains it
		var compactedSeq *int64
		err := v.pollConn.QueryRow(context.Background(),
			`SELECT compacted_seq FROM compaction_horizon WHERE bucket_id = $1 AND gvk = $2`,
			bucket, v.cfg.GVK).Scan(&compactedSeq)
		if err == nil && compactedSeq != nil && *compactedSeq >= prevHWM+1 {
			// I7: gap explained by compaction — OK
		} else {
			v.addViolation(Violation{
				Invariant: "I1",
				Bucket:    bucket,
				GVK:       v.cfg.GVK,
				Detail:    fmt.Sprintf("seq gap: expected %d, got %d (compaction does not explain)", prevHWM+1, seq),
				Time:      time.Now(),
			})
		}
	}

	// I3/I6: monotonic hwm — seq must be > previous hwm
	if seq <= prevHWM {
		v.addViolation(Violation{
			Invariant: "I3",
			Bucket:    bucket,
			GVK:       v.cfg.GVK,
			Detail:    fmt.Sprintf("non-monotonic: seq=%d <= hwm=%d", seq, prevHWM),
			Time:      time.Now(),
		})
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
		Spec:        json.RawMessage(`{"canary":true}`),
		Status:      json.RawMessage(`{}`),
		Metadata:    json.RawMessage(`{}`),
		LeaseHolder: v.cfg.CanaryHolder,
		LeaseEpoch:  v.cfg.CanaryEpoch,
	}

	start := time.Now()
	_, err := w.Write(ctx, req)
	latency := time.Since(start)

	if err != nil {
		return
	}

	v.mu.Lock()
	v.canaryCount++
	v.canaryTimes = append(v.canaryTimes, latency)
	v.mu.Unlock()
}

func (v *Verifier) addViolation(viol Violation) {
	v.violations = append(v.violations, viol)
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}
