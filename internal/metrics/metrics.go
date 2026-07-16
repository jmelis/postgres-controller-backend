package metrics

import "github.com/prometheus/client_golang/prometheus"

// WriterMetrics holds Prometheus metrics for the write path.
type WriterMetrics struct {
	WriteDuration         *prometheus.HistogramVec
	WriteStepDuration     *prometheus.HistogramVec
	WritesTotal           *prometheus.CounterVec
	NoopSuppressionsTotal  prometheus.Counter
	DoorbellErrorsTotal    prometheus.Counter
	StatementTimeoutsTotal prometheus.Counter
}

func NewWriterMetrics(reg prometheus.Registerer) *WriterMetrics {
	if reg == nil {
		return nil
	}
	m := &WriterMetrics{
		WriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "write_duration_seconds",
			Help:      "Duration of write operations.",
			Buckets:   []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0, 30.0},
		}, []string{"gvk", "result"}),
		WriteStepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "write_step_duration_seconds",
			Help:      "Duration of individual steps within a write transaction.",
			Buckets:   []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1},
		}, []string{"step"}),
		WritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "writes_total",
			Help:      "Total number of write operations.",
		}, []string{"gvk", "result"}),
		NoopSuppressionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "noop_suppressions_total",
			Help:      "Total number of writes suppressed due to identical content.",
		}),
		DoorbellErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "doorbell_errors_total",
			Help:      "Failed pg_notify doorbell sends (fire-and-forget, non-fatal).",
		}),
		StatementTimeoutsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "writer",
			Name:      "statement_timeouts_total",
			Help:      "Write transactions killed by statement_timeout — a stuck transaction pins xmin and freezes watcher HWM.",
		}),
	}
	reg.MustRegister(m.WriteDuration, m.WriteStepDuration, m.WritesTotal, m.NoopSuppressionsTotal, m.DoorbellErrorsTotal, m.StatementTimeoutsTotal)
	return m
}

// WatcherMetrics holds Prometheus metrics for the watch/poll path.
type WatcherMetrics struct {
	PollDuration         *prometheus.HistogramVec
	PollEventsDelivered  *prometheus.HistogramVec
	DoorbellPollsTotal   prometheus.Counter
	BaselinePollsTotal   prometheus.Counter
	BaselineCatchesTotal prometheus.Counter
	ListenErrorsTotal    prometheus.Counter
	ReconnectsTotal      prometheus.Counter
}

func NewWatcherMetrics(reg prometheus.Registerer) *WatcherMetrics {
	if reg == nil {
		return nil
	}
	m := &WatcherMetrics{
		PollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "poll_duration_seconds",
			Help:      "Duration of a single poll cycle.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		}, []string{"gvk"}),
		PollEventsDelivered: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "poll_events_delivered",
			Help:      "Number of events delivered per poll cycle.",
			Buckets:   []float64{0, 1, 5, 10, 25, 50, 100, 500},
		}, []string{"gvk"}),
		DoorbellPollsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "doorbell_polls_total",
			Help:      "Polls triggered by LISTEN doorbell notifications.",
		}),
		BaselinePollsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "baseline_polls_total",
			Help:      "Polls triggered by the baseline timer.",
		}),
		BaselineCatchesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "baseline_catches_total",
			Help:      "Baseline polls that delivered events while LISTEN was configured (missed notifications).",
		}),
		ListenErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "listen_errors_total",
			Help:      "WaitForNotification errors on the LISTEN connection.",
		}),
		ReconnectsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "watcher",
			Name:      "reconnects_total",
			Help:      "Successful LISTEN connection reconnects via ListenConnFactory.",
		}),
	}
	reg.MustRegister(
		m.PollDuration, m.PollEventsDelivered,
		m.DoorbellPollsTotal, m.BaselinePollsTotal, m.BaselineCatchesTotal,
		m.ListenErrorsTotal, m.ReconnectsTotal,
	)
	return m
}

// VerifierMetrics holds Prometheus metrics for the continuous verifier.
type VerifierMetrics struct {
	CanaryDelivery     prometheus.Histogram
	ViolationsTotal    *prometheus.CounterVec
	EventsCheckedTotal prometheus.Counter
}

func NewVerifierMetrics(reg prometheus.Registerer) *VerifierMetrics {
	if reg == nil {
		return nil
	}
	m := &VerifierMetrics{
		CanaryDelivery: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "verifier",
			Name:      "canary_delivery_seconds",
			Help:      "Write-to-delivery latency for canary objects.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),
		ViolationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "verifier",
			Name:      "violations_total",
			Help:      "Invariant violations detected by the verifier.",
		}, []string{"invariant"}),
		EventsCheckedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "verifier",
			Name:      "events_checked_total",
			Help:      "Total events processed by the verifier.",
		}),
	}
	reg.MustRegister(m.CanaryDelivery, m.ViolationsTotal, m.EventsCheckedTotal)
	return m
}
