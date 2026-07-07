package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jmelis/postgres-controller-backend/internal/metrics"
)

var (
	phaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "loadtest",
			Name:      "phase_duration_seconds",
			Help:      "Duration of each load test phase",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1s to ~145h
		},
		[]string{"phase"},
	)

	writesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadtest",
			Name:      "writes_total",
			Help:      "Total writes performed during load test",
		},
		[]string{"phase", "gvk", "bucket_id"},
	)

	errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadtest",
			Name:      "errors_total",
			Help:      "Total errors during load test",
		},
		[]string{"phase", "error_type"},
	)

	seedObjectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "loadtest",
			Name:      "seed_objects_total",
			Help:      "Total objects seeded",
		},
		[]string{"gvk"},
	)

	writeLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "loadtest",
			Name:      "write_latency_seconds",
			Help:      "Write latency during load test phases",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"phase", "gvk"},
	)

	deliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "loadtest",
			Name:      "delivery_latency_seconds",
			Help:      "Write-to-delivery latency for watchers",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"phase"},
	)
)

// Library metrics — registered once, wired into every writer/watcher/verifier instance.
var (
	libWriterMetrics   *metrics.WriterMetrics
	libWatcherMetrics  *metrics.WatcherMetrics
	libVerifierMetrics *metrics.VerifierMetrics
)

func init() {
	prometheus.MustRegister(
		phaseDuration,
		writesTotal,
		errorsTotal,
		seedObjectsTotal,
		writeLatency,
		deliveryLatency,
	)

	libWriterMetrics = metrics.NewWriterMetrics(prometheus.DefaultRegisterer)
	libWatcherMetrics = metrics.NewWatcherMetrics(prometheus.DefaultRegisterer)
	libVerifierMetrics = metrics.NewVerifierMetrics(prometheus.DefaultRegisterer)
}

// StartMetricsServer starts a Prometheus /metrics HTTP server in a background
// goroutine. It returns immediately.
func StartMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Printf("metrics server listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
}
