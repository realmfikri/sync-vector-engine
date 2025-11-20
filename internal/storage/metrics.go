package storage

import (
        "github.com/prometheus/client_golang/prometheus"
        "go.opentelemetry.io/otel"
)

var (
        walAppendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
                Namespace: "wal",
                Name:      "append_seconds",
                Help:      "Latency for appending operations to the WAL.",
                Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
        }, []string{"document"})

        walReplayLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
                Namespace: "wal",
                Name:      "replay_seconds",
                Help:      "Latency for replaying WAL batches per document.",
                Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
        }, []string{"document"})

        walBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
                Namespace: "wal",
                Name:      "backlog_entries",
                Help:      "Pending WAL entries beyond the last checkpoint per document.",
        }, []string{"document"})

        walTracer = otel.Tracer("github.com/example/sync-vector-engine/wal")
)

func init() {
        prometheus.MustRegister(walAppendLatency, walReplayLatency, walBacklog)
}
