package crdt

import "github.com/prometheus/client_golang/prometheus"

var (
        applyLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
                Namespace: "crdt",
                Name:      "apply_wal_seconds",
                Help:      "Time spent applying WAL records to CRDT stores.",
                Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
        }, []string{"document"})

        documentCount = prometheus.NewGauge(prometheus.GaugeOpts{
                Namespace: "crdt",
                Name:      "documents",
                Help:      "Number of CRDT documents loaded in memory.",
        })
)

func init() {
        prometheus.MustRegister(applyLatency, documentCount)
}
