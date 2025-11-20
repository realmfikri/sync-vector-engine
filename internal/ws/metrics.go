package ws

import (
        "sync"

        "github.com/prometheus/client_golang/prometheus"
        "go.opentelemetry.io/otel"
)

var (
        gatewayUpgradeLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
                Namespace: "gateway",
                Name:      "upgrade_seconds",
                Help:      "Latency spent upgrading HTTP connections to WebSockets.",
                Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
        }, []string{"document"})

        gatewayConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
                Namespace: "gateway",
                Name:      "connections",
                Help:      "Active WebSocket connections per document.",
        }, []string{"document"})

        gatewaySendQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
                Namespace: "gateway",
                Name:      "send_queue_depth",
                Help:      "Buffered outbound frames per document.",
        }, []string{"document"})

        once sync.Once
)

func init() {
        once.Do(func() {
                prometheus.MustRegister(gatewayUpgradeLatency, gatewayConnections, gatewaySendQueueDepth)
        })
}

var tracer = otel.Tracer("github.com/example/sync-vector-engine/ws")
