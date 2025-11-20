package observability

import (
        "context"
        "net/http"
        "runtime"
        "time"

        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promhttp"
        "github.com/rs/zerolog"
        "go.opentelemetry.io/otel"
        "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
        "go.opentelemetry.io/otel/sdk/resource"
        sdktrace "go.opentelemetry.io/otel/sdk/trace"
        "go.opentelemetry.io/otel/trace"
        semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Config controls telemetry exporters and listeners.
type Config struct {
        ServiceName  string
        MetricsAddr  string
        OTLPEndpoint string
}

// Start configures Prometheus metrics and OpenTelemetry tracing. The returned
// shutdown function should be invoked during graceful shutdown.
func Start(ctx context.Context, cfg Config, logger zerolog.Logger) (func(context.Context) error, error) {
        var tracerProvider *sdktrace.TracerProvider
        if cfg.OTLPEndpoint != "" {
                exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint), otlptracegrpc.WithInsecure())
                if err != nil {
                        return nil, err
                }
                tracerProvider = sdktrace.NewTracerProvider(
                        sdktrace.WithBatcher(exporter),
                        sdktrace.WithResource(resource.NewWithAttributes(
                                semconv.SchemaURL,
                                semconv.ServiceName(cfg.ServiceName),
                        )),
                )
                otel.SetTracerProvider(tracerProvider)
                logger.Info().Str("endpoint", cfg.OTLPEndpoint).Msg("otlp tracing enabled")
        }

        var metricsSrv *http.Server
        if cfg.MetricsAddr != "" {
                mux := http.NewServeMux()
                mux.Handle("/metrics", promhttp.Handler())
                metricsSrv = &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
                go func() {
                        if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                                logger.Error().Err(err).Msg("metrics server failed")
                        }
                }()
                logger.Info().Str("addr", cfg.MetricsAddr).Msg("metrics server started")
        }

        shutdown := func(ctx context.Context) error {
                if metricsSrv != nil {
                        _ = metricsSrv.Shutdown(ctx)
                }
                var traceErr error
                if tracerProvider != nil {
                        traceErr = tracerProvider.Shutdown(ctx)
                }
                return traceErr
        }

        return shutdown, nil
}

// LoggerWithTrace attaches trace context to the provided logger when available.
func LoggerWithTrace(ctx context.Context, logger zerolog.Logger) zerolog.Logger {
        span := trace.SpanFromContext(ctx)
        spanCtx := span.SpanContext()
        if !spanCtx.IsValid() {
                return logger
        }
        return logger.With().Str("trace_id", spanCtx.TraceID().String()).Str("span_id", spanCtx.SpanID().String()).Logger()
}

// RegisterRuntimeCollectors exposes basic Go runtime metrics (goroutines and GC pause).
func RegisterRuntimeCollectors() {
        prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
                Namespace: "runtime",
                Name:      "goroutines",
                Help:      "Number of goroutines in the process.",
        }, func() float64 {
                return float64(runtime.NumGoroutine())
        }))

        prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
                Namespace: "runtime",
                Name:      "last_gc_pause_seconds",
                Help:      "Duration of the most recent GC pause.",
        }, func() float64 {
                var stats runtime.MemStats
                runtime.ReadMemStats(&stats)
                if len(stats.PauseNs) == 0 {
                        return 0
                }
                return float64(stats.PauseNs[(stats.NumGC+255)%256]) / float64(time.Second)
        }))
}
