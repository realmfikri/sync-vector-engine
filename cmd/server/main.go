package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/example/sync-vector-engine/internal/config"
	"github.com/example/sync-vector-engine/internal/crdt"
	"github.com/example/sync-vector-engine/internal/observability"
	"github.com/example/sync-vector-engine/internal/playback"
	"github.com/example/sync-vector-engine/internal/snapshot"
	"github.com/example/sync-vector-engine/internal/storage"
	"github.com/example/sync-vector-engine/internal/types"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load configuration")
	}

	logger := log.With().Str("app", cfg.AppName).Logger()
	observability.RegisterRuntimeCollectors()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	telemetryShutdown, err := observability.Start(ctx, observability.Config{
		ServiceName:  cfg.AppName,
		MetricsAddr:  cfg.MetricsAddr,
		OTLPEndpoint: cfg.OTLPEndpoint,
	}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize telemetry")
	}
	defer telemetryShutdown(context.Background())

	resources, err := config.NewResources(ctx, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize resources")
	}
	defer resources.Close()

	wal := storage.NewWAL(resources.Postgres)
	engine := crdt.NewEngine(cfg.AppName, logger)
	snapshotWorker := snapshot.NewWorker(wal, engine, resources.Object, cfg.ObjectBucket, logger)

	if err := replayWAL(ctx, wal, engine, resources.Object, cfg.ObjectBucket, logger); err != nil {
		logger.Fatal().Err(err).Msg("failed to replay WAL")
	}

	go checkpointLoop(ctx, wal, engine, logger, cfg.HealthcheckProbe)
	snapshotWorker.Start(ctx)

	playbackSvc := playback.NewService(wal, cfg.ObjectBucket, playback.NewObjectLoader(resources.Object), logger, playback.ServiceConfig{})
	playbackHandler := playback.NewHTTPHandler(playbackSvc, logger)
	httpServer := &http.Server{Addr: cfg.HTTPListenAddr, Handler: playbackHandler}

	go func() {
		logger.Info().Str("addr", cfg.HTTPListenAddr).Msg("http server starting")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("http server failed")
		}
	}()

	logger.Info().Msg("server dependencies initialized")

	go func() {
		ticker := time.NewTicker(cfg.HealthcheckProbe)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := resources.HealthCheck(context.Background()); err != nil {
					logger.Error().Err(err).Msg("dependency healthcheck failed")
				} else {
					logger.Debug().Msg("dependency healthcheck ok")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		resources.Close()
		close(done)
	}()

	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	select {
	case <-done:
		logger.Info().Msg("shutdown complete")
	case <-shutdownCtx.Done():
		logger.Error().Err(shutdownCtx.Err()).Msg("forced shutdown")
	}
}

func replayWAL(ctx context.Context, wal *storage.WAL, engine *crdt.Engine, object *minio.Client, bucket string, logger zerolog.Logger) error {
	docs, err := wal.ActiveDocuments(ctx)
	if err != nil {
		return fmt.Errorf("list active wal documents: %w", err)
	}

	for _, docID := range docs {
		checkpoint, err := wal.LastCheckpoint(ctx, docID)
		if err != nil {
			return fmt.Errorf("read checkpoint for %s: %w", docID, err)
		}

		startLSN := checkpoint
		snapshotLSN, err := restoreFromSnapshot(ctx, wal, engine, object, bucket, docID, logger)
		if err != nil {
			logger.Error().Err(err).Str("document", string(docID)).Msg("failed to restore snapshot; continuing from checkpoint")
		} else if snapshotLSN > startLSN {
			startLSN = snapshotLSN
		}

		if err := wal.ReplayDocument(ctx, docID, startLSN, func(record types.WALRecord) error {
			return engine.ApplyWAL(record)
		}); err != nil {
			return fmt.Errorf("replay document %s: %w", docID, err)
		}

		if last := engine.LastLSN(docID); last > 0 {
			if err := wal.RecordCheckpoint(ctx, docID, last); err != nil {
				logger.Error().Err(err).Str("document", string(docID)).Msg("checkpoint after replay failed")
			}
		}
	}

	return nil
}

func restoreFromSnapshot(ctx context.Context, wal *storage.WAL, engine *crdt.Engine, object *minio.Client, bucket string, docID types.DocumentID, logger zerolog.Logger) (int64, error) {
	if object == nil {
		return 0, nil
	}

	ref, err := wal.LatestSnapshot(ctx, docID)
	if err != nil {
		return 0, fmt.Errorf("lookup snapshot: %w", err)
	}
	if ref.OperationID == "" || ref.ObjectPath == "" {
		return 0, nil
	}

	obj, err := object.GetObject(ctx, bucket, ref.ObjectPath, minio.GetObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("get snapshot object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return 0, fmt.Errorf("read snapshot object: %w", err)
	}

	payload, err := snapshot.DecodePayload(data)
	if err != nil {
		return 0, fmt.Errorf("decode snapshot: %w", err)
	}
	if payload.Document != "" && payload.Document != docID {
		logger.Warn().Str("document", string(docID)).Str("snapshot_doc", string(payload.Document)).Msg("snapshot document mismatch")
	}

	engine.Restore(docID, payload.Nodes, payload.VectorClock.Clone(), payload.LastOpID, ref.LastLSN)
	logger.Info().Str("document", string(docID)).Str("op_id", string(ref.OperationID)).Msg("restored snapshot")

	return ref.LastLSN, nil
}

func checkpointLoop(ctx context.Context, wal *storage.WAL, engine *crdt.Engine, logger zerolog.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, docID := range engine.Documents() {
				lsn := engine.LastLSN(docID)
				if lsn == 0 {
					continue
				}
				if err := wal.RecordCheckpoint(ctx, docID, lsn); err != nil {
					logger.Error().Err(err).Str("document", string(docID)).Msg("failed to persist checkpoint")
					continue
				}
				if backlog, err := wal.OperationCountAfterLSN(ctx, docID, lsn); err == nil {
					wal.RecordBacklogMetric(docID, backlog)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
