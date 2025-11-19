package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/example/sync-vector-engine/internal/config"
	"github.com/example/sync-vector-engine/internal/crdt"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	resources, err := config.NewResources(ctx, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize resources")
	}
	defer resources.Close()

	wal := storage.NewWAL(resources.Postgres)
	engine := crdt.NewEngine(cfg.AppName, logger)

	if err := replayWAL(ctx, wal, engine, logger); err != nil {
		logger.Fatal().Err(err).Msg("failed to replay WAL")
	}

	go checkpointLoop(ctx, wal, engine, logger, cfg.HealthcheckProbe)

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

	select {
	case <-done:
		logger.Info().Msg("shutdown complete")
	case <-shutdownCtx.Done():
		logger.Error().Err(shutdownCtx.Err()).Msg("forced shutdown")
	}
}

func replayWAL(ctx context.Context, wal *storage.WAL, engine *crdt.Engine, logger zerolog.Logger) error {
	docs, err := wal.ActiveDocuments(ctx)
	if err != nil {
		return fmt.Errorf("list active wal documents: %w", err)
	}

	for _, docID := range docs {
		checkpoint, err := wal.LastCheckpoint(ctx, docID)
		if err != nil {
			return fmt.Errorf("read checkpoint for %s: %w", docID, err)
		}

		if err := wal.ReplayDocument(ctx, docID, checkpoint, func(record types.WALRecord) error {
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
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
