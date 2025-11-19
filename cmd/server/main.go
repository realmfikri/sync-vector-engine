package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/example/sync-vector-engine/internal/config"
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
