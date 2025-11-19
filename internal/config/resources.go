package config

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
)

// Resources bundles the external connections used by the server so that their
// lifecycle can be managed in a single place.
type Resources struct {
	Postgres *pgxpool.Pool
	Redis    *redis.Client
	Object   *minio.Client
	cfg      Config
}

// NewResources builds all external dependencies using the provided
// configuration.
func NewResources(ctx context.Context, cfg Config) (*Resources, error) {
	pgCfg, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	pgPool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	objectClient, err := minio.New(cfg.ObjectEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.ObjectAccessKey, cfg.ObjectSecretKey, ""),
		Secure: cfg.ObjectUseSSL,
		Region: cfg.ObjectRegion,
	})
	if err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("create object client: %w", err)
	}

	res := &Resources{
		Postgres: pgPool,
		Redis:    redisClient,
		Object:   objectClient,
		cfg:      cfg,
	}

	if err := res.HealthCheck(ctx); err != nil {
		res.Close()
		return nil, err
	}

	return res, nil
}

// HealthCheck verifies that all dependency pools are healthy.
func (r *Resources) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := r.Postgres.Ping(ctx); err != nil {
		return fmt.Errorf("postgres healthcheck failed: %w", err)
	}

	if err := r.Redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis healthcheck failed: %w", err)
	}

	// MinIO/S3 doesn't expose a ping, so we attempt to stat the configured bucket.
	if _, err := r.Object.BucketExists(ctx, r.cfg.ObjectBucket); err != nil {
		return fmt.Errorf("object storage healthcheck failed: %w", err)
	}

	return nil
}

// Close disposes all active connections.
func (r *Resources) Close() {
	if r.Postgres != nil {
		r.Postgres.Close()
	}
	if r.Redis != nil {
		_ = r.Redis.Close()
	}
}
