package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config represents the application configuration sourced from the environment.
type Config struct {
	AppName          string
	PostgresURL      string
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	ObjectEndpoint   string
	ObjectRegion     string
	ObjectBucket     string
	ObjectAccessKey  string
	ObjectSecretKey  string
	ObjectUseSSL     bool
	HTTPListenAddr   string
	MetricsAddr      string
	ShutdownTimeout  time.Duration
	HealthcheckProbe time.Duration
	OTLPEndpoint     string
}

// Load reads configuration from the environment while applying sensible defaults
// for local development.
func Load() (Config, error) {
	cfg := Config{
		AppName:          getEnv("APP_NAME", "sync-vector-engine"),
		PostgresURL:      getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    os.Getenv("REDIS_PASSWORD"),
		ObjectEndpoint:   getEnv("OBJECT_ENDPOINT", "localhost:9000"),
		ObjectRegion:     getEnv("OBJECT_REGION", "us-east-1"),
		ObjectBucket:     getEnv("OBJECT_BUCKET", "sync-vector"),
		ObjectAccessKey:  getEnv("OBJECT_ACCESS_KEY", "minio"),
		ObjectSecretKey:  getEnv("OBJECT_SECRET_KEY", "miniostorage"),
		HTTPListenAddr:   getEnv("HTTP_LISTEN_ADDR", ":8080"),
		MetricsAddr:      getEnv("METRICS_LISTEN_ADDR", ":9090"),
		ShutdownTimeout:  getDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		HealthcheckProbe: getDuration("HEALTHCHECK_INTERVAL", 30*time.Second),
		OTLPEndpoint:     os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}

	redisDB := getInt("REDIS_DB", 0)
	useSSL := getBool("OBJECT_USE_SSL", false)

	cfg.RedisDB = redisDB
	cfg.ObjectUseSSL = useSSL

	if cfg.ObjectAccessKey == "" || cfg.ObjectSecretKey == "" {
		return Config{}, fmt.Errorf("object storage credentials must be provided")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}
