// Package config loads runtime configuration from environment variables.
// Every setting has a sensible default so `docker compose up` works out of the box.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is shared by the api and worker binaries; each uses what it needs.
type Config struct {
	ServiceName string
	Environment string
	LogLevel    slog.Level

	HTTPAddr        string // api: public API; worker: metrics/health
	ShutdownTimeout time.Duration

	DatabaseURL      string
	DBMaxConns       int32
	RunMigrations    bool
	RedisURL         string
	RabbitMQURL      string
	OTLPEndpoint     string // empty disables trace export
	TraceSampleRatio float64

	// Provider
	ProviderURL     string
	ProviderURLs    map[string]string // per-channel override
	ProviderTimeout time.Duration

	// Processing
	RateLimitPerSecond int // per channel, global across all worker instances
	RateLimitBurst     int
	WorkerConcurrency  int // goroutines per channel
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	BreakerThreshold   int
	BreakerOpenTimeout time.Duration

	// Dispatcher
	DispatchInterval     time.Duration
	DispatchBatchSize    int
	BrokerMaxQueueDepth  int // per channel; backlog beyond this stays in Postgres
	AgingInterval        time.Duration
	StuckQueuedAfter     time.Duration
	StuckProcessingAfter time.Duration
}

// Load reads the environment.
func Load(serviceName string) (Config, error) {
	c := Config{
		ServiceName:     env("SERVICE_NAME", serviceName),
		Environment:     env("APP_ENV", "development"),
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),

		DatabaseURL:      env("DATABASE_URL", "postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable"),
		DBMaxConns:       int32(envInt("DB_MAX_CONNS", 20)),
		RunMigrations:    envBool("RUN_MIGRATIONS", true),
		RedisURL:         env("REDIS_URL", "redis://localhost:6379/0"),
		RabbitMQURL:      env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		OTLPEndpoint:     env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		TraceSampleRatio: envFloat("OTEL_TRACES_SAMPLER_RATIO", 1.0),

		ProviderURL:     env("PROVIDER_URL", "http://localhost:8090/webhook"),
		ProviderTimeout: envDuration("PROVIDER_TIMEOUT", 5*time.Second),
		ProviderURLs:    map[string]string{},

		RateLimitPerSecond: envInt("RATE_LIMIT_PER_SECOND", 100),
		RateLimitBurst:     envInt("RATE_LIMIT_BURST", 1),
		WorkerConcurrency:  envInt("WORKER_CONCURRENCY", 20),
		MaxAttempts:        envInt("MAX_ATTEMPTS", 5),
		RetryBaseDelay:     envDuration("RETRY_BASE_DELAY", 2*time.Second),
		RetryMaxDelay:      envDuration("RETRY_MAX_DELAY", 10*time.Minute),
		BreakerThreshold:   envInt("BREAKER_FAILURE_THRESHOLD", 5),
		BreakerOpenTimeout: envDuration("BREAKER_OPEN_TIMEOUT", 30*time.Second),

		DispatchInterval:     envDuration("DISPATCH_INTERVAL", 250*time.Millisecond),
		DispatchBatchSize:    envInt("DISPATCH_BATCH_SIZE", 500),
		BrokerMaxQueueDepth:  envInt("BROKER_MAX_QUEUE_DEPTH", 2000),
		AgingInterval:        envDuration("PRIORITY_AGING_INTERVAL", 5*time.Minute),
		StuckQueuedAfter:     envDuration("STUCK_QUEUED_AFTER", 15*time.Minute),
		StuckProcessingAfter: envDuration("STUCK_PROCESSING_AFTER", 2*time.Minute),
	}
	for _, ch := range []string{"sms", "email", "push"} {
		if v := os.Getenv("PROVIDER_URL_" + strings.ToUpper(ch)); v != "" {
			c.ProviderURLs[ch] = v
		}
	}

	lvl := strings.ToLower(env("LOG_LEVEL", "info"))
	if err := c.LogLevel.UnmarshalText([]byte(lvl)); err != nil {
		return c, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return c, c.validate()
}

func (c Config) validate() error {
	var problems []string
	if c.RateLimitPerSecond <= 0 {
		problems = append(problems, "RATE_LIMIT_PER_SECOND must be > 0")
	}
	if c.RateLimitBurst <= 0 {
		problems = append(problems, "RATE_LIMIT_BURST must be > 0")
	}
	if c.WorkerConcurrency <= 0 {
		problems = append(problems, "WORKER_CONCURRENCY must be > 0")
	}
	if c.MaxAttempts <= 0 {
		problems = append(problems, "MAX_ATTEMPTS must be > 0")
	}
	if c.DispatchBatchSize <= 0 {
		problems = append(problems, "DISPATCH_BATCH_SIZE must be > 0")
	}
	if c.BrokerMaxQueueDepth <= 0 {
		problems = append(problems, "BROKER_MAX_QUEUE_DEPTH must be > 0")
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		problems = append(problems, "OTEL_TRACES_SAMPLER_RATIO must be within [0,1]")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
	}
	return nil
}

// ProviderURLFor returns the provider endpoint for a channel.
func (c Config) ProviderURLFor(channel string) string {
	if u, ok := c.ProviderURLs[channel]; ok {
		return u
	}
	return c.ProviderURL
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, err := strconv.ParseBool(env(key, "")); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil {
		return v
	}
	return def
}
