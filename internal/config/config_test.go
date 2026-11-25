package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// clearEnv blanks every variable Load reads, so the test sees the defaults even
// when it runs inside the docker compose test container (which sets some of them).
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"SERVICE_NAME", "APP_ENV", "HTTP_ADDR", "SHUTDOWN_TIMEOUT", "DATABASE_URL", "DB_MAX_CONNS", "RUN_MIGRATIONS",
		"REDIS_URL", "RABBITMQ_URL", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_TRACES_SAMPLER_RATIO", "PROVIDER_URL",
		"PROVIDER_TIMEOUT", "PROVIDER_URL_SMS", "PROVIDER_URL_EMAIL", "PROVIDER_URL_PUSH", "RATE_LIMIT_PER_SECOND",
		"RATE_LIMIT_BURST", "WORKER_CONCURRENCY", "MAX_ATTEMPTS", "RETRY_BASE_DELAY", "RETRY_MAX_DELAY",
		"BREAKER_FAILURE_THRESHOLD", "BREAKER_OPEN_TIMEOUT", "DISPATCH_INTERVAL", "DISPATCH_BATCH_SIZE",
		"BROKER_MAX_QUEUE_DEPTH", "PRIORITY_AGING_INTERVAL", "STUCK_QUEUED_AFTER", "STUCK_PROCESSING_AFTER", "LOG_LEVEL",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	c, err := Load("notification-api")
	if err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	checks := map[string]bool{
		"service name":         c.ServiceName == "notification-api",
		"http addr":            c.HTTPAddr == ":8080",
		"rate limit 100/s":     c.RateLimitPerSecond == 100,
		"strict pacing":        c.RateLimitBurst == 1,
		"max attempts":         c.MaxAttempts == 5,
		"retry base delay":     c.RetryBaseDelay == 2*time.Second,
		"retry max delay":      c.RetryMaxDelay == 10*time.Minute,
		"broker depth":         c.BrokerMaxQueueDepth == 2000,
		"stuck queued":         c.StuckQueuedAfter == 5*time.Minute,
		"migrations on":        c.RunMigrations,
		"log level info":       c.LogLevel == slog.LevelInfo,
		"no trace export":      c.OTLPEndpoint == "",
		"sample everything":    c.TraceSampleRatio == 1.0,
		"mock provider":        strings.HasPrefix(c.ProviderURL, "http://localhost:8090"),
		"no channel overrides": len(c.ProviderURLs) == 0,
	}
	for name, ok := range checks {
		if !ok {
			t.Errorf("default %s is wrong: %+v", name, c)
		}
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("HTTP_ADDR", ":9999")
	t.Setenv("RATE_LIMIT_PER_SECOND", "250")
	t.Setenv("MAX_ATTEMPTS", "8")
	t.Setenv("RETRY_BASE_DELAY", "500ms")
	t.Setenv("RUN_MIGRATIONS", "false")
	t.Setenv("LOG_LEVEL", "DEBUG")
	t.Setenv("PROVIDER_URL", "  https://webhook.site/abc  ")
	t.Setenv("PROVIDER_URL_EMAIL", "https://mail.example/send")

	c, err := Load("notification-worker")
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9999" || c.RateLimitPerSecond != 250 || c.MaxAttempts != 8 || c.RetryBaseDelay != 500*time.Millisecond {
		t.Fatalf("overrides not applied: %+v", c)
	}
	if c.RunMigrations || c.LogLevel != slog.LevelDebug {
		t.Fatalf("bool/log level overrides not applied: %+v", c)
	}
	if c.ProviderURL != "https://webhook.site/abc" {
		t.Fatalf("values must be trimmed, got %q", c.ProviderURL)
	}
	if got := c.ProviderURLFor("email"); got != "https://mail.example/send" {
		t.Fatalf("per-channel override ignored: %q", got)
	}
	if got := c.ProviderURLFor("sms"); got != "https://webhook.site/abc" {
		t.Fatalf("channels without override must use PROVIDER_URL, got %q", got)
	}
}

func TestUnparsableValuesFallBackToDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("WORKER_CONCURRENCY", "lots")
	t.Setenv("PROVIDER_TIMEOUT", "5 seconds")
	t.Setenv("RUN_MIGRATIONS", "maybe")
	c, err := Load("x")
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerConcurrency != 20 || c.ProviderTimeout != 5*time.Second || !c.RunMigrations {
		t.Fatalf("unparsable values must fall back to defaults: %+v", c)
	}
}

func TestInvalidConfigurationIsRejected(t *testing.T) {
	cases := map[string]string{
		"RATE_LIMIT_PER_SECOND":     "0",
		"RATE_LIMIT_BURST":          "-1",
		"WORKER_CONCURRENCY":        "0",
		"MAX_ATTEMPTS":              "0",
		"DISPATCH_BATCH_SIZE":       "0",
		"BROKER_MAX_QUEUE_DEPTH":    "0",
		"OTEL_TRACES_SAMPLER_RATIO": "1.5",
	}
	for key, value := range cases {
		t.Run(key, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(key, value)
			_, err := Load("x")
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("%s=%s must be rejected with a message naming it, got %v", key, value, err)
			}
		})
	}
}

func TestInvalidLogLevel(t *testing.T) {
	clearEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")
	if _, err := Load("x"); err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("unknown log level must be rejected, got %v", err)
	}
}
