// Command worker runs the processing engine: the dispatcher that feeds RabbitMQ from
// the PostgreSQL backlog and the per-channel consumer pools that deliver messages.
// It also exposes /health/live, /health/ready and /metrics on HTTP_ADDR.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/saltindag22/case-notification-system/internal/api"
	"github.com/saltindag22/case-notification-system/internal/circuitbreaker"
	"github.com/saltindag22/case-notification-system/internal/config"
	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/healthcheck"
	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/provider"
	"github.com/saltindag22/case-notification-system/internal/queue"
	"github.com/saltindag22/case-notification-system/internal/ratelimit"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/retry"
	"github.com/saltindag22/case-notification-system/internal/tracing"
	"github.com/saltindag22/case-notification-system/internal/worker"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck.Run(":9091"))
	}
	if err := run(); err != nil {
		slog.Error("worker terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("notification-worker")
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, cfg.ServiceName, version, cfg.OTLPEndpoint, cfg.TraceSampleRatio)
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	if cfg.RunMigrations {
		if err := repository.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
	}
	pool, err := repository.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := repository.New(pool)

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	broker, err := queue.Dial(ctx, cfg.RabbitMQURL)
	if err != nil {
		return err
	}
	defer broker.Close()

	m := metrics.New("notification_worker")
	breakers := map[domain.Channel]*circuitbreaker.Breaker{}
	for _, ch := range domain.Channels {
		breakers[ch] = circuitbreaker.New(circuitbreaker.Config{
			FailureThreshold: cfg.BreakerThreshold,
			OpenTimeout:      cfg.BreakerOpenTimeout,
			HalfOpenMaxCalls: 1,
		}, func(s circuitbreaker.State) {
			m.BreakerState.WithLabelValues(string(ch)).Set(float64(s))
			log.Warn("circuit breaker state changed", "channel", ch, "state", s.String())
		})
		m.BreakerState.WithLabelValues(string(ch)).Set(0)
	}

	processor := worker.NewProcessor(worker.ProcessorDeps{
		Store:    store,
		Sender:   provider.NewClient(cfg.ProviderURLFor, cfg.ProviderTimeout),
		Limiter:  ratelimit.NewRedis(rdb, cfg.RateLimitPerSecond, cfg.RateLimitBurst),
		Breakers: breakers,
		Policy: retry.Policy{
			MaxAttempts: cfg.MaxAttempts, BaseDelay: cfg.RetryBaseDelay, MaxDelay: cfg.RetryMaxDelay,
			Multiplier: 2, Jitter: 0.5,
		},
		Events:  events.NewRedisBus(rdb, log),
		Metrics: m,
		Log:     log,
	})
	consumers := worker.NewPool(broker, processor, cfg.WorkerConcurrency, log)
	dispatcher := worker.NewDispatcher(store, broker, worker.DispatcherConfig{
		Interval:             cfg.DispatchInterval,
		BatchSize:            cfg.DispatchBatchSize,
		MaxQueueDepth:        cfg.BrokerMaxQueueDepth,
		AgingInterval:        cfg.AgingInterval,
		StuckQueuedAfter:     cfg.StuckQueuedAfter,
		StuckProcessingAfter: cfg.StuckProcessingAfter,
	}, m, log)

	// Ops HTTP server.
	checks := []api.Check{
		{Name: "postgres", Fn: store.Ping},
		{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		{Name: "rabbitmq", Fn: func(context.Context) error {
			if broker.IsClosed() {
				return errors.New("connection closed")
			}
			return nil
		}},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		status, body := api.RunChecks(r.Context(), checks)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	opsSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := opsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "error", err)
		}
	}()

	// Crash-only: if the broker connection drops, stop and let the orchestrator restart us.
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case amqpErr := <-broker.NotifyClose():
			if amqpErr != nil {
				cancel(fmt.Errorf("rabbitmq connection lost: %w", amqpErr))
			}
		case <-runCtx.Done():
		}
	}()
	go reportDeadLetters(runCtx, broker, m)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); dispatcher.Run(runCtx) }()
	var poolErr error
	go func() {
		defer wg.Done()
		if err := consumers.Run(runCtx); err != nil {
			poolErr = err
			cancel(err)
		}
	}()
	log.Info("worker started", "version", version, "rate_limit_per_channel", cfg.RateLimitPerSecond,
		"concurrency_per_channel", cfg.WorkerConcurrency, "max_attempts", cfg.MaxAttempts)

	<-runCtx.Done()
	log.Info("shutting down worker")
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("shutdown timeout reached with work in flight; the reaper will recover it")
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = opsSrv.Shutdown(shutdownCtx)

	if poolErr != nil {
		return poolErr
	}
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

func reportDeadLetters(ctx context.Context, broker *queue.Client, m *metrics.Metrics) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		if n, err := broker.QueueDepthAny(queue.DeadQueue); err == nil {
			m.BrokerQueueDepth.WithLabelValues(queue.DeadQueue).Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
