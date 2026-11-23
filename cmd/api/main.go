// Command api serves the notification management REST API, the WebSocket status
// stream, health checks and metrics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	apidocs "github.com/saltindag22/case-notification-system/api"
	"github.com/saltindag22/case-notification-system/internal/api"
	"github.com/saltindag22/case-notification-system/internal/config"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/healthcheck"
	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/service"
	"github.com/saltindag22/case-notification-system/internal/tracing"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck.Run(":8080"))
	}
	if err := run(); err != nil {
		slog.Error("api terminated", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("notification-api")
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
		if err := migrateWithRetry(ctx, cfg.DatabaseURL, log); err != nil {
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

	bus := events.NewRedisBus(rdb, log)
	m := metrics.New("notification_api")
	m.RegisterBacklog("notification", store, log)
	svc := service.New(store, bus, cfg.MaxAttempts)

	var origins []string
	if v := os.Getenv("WS_ALLOWED_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
	}
	hub := api.NewHub(log, m, origins)
	go hub.Run(ctx, bus.Subscribe)

	server := api.NewServer(api.Deps{
		Service: svc,
		Stats:   store,
		Checks: []api.Check{
			{Name: "postgres", Fn: store.Ping},
			{Name: "redis", Fn: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
		Hub:         hub,
		Metrics:     m,
		Log:         log,
		OpenAPISpec: apidocs.Spec,
		Version:     version,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second, // cleared by the websocket upgrader for /ws
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// migrateWithRetry tolerates the database still starting up.
func migrateWithRetry(ctx context.Context, url string, log *slog.Logger) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = repository.Migrate(url); err == nil {
			log.Info("database migrations applied")
			return nil
		}
		log.Warn("migration attempt failed, retrying", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return err
}
