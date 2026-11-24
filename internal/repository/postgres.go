// Package repository persists notifications, batches, attempts and templates in PostgreSQL.
//
// Concurrency rules enforced here (not in application code):
//   - idempotency: unique partial index + INSERT ... ON CONFLICT DO NOTHING
//   - cancel vs. worker race: every transition is a conditional UPDATE ... WHERE status = ...
//   - multiple dispatchers: SELECT ... FOR UPDATE SKIP LOCKED
//   - duplicate broker deliveries: claim requires (status = queued AND attempts = n)
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // migrate driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/saltindag22/case-notification-system/migrations"
)

// Store is the PostgreSQL repository.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Connect opens a connection pool and verifies connectivity.
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	// The database may still be starting (docker compose); retry briefly.
	var pingErr error
	for i := 0; i < 30; i++ {
		if pingErr = pool.Ping(ctx); pingErr == nil {
			return pool, nil
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	pool.Close()
	return nil, fmt.Errorf("ping database: %w", pingErr)
}

// Migrate applies all pending up migrations. golang-migrate takes an advisory
// lock, so concurrent api/worker start-ups are safe.
func Migrate(databaseURL string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toMigrateURL(databaseURL))
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// toMigrateURL converts postgres:// URLs to the pgx5:// scheme golang-migrate expects.
func toMigrateURL(u string) string {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if len(u) >= len(p) && u[:len(p)] == p {
			return "pgx5://" + u[len(p):]
		}
	}
	return u
}

// Ping checks database connectivity (readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// querier is satisfied by both the pool and a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
