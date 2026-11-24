package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// DispatchItem is the minimal information the dispatcher publishes to the broker.
type DispatchItem struct {
	ID            uuid.UUID
	BatchID       *uuid.UUID
	Channel       domain.Channel
	Priority      domain.Priority // effective (aged) priority
	Attempts      int
	CorrelationID string
	TraceParent   string
	// PreviousStatus is the status before dispatch (pending, scheduled or retrying);
	// the row returns to it if publishing fails.
	PreviousStatus domain.Status
}

// DispatchDue moves up to limit due notifications of a channel to "queued" and
// hands them to publish.
//
// The state change is committed BEFORE publishing. Publishing inside the
// transaction would let an idle consumer receive the message before the commit
// is visible; its conditional claim would then miss the row and drop the message.
// Consequences of the chosen order:
//   - publish fails: the rows are reverted to their previous status right away;
//   - the process crashes between commit and publish: the rows stay "queued"
//     without a message and the reaper returns them to the backlog
//     (RecoverStuck, STUCK_QUEUED_AFTER).
//
// SKIP LOCKED lets several worker replicas dispatch concurrently without
// blocking or double-dispatching.
func (s *Store) DispatchDue(ctx context.Context, ch domain.Channel, limit int, publish func(context.Context, []DispatchItem) error) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	var (
		items    []DispatchItem
		ids      []string
		previous []string
	)
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, batch_id, effective_priority, attempts, correlation_id, trace_parent, status
			FROM notifications
			WHERE channel = $1 AND status = ANY($2::text[]) AND next_attempt_at <= now()
			ORDER BY effective_priority DESC, next_attempt_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED`, string(ch), statusStrings(domain.DispatchableStatuses), limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var it DispatchItem
			var prio int16
			var corr, tp *string
			var status string
			if err := rows.Scan(&it.ID, &it.BatchID, &prio, &it.Attempts, &corr, &tp, &status); err != nil {
				rows.Close()
				return err
			}
			it.Channel, it.Priority, it.CorrelationID, it.TraceParent = ch, domain.Priority(prio), deref(corr), deref(tp)
			it.PreviousStatus = domain.Status(status)
			items = append(items, it)
			ids = append(ids, it.ID.String())
			previous = append(previous, status)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE notifications SET status = 'queued', updated_at = now() WHERE id = ANY($1::uuid[])`, ids)
		return err
	})
	if err != nil || len(items) == 0 {
		return 0, err
	}

	if err := publish(ctx, items); err != nil {
		// Only rows still queued are reverted; anything cancelled meanwhile stays cancelled.
		revertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, rerr := s.pool.Exec(revertCtx, `
			UPDATE notifications n SET status = v.prev, updated_at = now()
			FROM unnest($1::uuid[], $2::text[]) AS v(id, prev)
			WHERE n.id = v.id AND n.status = 'queued'`, ids, previous); rerr != nil {
			return 0, fmt.Errorf("publish: %w (revert failed, reaper will recover: %w)", err, rerr)
		}
		return 0, fmt.Errorf("publish: %w", err)
	}
	return len(items), nil
}

// Claim moves a queued notification to processing and consumes one attempt.
// It returns (nil, nil) when the message is stale: cancelled meanwhile, already
// claimed by a duplicate delivery, or re-dispatched with a different attempt count.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, attempts int) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND status = 'queued' AND attempts = $2
		RETURNING `+notificationColumns, id, attempts))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return n, err
}

// Defer puts a queued notification back into the backlog without consuming an
// attempt (used while the channel's circuit breaker is open).
func (s *Store) Defer(ctx context.Context, id uuid.UUID, attempts int, until time.Time, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notifications SET status = 'retrying', next_attempt_at = $3, last_error = $4, updated_at = now()
		WHERE id = $1 AND status = 'queued' AND attempts = $2`, id, attempts, until, reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Completion describes the result of a processing attempt.
type Completion struct {
	Status            domain.Status // sent | retrying | failed
	Attempt           domain.Attempt
	NextAttemptAt     time.Time // for retrying
	ProviderMessageID *string
	LastError         *string
}

// Complete records the attempt and applies the resulting transition atomically.
// The update is conditional on the row still being in processing.
func (s *Store) Complete(ctx context.Context, id uuid.UUID, c Completion) (*domain.Notification, error) {
	var out *domain.Notification
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		a := c.Attempt
		if _, err := tx.Exec(ctx, `
			INSERT INTO notification_attempts (notification_id, attempt_number, outcome, http_status, error, provider_message_id, duration_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id, a.AttemptNumber, string(a.Outcome), a.HTTPStatus, a.Error, a.ProviderMessageID, a.DurationMS); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		var q string
		var args []any
		switch c.Status {
		case domain.StatusSent:
			q = `UPDATE notifications SET status = 'sent', sent_at = now(), provider_message_id = $2, last_error = NULL, updated_at = now()
			     WHERE id = $1 AND status = 'processing' RETURNING ` + notificationColumns
			args = []any{id, c.ProviderMessageID}
		case domain.StatusRetrying:
			q = `UPDATE notifications SET status = 'retrying', next_attempt_at = $2, last_error = $3, updated_at = now()
			     WHERE id = $1 AND status = 'processing' RETURNING ` + notificationColumns
			args = []any{id, c.NextAttemptAt, c.LastError}
		case domain.StatusFailed:
			q = `UPDATE notifications SET status = 'failed', failed_at = now(), last_error = $2, updated_at = now()
			     WHERE id = $1 AND status = 'processing' RETURNING ` + notificationColumns
			args = []any{id, c.LastError}
		default:
			return fmt.Errorf("invalid completion status %q", c.Status)
		}
		n, err := scanNotification(tx.QueryRow(ctx, q, args...))
		if errors.Is(err, pgx.ErrNoRows) {
			// The reaper already recovered this row; keep the attempt record but do not override state.
			out = nil
			return nil
		}
		out = n
		return err
	})
	return out, err
}

// RecoverStuck repairs rows abandoned by crashed workers or lost broker messages.
//   - queued for longer than queuedAfter: back to pending (re-dispatch; duplicates are harmless
//     because Claim requires a matching attempt count)
//   - processing for longer than processingAfter: the worker died mid-attempt; schedule a retry,
//     or fail if the attempt budget is exhausted.
func (s *Store) RecoverStuck(ctx context.Context, queuedAfter, processingAfter time.Duration) (requeued, recovered int64, err error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE notifications SET status = 'pending', next_attempt_at = now(), updated_at = now()
		WHERE status = 'queued' AND updated_at < now() - make_interval(secs => $1::float8)`, queuedAfter.Seconds())
	if err != nil {
		return 0, 0, err
	}
	requeued = tag.RowsAffected()
	tag, err = s.pool.Exec(ctx, `
		UPDATE notifications SET
			status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'retrying' END,
			failed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
			next_attempt_at = now(),
			last_error = 'attempt abandoned by worker (recovered)',
			updated_at = now()
		WHERE status = 'processing' AND updated_at < now() - make_interval(secs => $1::float8)`, processingAfter.Seconds())
	if err != nil {
		return requeued, 0, err
	}
	return requeued, tag.RowsAffected(), nil
}

// AgePriorities promotes notifications that have been due for a long time by one
// priority level per agingInterval, so low priority traffic cannot starve forever
// behind a continuous stream of high priority messages.
func (s *Store) AgePriorities(ctx context.Context, agingInterval time.Duration) (int64, error) {
	if agingInterval <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		WITH aged AS (
			SELECT id, LEAST(3, priority + FLOOR(EXTRACT(EPOCH FROM (now() - COALESCE(scheduled_at, created_at)))::float8 / $1::float8)::int) AS p
			FROM notifications
			WHERE status = ANY($2::text[]) AND effective_priority < 3 AND next_attempt_at <= now()
			  AND COALESCE(scheduled_at, created_at) < now() - make_interval(secs => $1::float8)
		)
		UPDATE notifications n SET effective_priority = aged.p
		FROM aged WHERE n.id = aged.id AND aged.p > n.effective_priority`,
		agingInterval.Seconds(), statusStrings(domain.DispatchableStatuses))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
