package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

const notificationColumns = `id, batch_id, idempotency_key, request_hash, correlation_id, trace_parent,
	channel, recipient, subject, content, template_id, metadata, priority, effective_priority, status,
	attempts, max_attempts, scheduled_at, next_attempt_at, provider_message_id, last_error,
	created_at, updated_at, sent_at, failed_at, cancelled_at`

func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var (
		n                     domain.Notification
		requestHash, corrID   *string
		traceParent           *string
		channel, status       string
		priority, effPriority int16
		metadata              []byte
		nextAttemptAt         time.Time
	)
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &requestHash, &corrID, &traceParent,
		&channel, &n.Recipient, &n.Subject, &n.Content, &n.TemplateID, &metadata, &priority, &effPriority, &status,
		&n.Attempts, &n.MaxAttempts, &n.ScheduledAt, &nextAttemptAt, &n.ProviderMessageID, &n.LastError,
		&n.CreatedAt, &n.UpdatedAt, &n.SentAt, &n.FailedAt, &n.CancelledAt)
	if err != nil {
		return nil, err
	}
	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.Priority(priority)
	n.EffectivePriority = domain.Priority(effPriority)
	n.RequestHash = deref(requestHash)
	n.CorrelationID = deref(corrID)
	n.TraceParent = deref(traceParent)
	if len(metadata) > 0 {
		n.Metadata = metadata
	}
	if !n.Status.IsTerminal() {
		n.NextAttemptAt = &nextAttemptAt
	}
	return &n, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func metadataParam(n *domain.Notification) any {
	if len(n.Metadata) == 0 || string(n.Metadata) == "null" {
		return nil
	}
	return string(n.Metadata)
}

const insertNotificationSQL = `
INSERT INTO notifications (id, batch_id, idempotency_key, request_hash, correlation_id, trace_parent,
	channel, recipient, subject, content, template_id, metadata, priority, effective_priority, status,
	attempts, max_attempts, scheduled_at, next_attempt_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $13, $14, 0, $15, $16, $17, $18, $18)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING ` + notificationColumns

func insertArgs(n *domain.Notification) []any {
	next := n.CreatedAt
	if n.ScheduledAt != nil {
		next = *n.ScheduledAt
	}
	return []any{n.ID, n.BatchID, n.IdempotencyKey, nullIfEmpty(n.RequestHash), nullIfEmpty(n.CorrelationID),
		nullIfEmpty(n.TraceParent), string(n.Channel), n.Recipient, n.Subject, n.Content, n.TemplateID,
		metadataParam(n), int16(n.Priority), string(n.Status), n.MaxAttempts, n.ScheduledAt, next, n.CreatedAt}
}

// CreateNotification inserts n. When n carries an idempotency key that already
// exists, the existing row is returned with created=false; if the stored request
// fingerprint differs, domain.ErrIdempotencyConflict is returned.
func (s *Store) CreateNotification(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	stored, err := scanNotification(s.pool.QueryRow(ctx, insertNotificationSQL, insertArgs(n)...))
	if err == nil {
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("insert notification: %w", err)
	}
	existing, err := s.getByIdempotencyKey(ctx, s.pool, *n.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	if existing.RequestHash != n.RequestHash {
		return existing, false, domain.ErrIdempotencyConflict
	}
	return existing, false, nil
}

func (s *Store) getByIdempotencyKey(ctx context.Context, q querier, key string) (*domain.Notification, error) {
	n, err := scanNotification(q.QueryRow(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE idempotency_key = $1`, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return n, err
}

// BatchItemResult is the persisted outcome for one batch item.
type BatchItemResult struct {
	Notification *domain.Notification
	Created      bool // false: an item with the same idempotency key already existed
}

// BatchResult is the persisted outcome of a batch request.
type BatchResult struct {
	Batch    *domain.Batch
	Items    []BatchItemResult
	Replayed bool // the whole batch request was an idempotent replay
}

// ItemConflictError reports an item whose idempotency key was reused with a different payload.
type ItemConflictError struct{ Index int }

func (e *ItemConflictError) Error() string {
	return fmt.Sprintf("notifications[%d]: %s", e.Index, domain.ErrIdempotencyConflict)
}

// Unwrap allows errors.Is(err, domain.ErrIdempotencyConflict).
func (e *ItemConflictError) Unwrap() error { return domain.ErrIdempotencyConflict }

// CreateBatch stores a batch and its notifications atomically. All inserts are
// pipelined in a single round trip.
func (s *Store) CreateBatch(ctx context.Context, b *domain.Batch, items []*domain.Notification) (*BatchResult, error) {
	var result *BatchResult
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var inserted bool
		err := tx.QueryRow(ctx, `
			INSERT INTO batches (id, idempotency_key, request_hash, total, correlation_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING true`,
			b.ID, b.IdempotencyKey, nullIfEmpty(b.RequestHash), b.Total, nullIfEmpty(b.CorrelationID), b.CreatedAt,
		).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, err := s.replayBatch(ctx, tx, *b.IdempotencyKey, b.RequestHash)
			if err != nil {
				return err
			}
			result = existing
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}

		pb := &pgx.Batch{}
		for _, n := range items {
			pb.Queue(insertNotificationSQL, insertArgs(n)...)
		}
		br := tx.SendBatch(ctx, pb)
		res := &BatchResult{Batch: b, Items: make([]BatchItemResult, len(items))}
		var missing []int
		for i := range items {
			stored, err := scanNotification(br.QueryRow())
			switch {
			case err == nil:
				res.Items[i] = BatchItemResult{Notification: stored, Created: true}
			case errors.Is(err, pgx.ErrNoRows):
				missing = append(missing, i)
			default:
				_ = br.Close()
				return fmt.Errorf("insert notification %d: %w", i, err)
			}
		}
		if err := br.Close(); err != nil {
			return fmt.Errorf("insert notifications: %w", err)
		}
		for _, i := range missing {
			existing, err := s.getByIdempotencyKey(ctx, tx, *items[i].IdempotencyKey)
			if err != nil {
				return err
			}
			if existing.RequestHash != items[i].RequestHash {
				return &ItemConflictError{Index: i}
			}
			res.Items[i] = BatchItemResult{Notification: existing, Created: false}
		}
		result = res
		return nil
	})
	return result, err
}

func (s *Store) replayBatch(ctx context.Context, tx pgx.Tx, key, hash string) (*BatchResult, error) {
	b := &domain.Batch{}
	var storedHash, corrID *string
	err := tx.QueryRow(ctx, `SELECT id, idempotency_key, request_hash, total, correlation_id, created_at FROM batches WHERE idempotency_key = $1`, key).
		Scan(&b.ID, &b.IdempotencyKey, &storedHash, &b.Total, &corrID, &b.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("load batch for replay: %w", err)
	}
	b.RequestHash, b.CorrelationID = deref(storedHash), deref(corrID)
	if b.RequestHash != hash {
		return nil, domain.ErrIdempotencyConflict
	}
	rows, err := tx.Query(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE batch_id = $1 ORDER BY id`, b.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := &BatchResult{Batch: b, Replayed: true}
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		res.Items = append(res.Items, BatchItemResult{Notification: n})
	}
	return res, rows.Err()
}

// GetNotification loads one notification.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return n, err
}

// ListAttempts returns the delivery attempts of a notification, oldest first.
func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, notification_id, attempt_number, outcome, http_status, error, provider_message_id, duration_ms, created_at
		FROM notification_attempts WHERE notification_id = $1 ORDER BY attempt_number, id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		var a domain.Attempt
		var outcome string
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.AttemptNumber, &outcome, &a.HTTPStatus, &a.Error,
			&a.ProviderMessageID, &a.DurationMS, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Outcome = domain.AttemptOutcome(outcome)
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetBatchSummary aggregates per-status counts for a batch.
func (s *Store) GetBatchSummary(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	sum := &domain.BatchSummary{BatchID: id, Counts: map[domain.Status]int{}}
	err := s.pool.QueryRow(ctx, `SELECT total, created_at FROM batches WHERE id = $1`, id).Scan(&sum.Total, &sum.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	terminal, all := 0, 0
	for rows.Next() {
		var st string
		var c int
		if err := rows.Scan(&st, &c); err != nil {
			return nil, err
		}
		sum.Counts[domain.Status(st)] = c
		all += c
		if domain.Status(st).IsTerminal() {
			terminal += c
		}
	}
	sum.Completed = all > 0 && terminal == all
	return sum, rows.Err()
}

// ListFilter narrows ListNotifications.
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	BatchID *uuid.UUID
	From    *time.Time // created_at >= From
	To      *time.Time // created_at <  To
	Cursor  *uuid.UUID // return items with id < Cursor (ids are time ordered)
	Limit   int
}

// ListNotifications returns up to Limit items, newest first, plus whether more exist.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]*domain.Notification, bool, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		add("status = $%d", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = $%d", string(*f.Channel))
	}
	if f.BatchID != nil {
		add("batch_id = $%d", *f.BatchID)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	if f.Cursor != nil {
		add("id < $%d", *f.Cursor)
	}
	q := `SELECT ` + notificationColumns + ` FROM notifications`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1)
	q += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := make([]*domain.Notification, 0, f.Limit)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, false, err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(out) > f.Limit
	if hasMore {
		out = out[:f.Limit]
	}
	return out, hasMore, nil
}

func statusStrings(list []domain.Status) []string {
	out := make([]string, len(list))
	for i, s := range list {
		out[i] = string(s)
	}
	return out
}

// Cancel atomically cancels a notification that has not been picked up yet.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications SET status = 'cancelled', cancelled_at = now(), updated_at = now()
		WHERE id = $1 AND status = ANY($2::text[])
		RETURNING `+notificationColumns, id, statusStrings(domain.CancellableStatuses)))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	current, err := s.GetNotification(ctx, id)
	if err != nil {
		return nil, err
	}
	return current, &domain.InvalidStateError{Current: current.Status, Action: "cancel"}
}

// CancelBatch cancels every cancellable notification of a batch and returns them.
func (s *Store) CancelBatch(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM batches WHERE id = $1)`, batchID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications SET status = 'cancelled', cancelled_at = now(), updated_at = now()
		WHERE batch_id = $1 AND status = ANY($2::text[])
		RETURNING `+notificationColumns, batchID, statusStrings(domain.CancellableStatuses))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// Requeue moves a failed notification back to pending with a fresh attempt budget
// (manual dead-letter replay).
func (s *Store) Requeue(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'pending', attempts = 0, next_attempt_at = now(), effective_priority = priority,
		    failed_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'failed'
		RETURNING `+notificationColumns, id))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	current, err := s.GetNotification(ctx, id)
	if err != nil {
		return nil, err
	}
	return current, &domain.InvalidStateError{Current: current.Status, Action: "retry"}
}
