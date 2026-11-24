package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// These tests need a real PostgreSQL. They run when DATABASE_URL is set, e.g.:
//
//	docker compose run --rm tests
func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	if err := Migrate(url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := Connect(ctx, url, 10)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `TRUNCATE notification_attempts, notifications, batches, templates CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return New(pool)
}

func newNotification(ch domain.Channel, p domain.Priority) *domain.Notification {
	return &domain.Notification{
		ID:          domain.NewID(),
		Channel:     ch,
		Recipient:   "+905551234567",
		Content:     "hello",
		Priority:    p,
		Status:      domain.StatusPending,
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
		RequestHash: "h1",
	}
}

func TestCreateAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	n.Metadata = []byte(`{"campaign":"x"}`)
	stored, created, err := s.CreateNotification(ctx, n)
	if err != nil || !created {
		t.Fatalf("create: %v created=%v", err, created)
	}
	got, err := s.GetNotification(ctx, stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Priority != domain.PriorityHigh || got.Status != domain.StatusPending || string(got.Metadata) != `{"campaign": "x"}` {
		t.Fatalf("unexpected row %+v (metadata %s)", got, got.Metadata)
	}
	if _, err := s.GetNotification(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestIdempotentCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	key := "order-42"

	first := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	first.IdempotencyKey = &key
	a, created, err := s.CreateNotification(ctx, first)
	if err != nil || !created {
		t.Fatalf("first create: %v", err)
	}

	again := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	again.IdempotencyKey = &key
	b, created, err := s.CreateNotification(ctx, again)
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("replay should return original: created=%v id=%v err=%v", created, b.ID, err)
	}

	different := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	different.IdempotencyKey = &key
	different.RequestHash = "other"
	if _, _, err := s.CreateNotification(ctx, different); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestConcurrentIdempotentCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	key := "race-key"
	var wg sync.WaitGroup
	var createdCount atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := newNotification(domain.ChannelEmail, domain.PriorityNormal)
			n.IdempotencyKey = &key
			if _, created, err := s.CreateNotification(ctx, n); err == nil && created {
				createdCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if createdCount.Load() != 1 {
		t.Fatalf("exactly one insert must win, got %d", createdCount.Load())
	}
}

func TestBatchCreateAndReplay(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	key := "batch-1"
	mk := func() (*domain.Batch, []*domain.Notification) {
		b := &domain.Batch{ID: domain.NewID(), IdempotencyKey: &key, RequestHash: "bh", Total: 3, CreatedAt: time.Now()}
		var items []*domain.Notification
		for i := 0; i < 3; i++ {
			n := newNotification(domain.ChannelPush, domain.PriorityLow)
			n.BatchID = &b.ID
			items = append(items, n)
		}
		return b, items
	}
	b, items := mk()
	res, err := s.CreateBatch(ctx, b, items)
	if err != nil || res.Replayed || len(res.Items) != 3 {
		t.Fatalf("create batch: %v %+v", err, res)
	}
	b2, items2 := mk()
	res2, err := s.CreateBatch(ctx, b2, items2)
	if err != nil || !res2.Replayed || res2.Batch.ID != b.ID || len(res2.Items) != 3 {
		t.Fatalf("replay batch: %v %+v", err, res2)
	}
	sum, err := s.GetBatchSummary(ctx, b.ID)
	if err != nil || sum.Total != 3 || sum.Counts[domain.StatusPending] != 3 || sum.Completed {
		t.Fatalf("summary: %v %+v", err, sum)
	}
}

func TestCancelRules(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n, _, _ := s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityNormal))

	if _, err := s.Cancel(ctx, n.ID); err != nil {
		t.Fatalf("cancel pending: %v", err)
	}
	_, err := s.Cancel(ctx, n.ID)
	var ise *domain.InvalidStateError
	if !errors.As(err, &ise) || ise.Current != domain.StatusCancelled {
		t.Fatalf("second cancel must be invalid state, got %v", err)
	}
	if _, err := s.Cancel(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

// TestDispatchClaimComplete walks the happy path and the stale-message guards.
func TestDispatchClaimComplete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	low, _, _ := s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityLow))
	high, _, _ := s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityHigh))
	_, _, _ = s.CreateNotification(ctx, newNotification(domain.ChannelEmail, domain.PriorityHigh))

	var published []DispatchItem
	n, err := s.DispatchDue(ctx, domain.ChannelSMS, 10, func(_ context.Context, items []DispatchItem) error {
		published = append(published, items...)
		return nil
	})
	if err != nil || n != 2 {
		t.Fatalf("dispatch: n=%d err=%v", n, err)
	}
	if published[0].ID != high.ID || published[1].ID != low.ID {
		t.Fatal("high priority must be dispatched first")
	}

	// Duplicate delivery: only the first claim wins.
	c1, err := s.Claim(ctx, high.ID, 0)
	if err != nil || c1 == nil || c1.Attempts != 1 || c1.Status != domain.StatusProcessing {
		t.Fatalf("claim: %+v %v", c1, err)
	}
	if c2, _ := s.Claim(ctx, high.ID, 0); c2 != nil {
		t.Fatal("duplicate claim must be rejected")
	}
	// Processing notifications can no longer be cancelled.
	if _, err := s.Cancel(ctx, high.ID); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("cancel during processing should fail, got %v", err)
	}

	mid := "provider-1"
	done, err := s.Complete(ctx, high.ID, Completion{
		Status:            domain.StatusSent,
		ProviderMessageID: &mid,
		Attempt:           domain.Attempt{AttemptNumber: 1, Outcome: domain.OutcomeSuccess, DurationMS: 12},
	})
	if err != nil || done.Status != domain.StatusSent || done.SentAt == nil || *done.ProviderMessageID != mid {
		t.Fatalf("complete: %+v %v", done, err)
	}
	attempts, _ := s.ListAttempts(ctx, high.ID)
	if len(attempts) != 1 || attempts[0].Outcome != domain.OutcomeSuccess {
		t.Fatalf("attempts: %+v", attempts)
	}

	// A message cancelled while waiting in the broker is skipped at claim time.
	if _, err := s.Cancel(ctx, low.ID); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	if c, _ := s.Claim(ctx, low.ID, 0); c != nil {
		t.Fatal("cancelled message must not be claimed")
	}
}

func TestRetryAndFailFlow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	n, _, _ := s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityNormal))
	noop := func(context.Context, []DispatchItem) error { return nil }

	_, _ = s.DispatchDue(ctx, domain.ChannelSMS, 10, noop)
	_, _ = s.Claim(ctx, n.ID, 0)
	msg := "503"
	retrying, err := s.Complete(ctx, n.ID, Completion{
		Status: domain.StatusRetrying, NextAttemptAt: time.Now().Add(time.Hour), LastError: &msg,
		Attempt: domain.Attempt{AttemptNumber: 1, Outcome: domain.OutcomeRetryableError, DurationMS: 5},
	})
	if err != nil || retrying.Status != domain.StatusRetrying {
		t.Fatalf("retry: %+v %v", retrying, err)
	}
	// Not due yet: dispatcher must not pick it up.
	if c, _ := s.DispatchDue(ctx, domain.ChannelSMS, 10, noop); c != 0 {
		t.Fatal("retry scheduled in the future must not be dispatched")
	}

	_, _ = s.pool.Exec(ctx, `UPDATE notifications SET next_attempt_at = now() WHERE id = $1`, n.ID)
	_, _ = s.DispatchDue(ctx, domain.ChannelSMS, 10, noop)
	if c, _ := s.Claim(ctx, n.ID, 0); c != nil {
		t.Fatal("stale attempt number must not claim")
	}
	_, _ = s.Claim(ctx, n.ID, 1)
	failed, err := s.Complete(ctx, n.ID, Completion{
		Status: domain.StatusFailed, LastError: &msg,
		Attempt: domain.Attempt{AttemptNumber: 2, Outcome: domain.OutcomePermanentError, DurationMS: 5},
	})
	if err != nil || failed.Status != domain.StatusFailed || failed.FailedAt == nil {
		t.Fatalf("fail: %+v %v", failed, err)
	}

	requeued, err := s.Requeue(ctx, n.ID)
	if err != nil || requeued.Status != domain.StatusPending || requeued.Attempts != 0 {
		t.Fatalf("requeue: %+v %v", requeued, err)
	}
}

func TestRecoverStuckAndAging(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	stuck, _, _ := s.CreateNotification(ctx, newNotification(domain.ChannelPush, domain.PriorityNormal))
	_, _ = s.DispatchDue(ctx, domain.ChannelPush, 10, func(context.Context, []DispatchItem) error { return nil })
	_, _ = s.Claim(ctx, stuck.ID, 0)
	_, _ = s.pool.Exec(ctx, `UPDATE notifications SET updated_at = now() - interval '1 hour' WHERE id = $1`, stuck.ID)

	_, recovered, err := s.RecoverStuck(ctx, time.Hour*2, time.Minute)
	if err != nil || recovered != 1 {
		t.Fatalf("recover: %d %v", recovered, err)
	}
	got, _ := s.GetNotification(ctx, stuck.ID)
	if got.Status != domain.StatusRetrying {
		t.Fatalf("stuck processing should be retried, got %s", got.Status)
	}

	old := newNotification(domain.ChannelEmail, domain.PriorityLow)
	old.CreatedAt = time.Now().Add(-11 * time.Minute)
	_, _, _ = s.CreateNotification(ctx, old)
	aged, err := s.AgePriorities(ctx, 5*time.Minute)
	if err != nil || aged < 1 {
		t.Fatalf("aging: %d %v", aged, err)
	}
	got, _ = s.GetNotification(ctx, old.ID)
	if got.EffectivePriority != domain.PriorityHigh || got.Priority != domain.PriorityLow {
		t.Fatalf("low priority waiting 11m with 5m aging should be promoted to high, got eff=%v", got.EffectivePriority)
	}
}

func TestListPagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _, _ = s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityNormal))
	}
	_, _, _ = s.CreateNotification(ctx, newNotification(domain.ChannelEmail, domain.PriorityNormal))

	ch := domain.ChannelSMS
	page1, more, err := s.ListNotifications(ctx, ListFilter{Channel: &ch, Limit: 3})
	if err != nil || len(page1) != 3 || !more {
		t.Fatalf("page1: %d more=%v err=%v", len(page1), more, err)
	}
	cursor := page1[2].ID
	page2, more, err := s.ListNotifications(ctx, ListFilter{Channel: &ch, Limit: 3, Cursor: &cursor})
	if err != nil || len(page2) != 2 || more {
		t.Fatalf("page2: %d more=%v err=%v", len(page2), more, err)
	}
	if page2[0].ID.String() >= page1[2].ID.String() {
		t.Fatal("pages must not overlap and must be newest first")
	}
}

func TestTemplates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	tpl := &domain.Template{ID: domain.NewID(), Name: "otp", Channel: domain.ChannelSMS, Content: "Code {{.code}}", CreatedAt: time.Now()}
	if _, err := s.CreateTemplate(ctx, tpl); err != nil {
		t.Fatal(err)
	}
	dup := *tpl
	dup.ID = domain.NewID()
	if _, err := s.CreateTemplate(ctx, &dup); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate name should conflict, got %v", err)
	}
	if err := s.DeleteTemplate(ctx, tpl.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetTemplate(ctx, tpl.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestStats(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, _, _ = s.CreateNotification(ctx, newNotification(domain.ChannelSMS, domain.PriorityNormal))
	backlog, err := s.BacklogCounts(ctx)
	if err != nil || backlog[domain.ChannelSMS][domain.StatusPending] != 1 {
		t.Fatalf("backlog: %v %v", backlog, err)
	}
	stats, err := s.DeliveryStats(ctx, time.Hour)
	if err != nil || stats[domain.ChannelSMS] == nil {
		t.Fatalf("stats: %v %v", stats, err)
	}
}
