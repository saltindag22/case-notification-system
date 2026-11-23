package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/circuitbreaker"
	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/provider"
	"github.com/saltindag22/case-notification-system/internal/queue"
	"github.com/saltindag22/case-notification-system/internal/ratelimit"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/retry"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// memStore emulates the conditional transitions of the real repository.
type memStore struct {
	mu          sync.Mutex
	n           *domain.Notification
	completions []repository.Completion
	deferred    bool
}

func newMemStore(attempts int) *memStore {
	return &memStore{n: &domain.Notification{
		ID: domain.NewID(), Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi",
		Status: domain.StatusQueued, Attempts: attempts, MaxAttempts: 3, CreatedAt: time.Now(),
	}}
}

func (s *memStore) Claim(_ context.Context, id uuid.UUID, attempts int) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n.ID != id || s.n.Status != domain.StatusQueued || s.n.Attempts != attempts {
		return nil, nil
	}
	s.n.Status = domain.StatusProcessing
	s.n.Attempts++
	cp := *s.n
	return &cp, nil
}

func (s *memStore) Defer(_ context.Context, id uuid.UUID, attempts int, _ time.Time, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n.Status != domain.StatusQueued || s.n.Attempts != attempts {
		return false, nil
	}
	s.n.Status = domain.StatusRetrying
	s.deferred = true
	return true, nil
}

func (s *memStore) Complete(_ context.Context, _ uuid.UUID, c repository.Completion) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completions = append(s.completions, c)
	s.n.Status = c.Status
	if c.Status == domain.StatusSent {
		now := time.Now()
		s.n.SentAt = &now
	}
	cp := *s.n
	return &cp, nil
}

type stubSender struct {
	res   provider.Result
	err   error
	calls int
	last  provider.Message
}

func (s *stubSender) Send(_ context.Context, m provider.Message) (provider.Result, error) {
	s.calls++
	s.last = m
	return s.res, s.err
}

func newProcessor(store ProcessStore, sender provider.Sender, breaker *circuitbreaker.Breaker) *Processor {
	breakers := map[domain.Channel]*circuitbreaker.Breaker{}
	if breaker != nil {
		breakers[domain.ChannelSMS] = breaker
	}
	return NewProcessor(ProcessorDeps{
		Store: store, Sender: sender, Limiter: ratelimit.NewLocal(1000, 10), Breakers: breakers,
		Policy:  retry.Policy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute, Multiplier: 2, Jitter: 0},
		Metrics: metrics.New("test"), Log: discard,
	})
}

func TestProcessSuccess(t *testing.T) {
	store := newMemStore(0)
	sender := &stubSender{res: provider.Result{MessageID: "m-1", StatusCode: 202}}
	p := newProcessor(store, sender, nil)

	res, err := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if err != nil || res != ResultSent {
		t.Fatalf("got %s %v", res, err)
	}
	c := store.completions[0]
	if c.Status != domain.StatusSent || *c.ProviderMessageID != "m-1" || c.Attempt.Outcome != domain.OutcomeSuccess || c.Attempt.AttemptNumber != 1 {
		t.Fatalf("unexpected completion %+v", c)
	}
	if sender.last.ID != store.n.ID.String() || sender.last.To != "+905551234567" {
		t.Fatalf("unexpected provider message %+v", sender.last)
	}
}

func TestProcessRetryableErrorSchedulesRetry(t *testing.T) {
	store := newMemStore(0)
	sender := &stubSender{err: &provider.Error{StatusCode: 503, Retryable: true, Message: "down"}}
	p := newProcessor(store, sender, nil)
	before := time.Now()

	res, _ := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if res != ResultRetry {
		t.Fatalf("expected retry, got %s", res)
	}
	c := store.completions[0]
	if c.Status != domain.StatusRetrying || c.Attempt.Outcome != domain.OutcomeRetryableError || *c.Attempt.HTTPStatus != 503 {
		t.Fatalf("unexpected completion %+v", c)
	}
	if d := c.NextAttemptAt.Sub(before); d < 900*time.Millisecond || d > 2*time.Second {
		t.Fatalf("first retry should be ~1s later, got %v", d)
	}
}

func TestProcessRetryAfterIsHonoured(t *testing.T) {
	store := newMemStore(0)
	sender := &stubSender{err: &provider.Error{StatusCode: 429, Retryable: true, RetryAfter: 20 * time.Second}}
	p := newProcessor(store, sender, nil)
	before := time.Now()
	_, _ = p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if d := store.completions[0].NextAttemptAt.Sub(before); d < 19*time.Second {
		t.Fatalf("Retry-After ignored: %v", d)
	}
}

func TestProcessExhaustedAttemptsFail(t *testing.T) {
	store := newMemStore(2) // third and last attempt
	sender := &stubSender{err: &provider.Error{StatusCode: 500, Retryable: true, Message: "boom"}}
	p := newProcessor(store, sender, nil)
	res, _ := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 2)
	if res != ResultFailed || store.completions[0].Status != domain.StatusFailed {
		t.Fatalf("expected failed, got %s", res)
	}
}

func TestProcessPermanentErrorFailsImmediately(t *testing.T) {
	store := newMemStore(0)
	sender := &stubSender{err: &provider.Error{StatusCode: 400, Retryable: false, Message: "bad number"}}
	breaker := circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 1, OpenTimeout: time.Minute}, nil)
	p := newProcessor(store, sender, breaker)
	res, _ := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if res != ResultFailed || store.completions[0].Attempt.Outcome != domain.OutcomePermanentError {
		t.Fatalf("expected permanent failure, got %s", res)
	}
	if breaker.State() != circuitbreaker.StateClosed {
		t.Fatal("a 4xx must not trip the circuit breaker")
	}
}

func TestProcessStaleMessageSkipped(t *testing.T) {
	store := newMemStore(0)
	store.n.Status = domain.StatusCancelled
	sender := &stubSender{}
	p := newProcessor(store, sender, nil)
	res, err := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if err != nil || res != ResultSkipped || sender.calls != 0 {
		t.Fatalf("cancelled notification must not be sent: %s %v calls=%d", res, err, sender.calls)
	}
}

func TestProcessOpenBreakerDefersWithoutAttempt(t *testing.T) {
	breaker := circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 1, OpenTimeout: time.Minute}, nil)
	breaker.Failure() // open
	store := newMemStore(0)
	sender := &stubSender{}
	p := newProcessor(store, sender, breaker)
	res, err := p.Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	if err != nil || res != ResultDeferred || !store.deferred || store.n.Attempts != 0 || sender.calls != 0 {
		t.Fatalf("expected deferral without consuming an attempt: %s %v attempts=%d", res, err, store.n.Attempts)
	}
}

func TestProcessRepeatedFailuresOpenBreaker(t *testing.T) {
	breaker := circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 2, OpenTimeout: time.Minute}, nil)
	sender := &stubSender{err: &provider.Error{StatusCode: 502, Retryable: true}}
	for i := 0; i < 2; i++ {
		store := newMemStore(0)
		_, _ = newProcessor(store, sender, breaker).Handle(context.Background(), store.n.ID, domain.ChannelSMS, 0)
	}
	if breaker.State() != circuitbreaker.StateOpen {
		t.Fatal("breaker should open after consecutive provider failures")
	}
}

func TestProcessLimiterCancellationRequeues(t *testing.T) {
	store := newMemStore(0)
	p := newProcessor(store, &stubSender{}, nil)
	p.limiter = ratelimit.NewLocal(1, 1)
	_, _ = p.limiter.Wait(context.Background(), "sms") // exhaust the bucket
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Handle(ctx, store.n.ID, domain.ChannelSMS, 0); err == nil {
		t.Fatal("expected error so the message is requeued")
	}
	if store.n.Status != domain.StatusQueued || store.n.Attempts != 0 {
		t.Fatal("notification must stay untouched when shutdown interrupts the wait")
	}
}

// --- dispatcher ---

type fakeDispatchStore struct {
	due      map[domain.Channel]int
	maxLimit int
}

func (f *fakeDispatchStore) DispatchDue(ctx context.Context, ch domain.Channel, limit int, publish func(context.Context, []repository.DispatchItem) error) (int, error) {
	if limit > f.maxLimit {
		f.maxLimit = limit
	}
	n := min(limit, f.due[ch])
	items := make([]repository.DispatchItem, n)
	for i := range items {
		items[i] = repository.DispatchItem{ID: domain.NewID(), Channel: ch, Priority: domain.PriorityHigh}
	}
	if n > 0 {
		if err := publish(ctx, items); err != nil {
			return 0, err
		}
	}
	f.due[ch] -= n
	return n, nil
}
func (f *fakeDispatchStore) RecoverStuck(context.Context, time.Duration, time.Duration) (int64, int64, error) {
	return 0, 0, nil
}
func (f *fakeDispatchStore) AgePriorities(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

type fakeBroker struct {
	depth     int
	published []queue.Outgoing
	fail      bool
}

func (b *fakeBroker) PublishBatch(_ context.Context, msgs []queue.Outgoing) error {
	if b.fail {
		return errors.New("broker down")
	}
	b.published = append(b.published, msgs...)
	return nil
}
func (b *fakeBroker) QueueDepth(string) (int, error) { return b.depth, nil }

func TestDispatcherRespectsQueueDepthLimit(t *testing.T) {
	store := &fakeDispatchStore{due: map[domain.Channel]int{domain.ChannelSMS: 10_000}}
	broker := &fakeBroker{depth: 1500}
	d := NewDispatcher(store, broker, DispatcherConfig{BatchSize: 200, MaxQueueDepth: 2000}, metrics.New("test"), discard)
	if err := d.DispatchChannel(context.Background(), domain.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	if len(broker.published) != 500 {
		t.Fatalf("should fill only up to the depth limit (500), published %d", len(broker.published))
	}
	if store.maxLimit > 200 {
		t.Fatalf("batch size exceeded: %d", store.maxLimit)
	}
	if broker.published[0].Priority != domain.PriorityHigh || broker.published[0].Headers == nil {
		t.Fatal("priority and trace headers must be propagated")
	}
}

func TestDispatcherStopsWhenBacklogEmpty(t *testing.T) {
	store := &fakeDispatchStore{due: map[domain.Channel]int{domain.ChannelEmail: 3}}
	broker := &fakeBroker{}
	d := NewDispatcher(store, broker, DispatcherConfig{BatchSize: 100, MaxQueueDepth: 2000}, nil, discard)
	if err := d.DispatchChannel(context.Background(), domain.ChannelEmail); err != nil || len(broker.published) != 3 {
		t.Fatalf("published %d err %v", len(broker.published), err)
	}
}

func TestDispatcherPublishFailureKeepsBacklog(t *testing.T) {
	store := &fakeDispatchStore{due: map[domain.Channel]int{domain.ChannelPush: 5}}
	d := NewDispatcher(store, &fakeBroker{fail: true}, DispatcherConfig{BatchSize: 100, MaxQueueDepth: 2000}, nil, discard)
	if err := d.DispatchChannel(context.Background(), domain.ChannelPush); err == nil {
		t.Fatal("expected error")
	}
	if store.due[domain.ChannelPush] != 5 {
		t.Fatal("failed publish must leave rows due")
	}
}
