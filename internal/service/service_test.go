package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/repository"
)

// fakeStore is an in-memory Store good enough for use-case tests.
type fakeStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]*domain.Notification
	byKey     map[string]*domain.Notification
	templates map[uuid.UUID]*domain.Template
	batches   []*domain.Batch
}

func newFakeStore() *fakeStore {
	return &fakeStore{byID: map[uuid.UUID]*domain.Notification{}, byKey: map[string]*domain.Notification{}, templates: map[uuid.UUID]*domain.Template{}}
}

func (f *fakeStore) CreateNotification(_ context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n.IdempotencyKey != nil {
		if ex, ok := f.byKey[*n.IdempotencyKey]; ok {
			if ex.RequestHash != n.RequestHash {
				return ex, false, domain.ErrIdempotencyConflict
			}
			return ex, false, nil
		}
		f.byKey[*n.IdempotencyKey] = n
	}
	f.byID[n.ID] = n
	return n, true, nil
}

func (f *fakeStore) CreateBatch(ctx context.Context, b *domain.Batch, items []*domain.Notification) (*repository.BatchResult, error) {
	f.batches = append(f.batches, b)
	res := &repository.BatchResult{Batch: b}
	for _, n := range items {
		stored, created, err := f.CreateNotification(ctx, n)
		if err != nil {
			return nil, err
		}
		res.Items = append(res.Items, repository.BatchItemResult{Notification: stored, Created: created})
	}
	return res, nil
}

func (f *fakeStore) GetNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	if n, ok := f.byID[id]; ok {
		return n, nil
	}
	return nil, domain.ErrNotFound
}
func (f *fakeStore) ListAttempts(context.Context, uuid.UUID) ([]domain.Attempt, error) {
	return nil, nil
}
func (f *fakeStore) GetBatchSummary(context.Context, uuid.UUID) (*domain.BatchSummary, error) {
	return nil, domain.ErrNotFound
}
func (f *fakeStore) ListNotifications(context.Context, repository.ListFilter) ([]*domain.Notification, bool, error) {
	return nil, false, nil
}
func (f *fakeStore) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, ok := f.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !n.Status.IsCancellable() {
		return n, &domain.InvalidStateError{Current: n.Status, Action: "cancel"}
	}
	n.Status = domain.StatusCancelled
	return n, nil
}
func (f *fakeStore) CancelBatch(context.Context, uuid.UUID) ([]*domain.Notification, error) {
	return nil, nil
}
func (f *fakeStore) Requeue(context.Context, uuid.UUID) (*domain.Notification, error) {
	return nil, domain.ErrNotFound
}
func (f *fakeStore) CreateTemplate(_ context.Context, t *domain.Template) (*domain.Template, error) {
	f.templates[t.ID] = t
	return t, nil
}
func (f *fakeStore) GetTemplate(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	if t, ok := f.templates[id]; ok {
		return t, nil
	}
	return nil, domain.ErrNotFound
}
func (f *fakeStore) ListTemplates(context.Context) ([]*domain.Template, error) { return nil, nil }
func (f *fakeStore) UpdateTemplate(_ context.Context, t *domain.Template) (*domain.Template, error) {
	return t, nil
}
func (f *fakeStore) DeleteTemplate(context.Context, uuid.UUID) error { return nil }

type recordingPublisher struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *recordingPublisher) Publish(_ context.Context, e events.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func newTestService() (*Service, *fakeStore, *recordingPublisher) {
	store := newFakeStore()
	pub := &recordingPublisher{}
	svc := New(store, pub, 5)
	svc.now = func() time.Time { return time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC) }
	return svc, store, pub
}

func sms(content string) CreateNotificationInput {
	return CreateNotificationInput{Recipient: "+905551234567", Channel: "sms", Content: content}
}

func fieldErrors(t *testing.T, err error) map[string]string {
	t.Helper()
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected validation error, got %v", err)
	}
	out := map[string]string{}
	for _, f := range ve.Fields {
		out[f.Field] = f.Message
	}
	return out
}

func TestCreateDefaultsAndEvent(t *testing.T) {
	svc, _, pub := newTestService()
	ctx := logging.WithCorrelationID(context.Background(), "corr-1")
	n, created, err := svc.Create(ctx, sms("hello"), "")
	if err != nil || !created {
		t.Fatalf("create: %v", err)
	}
	if n.Priority != domain.PriorityNormal || n.Status != domain.StatusPending || n.MaxAttempts != 5 || n.CorrelationID != "corr-1" {
		t.Fatalf("unexpected defaults %+v", n)
	}
	if len(pub.events) != 1 || pub.events[0].Status != domain.StatusPending {
		t.Fatalf("expected one pending event, got %+v", pub.events)
	}
}

func TestCreateValidationCollectsAllErrors(t *testing.T) {
	svc, _, _ := newTestService()
	_, _, err := svc.Create(context.Background(), CreateNotificationInput{Channel: "fax", Priority: "urgent"}, "")
	fe := fieldErrors(t, err)
	for _, f := range []string{"channel", "priority"} {
		if _, ok := fe[f]; !ok {
			t.Errorf("missing error for %s in %v", f, fe)
		}
	}
}

func TestCreateScheduled(t *testing.T) {
	svc, _, _ := newTestService()
	in := sms("later")
	at := svc.now().Add(time.Hour)
	in.ScheduledAt = &at
	n, _, err := svc.Create(context.Background(), in, "")
	if err != nil || n.Status != domain.StatusScheduled || !n.ScheduledAt.Equal(at) {
		t.Fatalf("scheduled: %+v %v", n, err)
	}
	past := svc.now().Add(-time.Hour)
	in.ScheduledAt = &past
	_, _, err = svc.Create(context.Background(), in, "")
	if _, ok := fieldErrors(t, err)["scheduledAt"]; !ok {
		t.Fatal("past schedule must be rejected")
	}
}

func TestIdempotency(t *testing.T) {
	svc, _, pub := newTestService()
	ctx := context.Background()
	a, created, err := svc.Create(ctx, sms("hi"), "key-1")
	if err != nil || !created {
		t.Fatal(err)
	}
	b, created, err := svc.Create(ctx, sms("hi"), "key-1")
	if err != nil || created || a.ID != b.ID {
		t.Fatalf("replay must return the original: %v %v", created, err)
	}
	if len(pub.events) != 1 {
		t.Fatal("replay must not emit another event")
	}
	if _, _, err := svc.Create(ctx, sms("different"), "key-1"); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("payload change must conflict, got %v", err)
	}
	in := sms("hi")
	in.IdempotencyKey = "body-key"
	if _, _, err := svc.Create(ctx, in, "header-key"); err == nil {
		t.Fatal("header/body key mismatch must be rejected")
	}
}

func TestFingerprintIgnoresMetadataFormatting(t *testing.T) {
	svc, _, _ := newTestService()
	a := sms("x")
	a.Metadata = []byte(`{"b":1, "a":2}`)
	b := sms("x")
	b.Metadata = []byte(`{"a":2,"b":1}`)
	na, err1 := svc.build(context.Background(), a, templateCache{})
	nb, err2 := svc.build(context.Background(), b, templateCache{})
	if err1 != nil || err2 != nil || na.RequestHash != nb.RequestHash {
		t.Fatal("semantically equal metadata must produce the same fingerprint")
	}
}

func TestTemplateRendering(t *testing.T) {
	svc, store, _ := newTestService()
	subj := "Welcome {{.name}}"
	tpl, err := svc.CreateTemplate(context.Background(), TemplateInput{Name: "welcome", Channel: "email", Subject: &subj, Content: "Hi {{.name}}!"})
	if err != nil {
		t.Fatal(err)
	}
	_ = store
	id := tpl.ID.String()
	n, _, err := svc.Create(context.Background(), CreateNotificationInput{
		Recipient: "a@example.com", TemplateID: &id, Variables: map[string]any{"name": "Ali"},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if n.Channel != domain.ChannelEmail || n.Content != "Hi Ali!" || *n.Subject != "Welcome Ali" || *n.TemplateID != tpl.ID {
		t.Fatalf("unexpected rendering %+v", n)
	}

	_, _, err = svc.Create(context.Background(), CreateNotificationInput{Recipient: "a@example.com", TemplateID: &id}, "")
	if _, ok := fieldErrors(t, err)["variables"]; !ok {
		t.Fatal("missing variable must be reported")
	}
	_, _, err = svc.Create(context.Background(), CreateNotificationInput{Recipient: "+905551234567", Channel: "sms", TemplateID: &id, Variables: map[string]any{"name": "x"}}, "")
	if _, ok := fieldErrors(t, err)["templateId"]; !ok {
		t.Fatal("channel mismatch must be reported")
	}
	bad := "<id>"
	_, _, err = svc.Create(context.Background(), CreateNotificationInput{Recipient: "a@example.com", TemplateID: &bad}, "")
	if fe := fieldErrors(t, err); fe["templateId"] == "" || fe["channel"] != "" {
		t.Fatalf("invalid templateId must be the only error, got %v", fe)
	}
	_, _, err = svc.Create(context.Background(), CreateNotificationInput{Recipient: "a@example.com", TemplateID: &id, Content: "x"}, "")
	if _, ok := fieldErrors(t, err)["content"]; !ok {
		t.Fatal("content together with templateId must be rejected")
	}
}

func TestTemplateValidation(t *testing.T) {
	svc, _, _ := newTestService()
	_, err := svc.CreateTemplate(context.Background(), TemplateInput{Name: "Bad Name", Channel: "email", Content: "{{.x"})
	fe := fieldErrors(t, err)
	for _, f := range []string{"name", "content", "subject"} {
		if _, ok := fe[f]; !ok {
			t.Errorf("expected error on %s, got %v", f, fe)
		}
	}
}

func TestBatch(t *testing.T) {
	svc, _, pub := newTestService()
	ctx := context.Background()
	in := CreateBatchInput{Notifications: []CreateNotificationInput{sms("a"), sms("b"), sms("c")}}
	out, err := svc.CreateBatch(ctx, in, "")
	if err != nil || out.Total != 3 || out.Created != 3 || len(pub.events) != 3 {
		t.Fatalf("batch: %+v %v", out, err)
	}
	for _, it := range out.Items {
		if it.Notification.BatchID == nil || *it.Notification.BatchID != out.BatchID {
			t.Fatal("every item must carry the batch id")
		}
	}
}

func TestBatchValidation(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()

	if _, err := svc.CreateBatch(ctx, CreateBatchInput{}, ""); err == nil {
		t.Fatal("empty batch must be rejected")
	}
	big := CreateBatchInput{}
	for i := 0; i <= MaxBatchSize; i++ {
		big.Notifications = append(big.Notifications, sms("x"))
	}
	if _, err := svc.CreateBatch(ctx, big, ""); err == nil || !strings.Contains(err.Error(), "at most 1000") {
		t.Fatalf("oversized batch must be rejected, got %v", err)
	}

	a, b := sms("a"), sms("b")
	a.IdempotencyKey, b.IdempotencyKey = "same", "same"
	bad := CreateBatchInput{Notifications: []CreateNotificationInput{a, {Channel: "sms", Recipient: "123", Content: "x"}, b}}
	_, err := svc.CreateBatch(ctx, bad, "")
	fe := fieldErrors(t, err)
	if _, ok := fe["notifications[1].recipient"]; !ok {
		t.Errorf("expected indexed recipient error, got %v", fe)
	}
	if _, ok := fe["notifications[2].idempotencyKey"]; !ok {
		t.Errorf("expected duplicate key error, got %v", fe)
	}
}

func TestCancel(t *testing.T) {
	svc, _, pub := newTestService()
	n, _, _ := svc.Create(context.Background(), sms("x"), "")
	if _, err := svc.Cancel(context.Background(), n.ID); err != nil {
		t.Fatal(err)
	}
	if last := pub.events[len(pub.events)-1]; last.Status != domain.StatusCancelled {
		t.Fatalf("expected cancelled event, got %s", last.Status)
	}
	if _, err := svc.Cancel(context.Background(), n.ID); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("expected invalid state, got %v", err)
	}
}
