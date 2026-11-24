package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/service"
)

// stubService lets each test define only the behaviour it needs.
type stubService struct {
	create      func(context.Context, service.CreateNotificationInput, string) (*domain.Notification, bool, error)
	createBatch func(context.Context, service.CreateBatchInput, string) (*service.BatchOutcome, error)
	get         func(context.Context, uuid.UUID) (*domain.Notification, []domain.Attempt, error)
	list        func(context.Context, repository.ListFilter) ([]*domain.Notification, bool, error)
	cancel      func(context.Context, uuid.UUID) (*domain.Notification, error)
	lastFilter  repository.ListFilter
}

func (s *stubService) Create(ctx context.Context, in service.CreateNotificationInput, k string) (*domain.Notification, bool, error) {
	return s.create(ctx, in, k)
}
func (s *stubService) CreateBatch(ctx context.Context, in service.CreateBatchInput, k string) (*service.BatchOutcome, error) {
	return s.createBatch(ctx, in, k)
}
func (s *stubService) Get(ctx context.Context, id uuid.UUID) (*domain.Notification, []domain.Attempt, error) {
	return s.get(ctx, id)
}
func (s *stubService) GetBatch(context.Context, uuid.UUID) (*domain.BatchSummary, error) {
	return nil, domain.ErrNotFound
}
func (s *stubService) List(ctx context.Context, f repository.ListFilter) ([]*domain.Notification, bool, error) {
	s.lastFilter = f
	return s.list(ctx, f)
}
func (s *stubService) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.cancel(ctx, id)
}
func (s *stubService) CancelBatch(context.Context, uuid.UUID) (int, error) { return 0, nil }
func (s *stubService) Retry(context.Context, uuid.UUID) (*domain.Notification, error) {
	return nil, domain.ErrNotFound
}
func (s *stubService) CreateTemplate(context.Context, service.TemplateInput) (*domain.Template, error) {
	return nil, domain.ErrConflict
}
func (s *stubService) UpdateTemplate(context.Context, uuid.UUID, service.TemplateInput) (*domain.Template, error) {
	return nil, domain.ErrNotFound
}
func (s *stubService) GetTemplate(context.Context, uuid.UUID) (*domain.Template, error) {
	return nil, domain.ErrNotFound
}
func (s *stubService) ListTemplates(context.Context) ([]*domain.Template, error) { return nil, nil }
func (s *stubService) DeleteTemplate(context.Context, uuid.UUID) error           { return nil }

type stubStats struct{}

func (stubStats) BacklogCounts(context.Context) (map[domain.Channel]map[domain.Status]int64, error) {
	return map[domain.Channel]map[domain.Status]int64{domain.ChannelSMS: {domain.StatusPending: 7, domain.StatusQueued: 3}}, nil
}
func (stubStats) DeliveryStats(context.Context, time.Duration) (map[domain.Channel]*repository.ChannelStats, error) {
	return map[domain.Channel]*repository.ChannelStats{domain.ChannelSMS: {Sent: 9, Failed: 1}}, nil
}

func newTestServer(svc *stubService, checks ...Check) (*Server, *Hub) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New("test")
	hub := NewHub(log, m, nil)
	return NewServer(Deps{Service: svc, Stats: stubStats{}, Checks: checks, Hub: hub, Metrics: m, Log: log, OpenAPISpec: []byte("openapi: 3.0.3"), Version: "test"}), hub
}

func do(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("invalid JSON response %q: %v", rec.Body.String(), err)
	}
	return m
}

func sample() *domain.Notification {
	return &domain.Notification{ID: domain.NewID(), Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", Priority: domain.PriorityHigh, Status: domain.StatusPending}
}

func TestCreateNotificationAccepted(t *testing.T) {
	var gotKey string
	svc := &stubService{create: func(_ context.Context, in service.CreateNotificationInput, k string) (*domain.Notification, bool, error) {
		gotKey = k
		if in.Channel != "sms" || in.Priority != "high" {
			t.Errorf("unexpected input %+v", in)
		}
		return sample(), true, nil
	}}
	srv, _ := newTestServer(svc)
	rec := do(t, srv, "POST", "/api/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"hi","priority":"high"}`,
		map[string]string{"Idempotency-Key": "k1", "X-Correlation-ID": "corr-123"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	if gotKey != "k1" {
		t.Fatalf("idempotency key not forwarded: %q", gotKey)
	}
	if rec.Header().Get("X-Correlation-ID") != "corr-123" || !strings.HasPrefix(rec.Header().Get("Location"), "/api/v1/notifications/") {
		t.Fatalf("missing headers: %v", rec.Header())
	}
	if b := decodeBody(t, rec); b["priority"] != "high" || b["status"] != "pending" {
		t.Fatalf("unexpected body %v", b)
	}
}

func TestCreateNotificationReplay(t *testing.T) {
	svc := &stubService{create: func(context.Context, service.CreateNotificationInput, string) (*domain.Notification, bool, error) {
		return sample(), false, nil
	}}
	srv, _ := newTestServer(svc)
	rec := do(t, srv, "POST", "/api/v1/notifications", `{"recipient":"x","channel":"sms","content":"hi"}`, nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay should be 200 with header, got %d %v", rec.Code, rec.Header())
	}
}

func TestCreateNotificationErrors(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		err    error
		status int
		code   string
	}{
		{"malformed json", `{"recipient":`, nil, 400, "invalid_request"},
		{"unknown field", `{"recipent":"x"}`, nil, 400, "invalid_request"},
		{"wrong type", `{"recipient":123}`, nil, 400, "invalid_request"},
		{"empty body", ``, nil, 400, "invalid_request"},
		{"two objects", `{} {}`, nil, 400, "invalid_request"},
		{"validation", `{}`, &domain.ValidationError{Fields: []domain.FieldError{{Field: "recipient", Message: "is required"}}}, 400, "validation_error"},
		{"idempotency conflict", `{}`, domain.ErrIdempotencyConflict, 409, "idempotency_conflict"},
		{"internal", `{}`, errors.New("db down"), 500, "internal_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &stubService{create: func(context.Context, service.CreateNotificationInput, string) (*domain.Notification, bool, error) {
				if tc.err == nil {
					t.Fatal("service must not be called for invalid requests")
				}
				return nil, false, tc.err
			}}
			srv, _ := newTestServer(svc)
			rec := do(t, srv, "POST", "/api/v1/notifications", tc.body, nil)
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d (%s)", rec.Code, tc.status, rec.Body)
			}
			b := decodeBody(t, rec)
			e := b["error"].(map[string]any)
			if e["code"] != tc.code || b["correlationId"] == "" {
				t.Fatalf("unexpected error body %v", b)
			}
			if tc.code == "internal_error" && strings.Contains(rec.Body.String(), "db down") {
				t.Fatal("internal error details must not leak")
			}
		})
	}
}

func TestCreateNotificationRejectsWrongContentType(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	req := httptest.NewRequest("POST", "/api/v1/notifications", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestBatchTooLarge(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	big := `{"notifications":[{"content":"` + strings.Repeat("a", maxBatchBody) + `"}]}`
	rec := do(t, srv, "POST", "/api/v1/notifications/batch", big, nil)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestGetNotificationIncludesAttempts(t *testing.T) {
	n := sample()
	svc := &stubService{get: func(context.Context, uuid.UUID) (*domain.Notification, []domain.Attempt, error) {
		return n, []domain.Attempt{{AttemptNumber: 1, Outcome: domain.OutcomeSuccess}}, nil
	}}
	srv, _ := newTestServer(svc)
	rec := do(t, srv, "GET", "/api/v1/notifications/"+n.ID.String(), "", nil)
	b := decodeBody(t, rec)
	if rec.Code != 200 || b["id"] != n.ID.String() || len(b["attemptHistory"].([]any)) != 1 {
		t.Fatalf("unexpected response %d %v", rec.Code, b)
	}
	if rec := do(t, srv, "GET", "/api/v1/notifications/not-a-uuid", "", nil); rec.Code != 400 {
		t.Fatalf("invalid id should be 400, got %d", rec.Code)
	}
}

func TestCancelConflict(t *testing.T) {
	svc := &stubService{cancel: func(context.Context, uuid.UUID) (*domain.Notification, error) {
		return nil, &domain.InvalidStateError{Current: domain.StatusSent, Action: "cancel"}
	}}
	srv, _ := newTestServer(svc)
	rec := do(t, srv, "POST", "/api/v1/notifications/"+uuid.NewString()+"/cancel", "", nil)
	if rec.Code != 409 || decodeBody(t, rec)["error"].(map[string]any)["code"] != "invalid_state" {
		t.Fatalf("expected 409 invalid_state, got %d %s", rec.Code, rec.Body)
	}
}

func TestListFiltersAndCursor(t *testing.T) {
	items := []*domain.Notification{sample(), sample()}
	svc := &stubService{list: func(context.Context, repository.ListFilter) ([]*domain.Notification, bool, error) {
		return items, true, nil
	}}
	srv, _ := newTestServer(svc)
	rec := do(t, srv, "GET", "/api/v1/notifications?status=sent&channel=email&from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&limit=2", "", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	f := svc.lastFilter
	if *f.Status != domain.StatusSent || *f.Channel != domain.ChannelEmail || f.Limit != 2 || f.From == nil || f.To == nil {
		t.Fatalf("filter not parsed: %+v", f)
	}
	b := decodeBody(t, rec)
	cursor := b["pagination"].(map[string]any)["nextCursor"].(string)

	rec = do(t, srv, "GET", "/api/v1/notifications?cursor="+cursor, "", nil)
	if rec.Code != 200 || *svc.lastFilter.Cursor != items[1].ID {
		t.Fatalf("cursor round trip failed: %d %v", rec.Code, svc.lastFilter.Cursor)
	}
}

func TestListValidation(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	for _, q := range []string{"status=delivered", "channel=fax", "limit=0", "limit=500", "from=yesterday", "cursor=abc", "from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		if rec := do(t, srv, "GET", "/api/v1/notifications?"+q, "", nil); rec.Code != 400 {
			t.Errorf("%s: expected 400, got %d", q, rec.Code)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	rec := do(t, srv, "GET", "/api/v1/metrics?window=10m", "", nil)
	b := decodeBody(t, rec)
	if rec.Code != 200 || b["window"] != "10m0s" {
		t.Fatalf("unexpected %d %v", rec.Code, b)
	}
	totals := b["totals"].(map[string]any)
	if totals["backlog"].(float64) != 10 || totals["successRate"].(float64) != 0.9 {
		t.Fatalf("unexpected totals %v", totals)
	}
	if rec := do(t, srv, "GET", "/api/v1/metrics?window=1s", "", nil); rec.Code != 400 {
		t.Fatal("too small window must be rejected")
	}
	if rec := do(t, srv, "GET", "/metrics", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "test_http_requests_total") {
		t.Fatal("prometheus endpoint must expose http metrics")
	}
}

func TestHealth(t *testing.T) {
	ok := Check{Name: "postgres", Fn: func(context.Context) error { return nil }}
	bad := Check{Name: "redis", Fn: func(context.Context) error { return errors.New("connection refused") }}
	srv, _ := newTestServer(&stubService{}, ok)
	if rec := do(t, srv, "GET", "/health/ready", "", nil); rec.Code != 200 {
		t.Fatalf("ready should be 200, got %d", rec.Code)
	}
	srv, _ = newTestServer(&stubService{}, ok, bad)
	rec := do(t, srv, "GET", "/health/ready", "", nil)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "connection refused") {
		t.Fatalf("ready should be 503, got %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, srv, "GET", "/health/live", "", nil); rec.Code != 200 {
		t.Fatal("liveness must not depend on dependencies")
	}
}

func TestCorrelationIDGeneratedWhenInvalid(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	rec := do(t, srv, "GET", "/health/live", "", map[string]string{"X-Correlation-ID": "bad id with spaces"})
	if id := rec.Header().Get("X-Correlation-ID"); len(id) != 36 {
		t.Fatalf("expected generated uuid, got %q", id)
	}
}

func TestUnknownRoute(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	rec := do(t, srv, "GET", "/nope", "", nil)
	if rec.Code != 404 || decodeBody(t, rec)["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("expected JSON 404, got %d %s", rec.Code, rec.Body)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	rec := do(t, srv, "DELETE", "/api/v1/notifications", "", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Header().Get("Allow"), "POST") || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("405 must be JSON and list allowed methods, headers %v", rec.Header())
	}
	if decodeBody(t, rec)["error"].(map[string]any)["code"] != "method_not_allowed" {
		t.Fatalf("unexpected body %s", rec.Body)
	}
}

func TestBadRequestMessageHasNoPrefix(t *testing.T) {
	srv, _ := newTestServer(&stubService{})
	rec := do(t, srv, "POST", "/api/v1/notifications", `{"chanel":"sms"}`, nil)
	msg := decodeBody(t, rec)["error"].(map[string]any)["message"].(string)
	if rec.Code != 400 || msg != `invalid JSON: unknown field "chanel"` {
		t.Fatalf("unexpected message %q", msg)
	}
}

func TestWebSocketFiltering(t *testing.T) {
	srv, hub := newTestServer(&stubService{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	wanted, other := domain.NewID(), domain.NewID()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?notificationId=" + wanted.String()
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var welcome map[string]any
	if err := conn.ReadJSON(&welcome); err != nil || welcome["type"] != "connected" {
		t.Fatalf("welcome: %v %v", welcome, err)
	}
	for hub.Clients() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	hub.Broadcast(events.Event{Type: "notification.status", NotificationID: other, Status: domain.StatusSent})
	hub.Broadcast(events.Event{Type: "notification.status", NotificationID: wanted, Status: domain.StatusSent})

	var got events.Event
	if err := conn.ReadJSON(&got); err != nil {
		t.Fatal(err)
	}
	if got.NotificationID != wanted {
		t.Fatalf("received event for unsubscribed notification %s", got.NotificationID)
	}

	// Dynamic subscription.
	batch := domain.NewID()
	_ = conn.WriteJSON(map[string]any{"action": "subscribe", "batchIds": []string{batch.String()}})
	var ack map[string]any
	if err := conn.ReadJSON(&ack); err != nil || ack["type"] != "ack" {
		t.Fatalf("ack: %v %v", ack, err)
	}
	hub.Broadcast(events.Event{Type: "notification.status", NotificationID: other, BatchID: &batch, Status: domain.StatusQueued})
	if err := conn.ReadJSON(&got); err != nil || got.Status != domain.StatusQueued {
		t.Fatalf("batch subscription failed: %v %+v", err, got)
	}
}
