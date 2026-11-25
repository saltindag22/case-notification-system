// Package api is the HTTP interface: REST endpoints, WebSocket status stream,
// health checks, Prometheus metrics and the OpenAPI/Swagger documentation.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/service"
)

// NotificationService is the use-case port consumed by the handlers.
type NotificationService interface {
	Create(ctx context.Context, in service.CreateNotificationInput, idempotencyKey string) (*domain.Notification, bool, error)
	CreateBatch(ctx context.Context, in service.CreateBatchInput, idempotencyKey string) (*service.BatchOutcome, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.Notification, []domain.Attempt, error)
	GetBatch(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error)
	List(ctx context.Context, f repository.ListFilter) ([]*domain.Notification, bool, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	CancelBatch(ctx context.Context, id uuid.UUID) (int, error)
	Retry(ctx context.Context, id uuid.UUID) (*domain.Notification, error)

	CreateTemplate(ctx context.Context, in service.TemplateInput) (*domain.Template, error)
	UpdateTemplate(ctx context.Context, id uuid.UUID, in service.TemplateInput) (*domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	ListTemplates(ctx context.Context) ([]*domain.Template, error)
	DeleteTemplate(ctx context.Context, id uuid.UUID) error
}

// StatsSource feeds the JSON metrics endpoint.
type StatsSource interface {
	BacklogCounts(ctx context.Context) (map[domain.Channel]map[domain.Status]int64, error)
	DeliveryStats(ctx context.Context, window time.Duration) (map[domain.Channel]*repository.ChannelStats, error)
}

// Check is a named readiness dependency check.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Deps are the server's collaborators.
type Deps struct {
	Service     NotificationService
	Stats       StatsSource
	Checks      []Check
	Hub         *Hub
	Metrics     *metrics.Metrics
	Log         *slog.Logger
	OpenAPISpec []byte
	Version     string
}

// Server is the HTTP API.
type Server struct {
	svc     NotificationService
	stats   StatsSource
	checks  []Check
	hub     *Hub
	metrics *metrics.Metrics
	log     *slog.Logger
	spec    []byte
	version string
	mux     *http.ServeMux
	handler http.Handler
	started time.Time
}

// NewServer wires routes and middleware.
func NewServer(d Deps) *Server {
	s := &Server{
		svc: d.Service, stats: d.Stats, checks: d.Checks, hub: d.Hub, metrics: d.Metrics,
		log: d.Log, spec: d.OpenAPISpec, version: d.Version, mux: http.NewServeMux(), started: time.Now(),
	}

	s.route("POST /api/v1/notifications", s.createNotification)
	s.route("POST /api/v1/notifications/batch", s.createBatch)
	s.route("GET /api/v1/notifications", s.listNotifications)
	s.route("GET /api/v1/notifications/{id}", s.getNotification)
	s.route("POST /api/v1/notifications/{id}/cancel", s.cancelNotification)
	s.route("POST /api/v1/notifications/{id}/retry", s.retryNotification)

	s.route("GET /api/v1/batches/{id}", s.getBatch)
	s.route("GET /api/v1/batches/{id}/notifications", s.listBatchNotifications)
	s.route("POST /api/v1/batches/{id}/cancel", s.cancelBatch)

	s.route("POST /api/v1/templates", s.createTemplate)
	s.route("GET /api/v1/templates", s.listTemplates)
	s.route("GET /api/v1/templates/{id}", s.getTemplate)
	s.route("PUT /api/v1/templates/{id}", s.updateTemplate)
	s.route("DELETE /api/v1/templates/{id}", s.deleteTemplate)

	s.route("GET /api/v1/metrics", s.getMetrics)
	s.route("GET /api/v1", s.index)
	s.route("GET /api/v1/{$}", s.index)

	s.route("GET /health/live", s.live)
	s.route("GET /health/ready", s.ready)
	s.route("GET /openapi.yaml", s.openAPI)
	s.route("GET /swagger", s.swaggerUI)
	s.route("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger", http.StatusFound)
	})

	if d.Metrics != nil {
		s.mux.Handle("GET /metrics", promhttp.HandlerFor(d.Metrics.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	}
	if d.Hub != nil {
		// Not wrapped in otelhttp/recorder: the connection is hijacked and long lived.
		s.mux.HandleFunc("GET /ws", s.hub.ServeWS)
	}
	s.handler = s.recoverMiddleware(correlationMiddleware(routerErrorsAsJSON(s.mux)))
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }
