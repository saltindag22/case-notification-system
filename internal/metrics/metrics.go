// Package metrics defines the Prometheus instruments exposed on /metrics.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// Metrics groups every instrument. Each binary registers what it uses.
type Metrics struct {
	Registry *prometheus.Registry

	// API
	HTTPRequests         *prometheus.CounterVec
	HTTPDuration         *prometheus.HistogramVec
	NotificationsCreated *prometheus.CounterVec
	WebSocketClients     prometheus.Gauge

	// Worker
	Dispatched       *prometheus.CounterVec
	Deliveries       *prometheus.CounterVec   // channel, result: sent|retry|failed|deferred|skipped
	ProviderLatency  *prometheus.HistogramVec // channel, outcome
	EndToEndLatency  *prometheus.HistogramVec // channel
	RateLimiterWait  *prometheus.HistogramVec // channel
	BreakerState     *prometheus.GaugeVec     // channel
	BrokerQueueDepth *prometheus.GaugeVec     // queue
	InFlight         *prometheus.GaugeVec     // channel
	Recovered        *prometheus.CounterVec   // kind
}

// New creates and registers all instruments plus Go/process collectors.
func New(namespace string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	latencyBuckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	e2eBuckets := []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total", Help: "HTTP requests by route, method and status code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: latencyBuckets,
		}, []string{"method", "route"}),
		NotificationsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notifications_created_total", Help: "Notifications accepted by the API.",
		}, []string{"channel", "priority"}),
		WebSocketClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "websocket_clients", Help: "Connected WebSocket clients.",
		}),
		Dispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notifications_dispatched_total", Help: "Notifications moved from the backlog to the broker.",
		}, []string{"channel"}),
		Deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notification_deliveries_total", Help: "Processing results by channel.",
		}, []string{"channel", "result"}),
		ProviderLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "provider_request_duration_seconds", Help: "Latency of provider calls.", Buckets: latencyBuckets,
		}, []string{"channel", "outcome"}),
		EndToEndLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "notification_end_to_end_seconds", Help: "Time from creation (or schedule time) until sent.", Buckets: e2eBuckets,
		}, []string{"channel"}),
		RateLimiterWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "rate_limiter_wait_seconds", Help: "Time spent waiting for a rate limiter slot.", Buckets: latencyBuckets,
		}, []string{"channel"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "circuit_breaker_state", Help: "0=closed, 1=half-open, 2=open.",
		}, []string{"channel"}),
		BrokerQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "broker_queue_depth", Help: "Ready messages per broker queue.",
		}, []string{"queue"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "notifications_in_flight", Help: "Notifications currently being processed by this worker.",
		}, []string{"channel"}),
		Recovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "notifications_recovered_total", Help: "Rows repaired by the reaper.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.HTTPRequests, m.HTTPDuration, m.NotificationsCreated, m.WebSocketClients,
		m.Dispatched, m.Deliveries, m.ProviderLatency, m.EndToEndLatency, m.RateLimiterWait,
		m.BreakerState, m.BrokerQueueDepth, m.InFlight, m.Recovered)
	return m
}

// BacklogSource provides backlog counts for the backlog collector.
type BacklogSource interface {
	BacklogCounts(ctx context.Context) (map[domain.Channel]map[domain.Status]int64, error)
}

// backlogCollector queries PostgreSQL at scrape time, so the gauge is correct no
// matter how many API/worker replicas exist (no per-process drift).
type backlogCollector struct {
	src  BacklogSource
	desc *prometheus.Desc
	log  *slog.Logger
}

// RegisterBacklog exposes <ns>_notifications_backlog{channel,status}.
func (m *Metrics) RegisterBacklog(namespace string, src BacklogSource, log *slog.Logger) {
	m.Registry.MustRegister(&backlogCollector{
		src:  src,
		log:  log,
		desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "notifications_backlog"), "Non-terminal notifications by channel and status (queue depth).", []string{"channel", "status"}, nil),
	})
}

func (c *backlogCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *backlogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	counts, err := c.src.BacklogCounts(ctx)
	if err != nil {
		c.log.Warn("backlog collector failed", "error", err)
		return
	}
	for chn, byStatus := range counts {
		for st, n := range byStatus {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), string(chn), string(st))
		}
	}
}
