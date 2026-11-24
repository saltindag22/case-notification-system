package worker

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/queue"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/tracing"
)

// DispatchStore is the persistence port of the dispatcher.
type DispatchStore interface {
	DispatchDue(ctx context.Context, ch domain.Channel, limit int, publish func(context.Context, []repository.DispatchItem) error) (int, error)
	RecoverStuck(ctx context.Context, queuedAfter, processingAfter time.Duration) (int64, int64, error)
	AgePriorities(ctx context.Context, agingInterval time.Duration) (int64, error)
}

// Broker is the publishing port of the dispatcher.
type Broker interface {
	PublishBatch(ctx context.Context, msgs []queue.Outgoing) error
	QueueDepth(name string) (int, error)
}

// DispatcherConfig tunes the dispatcher.
type DispatcherConfig struct {
	Interval             time.Duration
	BatchSize            int
	MaxQueueDepth        int // per channel
	AgingInterval        time.Duration
	StuckQueuedAfter     time.Duration
	StuckProcessingAfter time.Duration
	MaintenanceInterval  time.Duration
}

// Dispatcher moves due notifications from PostgreSQL into the broker.
//
// PostgreSQL is the backlog; the broker is only a short, bounded buffer in front of
// the consumers (MaxQueueDepth per channel). Keeping the backlog in the database means:
//   - priority ordering (with aging) applies to the whole backlog, not just to what
//     happens to be in the broker;
//   - scheduled and retrying notifications need no broker-side delay mechanism;
//   - cancelling a notification is a single UPDATE and the broker never fills with
//     millions of messages during a flash sale.
type Dispatcher struct {
	store   DispatchStore
	broker  Broker
	events  events.Publisher
	cfg     DispatcherConfig
	metrics *metrics.Metrics
	log     *slog.Logger
	tracer  trace.Tracer
}

// NewDispatcher creates a dispatcher. pub (optional) receives a "queued" status
// event for every dispatched notification.
func NewDispatcher(store DispatchStore, broker Broker, pub events.Publisher, cfg DispatcherConfig, m *metrics.Metrics, log *slog.Logger) *Dispatcher {
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 30 * time.Second
	}
	if pub == nil {
		pub = events.Nop{}
	}
	return &Dispatcher{store: store, broker: broker, events: pub, cfg: cfg, metrics: m, log: log, tracer: tracing.Tracer("dispatcher")}
}

// Run blocks until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	tick := time.NewTicker(d.cfg.Interval)
	defer tick.Stop()
	maint := time.NewTicker(d.cfg.MaintenanceInterval)
	defer maint.Stop()
	d.maintain(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, ch := range domain.Channels {
				if err := d.DispatchChannel(ctx, ch); err != nil && ctx.Err() == nil {
					d.log.Error("dispatch failed", "channel", ch, "error", err)
				}
			}
		case <-maint.C:
			d.maintain(ctx)
		}
	}
}

// DispatchChannel fills the channel's broker queue up to MaxQueueDepth.
func (d *Dispatcher) DispatchChannel(ctx context.Context, ch domain.Channel) error {
	depth, err := d.broker.QueueDepth(queue.QueueName(ch))
	if err != nil {
		return err
	}
	if d.metrics != nil {
		d.metrics.BrokerQueueDepth.WithLabelValues(queue.QueueName(ch)).Set(float64(depth))
	}
	room := d.cfg.MaxQueueDepth - depth
	for room > 0 {
		limit := min(room, d.cfg.BatchSize)
		n, err := d.store.DispatchDue(ctx, ch, limit, d.publish)
		if err != nil {
			return err
		}
		if d.metrics != nil && n > 0 {
			d.metrics.Dispatched.WithLabelValues(string(ch)).Add(float64(n))
		}
		room -= n
		if n < limit {
			return nil
		}
	}
	return nil
}

func (d *Dispatcher) publish(ctx context.Context, items []repository.DispatchItem) error {
	msgs := make([]queue.Outgoing, len(items))
	for i, it := range items {
		// Continue the trace started by the API request that created the notification.
		parent := tracing.ContextWithTraceParent(ctx, it.TraceParent)
		spanCtx, span := d.tracer.Start(parent, "notification.dispatch", trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("notification.id", it.ID.String()), attribute.String("notification.channel", string(it.Channel))))
		headers := tracing.HeaderCarrier{}
		otel.GetTextMapPropagator().Inject(spanCtx, headers)
		span.End()

		msgs[i] = queue.Outgoing{
			Message:       queue.Message{NotificationID: it.ID, Channel: it.Channel, Attempt: it.Attempts},
			Priority:      it.Priority,
			CorrelationID: it.CorrelationID,
			Headers:       headers,
		}
	}
	// The "queued" events go out BEFORE the broker publish. Once a message is in
	// RabbitMQ an idle consumer may claim it immediately and emit "processing";
	// announcing "queued" afterwards would make WebSocket clients see the status
	// move backwards. The row is already committed as queued at this point.
	d.emit(ctx, items, func(repository.DispatchItem) domain.Status { return domain.StatusQueued })
	if err := d.broker.PublishBatch(ctx, msgs); err != nil {
		// The repository reverts the rows; tell subscribers the queued state did not stick.
		d.emit(ctx, items, func(it repository.DispatchItem) domain.Status { return it.PreviousStatus })
		return err
	}
	return nil
}

func (d *Dispatcher) emit(ctx context.Context, items []repository.DispatchItem, status func(repository.DispatchItem) domain.Status) {
	now := time.Now().UTC()
	for _, it := range items {
		d.events.Publish(ctx, events.Event{
			Type: "notification.status", NotificationID: it.ID, BatchID: it.BatchID, Channel: it.Channel,
			Status: status(it), Attempts: it.Attempts, CorrelationID: it.CorrelationID, Timestamp: now,
		})
	}
}

func (d *Dispatcher) maintain(ctx context.Context) {
	requeued, recovered, err := d.store.RecoverStuck(ctx, d.cfg.StuckQueuedAfter, d.cfg.StuckProcessingAfter)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Error("recover stuck notifications failed", "error", err)
		}
	} else if requeued+recovered > 0 {
		d.log.Warn("recovered stuck notifications", "requeued_queued", requeued, "recovered_processing", recovered)
		if d.metrics != nil {
			d.metrics.Recovered.WithLabelValues("queued").Add(float64(requeued))
			d.metrics.Recovered.WithLabelValues("processing").Add(float64(recovered))
		}
	}
	aged, err := d.store.AgePriorities(ctx, d.cfg.AgingInterval)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Error("priority aging failed", "error", err)
		}
	} else if aged > 0 {
		d.log.Info("promoted long waiting notifications", "count", aged)
	}
}
