package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/queue"
	"github.com/saltindag22/case-notification-system/internal/tracing"
)

// Consumer is the broker port used by the consumer pool.
type Consumer interface {
	Consume(ctx context.Context, ch domain.Channel, prefetch int, consumerTag string) (<-chan queue.Delivery, func() error, error)
}

// Pool runs a fixed number of goroutines per channel. Channels have independent
// pools, so a slow or broken provider for one channel cannot starve the others.
type Pool struct {
	consumer    Consumer
	processor   *Processor
	concurrency int
	log         *slog.Logger
}

// NewPool creates a consumer pool.
func NewPool(c Consumer, p *Processor, concurrency int, log *slog.Logger) *Pool {
	return &Pool{consumer: c, processor: p, concurrency: concurrency, log: log}
}

// Run consumes until ctx is cancelled, then waits for in-flight messages to finish
// (graceful shutdown) before closing the AMQP channels.
func (p *Pool) Run(ctx context.Context) error {
	host, _ := os.Hostname()
	var wg sync.WaitGroup
	var closers []func() error
	for _, ch := range domain.Channels {
		deliveries, closeFn, err := p.consumer.Consume(ctx, ch, p.concurrency, fmt.Sprintf("%s-%s-%d", host, ch, os.Getpid()))
		if err != nil {
			return fmt.Errorf("consume %s: %w", ch, err)
		}
		closers = append(closers, closeFn)
		for i := 0; i < p.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for d := range deliveries {
					p.handle(ctx, d)
				}
			}()
		}
	}
	p.log.Info("consumers started", "channels", len(domain.Channels), "concurrency_per_channel", p.concurrency)
	<-ctx.Done()
	p.log.Info("stopping consumers, waiting for in-flight messages")
	wg.Wait()
	for _, c := range closers {
		_ = c()
	}
	return nil
}

func (p *Pool) handle(ctx context.Context, d queue.Delivery) {
	// Continue the trace from the dispatcher (W3C traceparent in AMQP headers).
	ctx = otel.GetTextMapPropagator().Extract(ctx, tracing.HeaderCarrier(d.Headers))
	ctx = withCorrelation(ctx, d.CorrelationID())

	_, err := p.processor.Handle(ctx, d.Message.NotificationID, d.Message.Channel, d.Message.Attempt)
	if err != nil {
		if ctx.Err() == nil {
			p.log.ErrorContext(ctx, "processing failed, message requeued", "notification_id", d.Message.NotificationID, "error", err)
			// Avoid a hot redelivery loop while a dependency (e.g. Redis) is down.
			time.Sleep(500 * time.Millisecond)
		}
		_ = d.Requeue()
		return
	}
	if err := d.Ack(); err != nil {
		p.log.WarnContext(ctx, "ack failed", "notification_id", d.Message.NotificationID, "error", err)
	}
}
