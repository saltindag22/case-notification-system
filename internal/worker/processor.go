// Package worker contains the asynchronous processing engine: the dispatcher that
// feeds the broker from the PostgreSQL backlog, and the processor that delivers
// messages to the provider with rate limiting, retries and circuit breaking.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/saltindag22/case-notification-system/internal/circuitbreaker"
	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/events"
	"github.com/saltindag22/case-notification-system/internal/logging"
	"github.com/saltindag22/case-notification-system/internal/metrics"
	"github.com/saltindag22/case-notification-system/internal/provider"
	"github.com/saltindag22/case-notification-system/internal/ratelimit"
	"github.com/saltindag22/case-notification-system/internal/repository"
	"github.com/saltindag22/case-notification-system/internal/retry"
	"github.com/saltindag22/case-notification-system/internal/tracing"
)

// ProcessStore is the persistence port of the processor.
type ProcessStore interface {
	Claim(ctx context.Context, id uuid.UUID, attempts int) (*domain.Notification, error)
	Defer(ctx context.Context, id uuid.UUID, attempts int, until time.Time, reason string) (bool, error)
	Complete(ctx context.Context, id uuid.UUID, c repository.Completion) (*domain.Notification, error)
}

// Result is what happened to a message.
type Result string

// Processing results (also used as metric labels).
const (
	ResultSent     Result = "sent"
	ResultRetry    Result = "retry"
	ResultFailed   Result = "failed"
	ResultDeferred Result = "deferred" // circuit open: back to backlog without using an attempt
	ResultSkipped  Result = "skipped"  // stale message (cancelled, duplicate, already handled)
)

// Processor delivers a single notification.
type Processor struct {
	store    ProcessStore
	sender   provider.Sender
	limiter  ratelimit.Limiter
	breakers map[domain.Channel]*circuitbreaker.Breaker
	policy   retry.Policy
	events   events.Publisher
	metrics  *metrics.Metrics
	log      *slog.Logger
	tracer   trace.Tracer
	now      func() time.Time
}

// ProcessorDeps bundles the processor's collaborators.
type ProcessorDeps struct {
	Store    ProcessStore
	Sender   provider.Sender
	Limiter  ratelimit.Limiter
	Breakers map[domain.Channel]*circuitbreaker.Breaker
	Policy   retry.Policy
	Events   events.Publisher
	Metrics  *metrics.Metrics
	Log      *slog.Logger
}

// NewProcessor creates a processor.
func NewProcessor(d ProcessorDeps) *Processor {
	if d.Events == nil {
		d.Events = events.Nop{}
	}
	return &Processor{
		store: d.Store, sender: d.Sender, limiter: d.Limiter, breakers: d.Breakers, policy: d.Policy.Normalize(),
		events: d.Events, metrics: d.Metrics, log: d.Log, tracer: tracing.Tracer("worker"), now: time.Now,
	}
}

// Handle processes one broker message. ctx may be cancelled on shutdown: that only
// interrupts the waiting phase (circuit/rate limiter) before the notification is
// claimed. Once claimed, the attempt runs to completion so state is never lost.
// A returned error means the message should be requeued.
func (p *Processor) Handle(ctx context.Context, id uuid.UUID, ch domain.Channel, attempt int) (Result, error) {
	log := p.log.With("notification_id", id, "channel", ch, "attempt", attempt+1)
	ctx, span := p.tracer.Start(ctx, "notification.process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("notification.id", id.String()), attribute.String("notification.channel", string(ch)),
			attribute.Int("notification.attempt", attempt+1)))
	defer span.End()

	result, err := p.handle(ctx, log, id, ch, attempt)
	span.SetAttributes(attribute.String("notification.result", string(result)))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	if p.metrics != nil && result != "" {
		p.metrics.Deliveries.WithLabelValues(string(ch), string(result)).Inc()
	}
	return result, err
}

func (p *Processor) handle(ctx context.Context, log *slog.Logger, id uuid.UUID, ch domain.Channel, attempt int) (Result, error) {
	breaker := p.breakers[ch]
	if breaker != nil {
		if ok, wait := breaker.Allow(); !ok {
			until := p.now().Add(wait)
			deferred, err := p.store.Defer(context.WithoutCancel(ctx), id, attempt, until, "circuit breaker open for channel "+string(ch))
			if err != nil {
				return "", err
			}
			if !deferred {
				return ResultSkipped, nil
			}
			log.InfoContext(ctx, "circuit open, notification deferred", "until", until)
			return ResultDeferred, nil
		}
	}
	probing := breaker != nil

	waited, err := p.limiter.Wait(ctx, string(ch))
	if err != nil {
		if probing {
			breaker.Release()
		}
		return "", err
	}
	if p.metrics != nil {
		p.metrics.RateLimiterWait.WithLabelValues(string(ch)).Observe(waited.Seconds())
	}

	// From here on the attempt must finish even if shutdown starts.
	ctx = context.WithoutCancel(ctx)
	n, err := p.store.Claim(ctx, id, attempt)
	if err != nil {
		if probing {
			breaker.Release()
		}
		return "", err
	}
	if n == nil {
		if probing {
			breaker.Release()
		}
		log.DebugContext(ctx, "stale message skipped")
		return ResultSkipped, nil
	}
	p.events.Publish(ctx, events.FromNotification(n))
	if p.metrics != nil {
		p.metrics.InFlight.WithLabelValues(string(ch)).Inc()
		defer p.metrics.InFlight.WithLabelValues(string(ch)).Dec()
	}

	msg := provider.Message{
		ID: n.ID.String(), CorrelationID: n.CorrelationID, Channel: string(n.Channel),
		To: n.Recipient, Content: n.Content,
	}
	if n.Subject != nil {
		msg.Subject = *n.Subject
	}
	start := p.now()
	res, sendErr := p.sender.Send(ctx, msg)
	elapsed := p.now().Sub(start)

	attemptRec := domain.Attempt{AttemptNumber: n.Attempts, DurationMS: int(elapsed.Milliseconds())}
	var completion repository.Completion
	var result Result

	if sendErr == nil {
		if breaker != nil {
			breaker.Success()
		}
		code := res.StatusCode
		attemptRec.Outcome, attemptRec.HTTPStatus = domain.OutcomeSuccess, &code
		if res.MessageID != "" {
			attemptRec.ProviderMessageID = &res.MessageID
		}
		completion = repository.Completion{Status: domain.StatusSent, Attempt: attemptRec, ProviderMessageID: attemptRec.ProviderMessageID}
		result = ResultSent
	} else {
		var pe *provider.Error
		if !errors.As(sendErr, &pe) {
			pe = &provider.Error{Retryable: true, Message: sendErr.Error()}
		}
		errMsg := pe.Error()
		attemptRec.Error = &errMsg
		if pe.StatusCode != 0 {
			code := pe.StatusCode
			attemptRec.HTTPStatus = &code
		}
		if breaker != nil {
			if pe.Retryable {
				breaker.Failure()
			} else {
				breaker.Success() // the provider answered sanely; the request itself was bad
			}
		}
		decision := p.policy.Decide(n.Attempts, pe.Retryable, pe.RetryAfter)
		if pe.Retryable {
			attemptRec.Outcome = domain.OutcomeRetryableError
		} else {
			attemptRec.Outcome = domain.OutcomePermanentError
		}
		if decision.Retry {
			completion = repository.Completion{Status: domain.StatusRetrying, Attempt: attemptRec, NextAttemptAt: p.now().Add(decision.Delay), LastError: &errMsg}
			result = ResultRetry
			log.WarnContext(ctx, "delivery failed, retry scheduled", "error", errMsg, "retry_in", decision.Delay.String())
		} else {
			reason := errMsg
			if pe.Retryable {
				reason = "max attempts exhausted: " + errMsg
			}
			completion = repository.Completion{Status: domain.StatusFailed, Attempt: attemptRec, LastError: &reason}
			result = ResultFailed
			log.ErrorContext(ctx, "delivery failed permanently", "error", reason)
		}
	}
	if p.metrics != nil {
		p.metrics.ProviderLatency.WithLabelValues(string(ch), string(attemptRec.Outcome)).Observe(elapsed.Seconds())
	}

	updated, err := p.store.Complete(ctx, n.ID, completion)
	if err != nil {
		// The row stays in processing; the reaper will recover it.
		log.ErrorContext(ctx, "failed to persist delivery result", "error", err)
		return result, nil
	}
	if updated == nil {
		return result, nil
	}
	if result == ResultSent {
		log.InfoContext(ctx, "notification sent", "provider_message_id", res.MessageID, "duration_ms", attemptRec.DurationMS)
		if p.metrics != nil {
			origin := updated.CreatedAt
			if updated.ScheduledAt != nil {
				origin = *updated.ScheduledAt
			}
			if updated.SentAt != nil {
				p.metrics.EndToEndLatency.WithLabelValues(string(ch)).Observe(updated.SentAt.Sub(origin).Seconds())
			}
		}
	}
	p.events.Publish(ctx, events.FromNotification(updated))
	return result, nil
}

// withCorrelation is a tiny helper used by the consumer loop.
func withCorrelation(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return logging.WithCorrelationID(ctx, id)
}
