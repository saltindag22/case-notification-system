// Package events broadcasts notification status changes.
//
// The worker and the API publish every state transition to a Redis Pub/Sub
// channel; each API replica subscribes and fans the events out to its WebSocket
// clients. Pub/Sub is fire-and-forget by design: events are a real-time view,
// the database stays the source of truth (clients can always GET the status).
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// Channel is the Redis Pub/Sub channel name.
const Channel = "notifications:events"

// Event is a status change.
type Event struct {
	Type           string         `json:"type"` // always "notification.status"
	NotificationID uuid.UUID      `json:"notificationId"`
	BatchID        *uuid.UUID     `json:"batchId,omitempty"`
	Channel        domain.Channel `json:"channel"`
	Status         domain.Status  `json:"status"`
	Attempts       int            `json:"attempts"`
	Error          *string        `json:"error,omitempty"`
	CorrelationID  string         `json:"correlationId,omitempty"`
	Timestamp      time.Time      `json:"timestamp"`
}

// FromNotification builds the event for n's current state.
func FromNotification(n *domain.Notification) Event {
	return Event{
		Type:           "notification.status",
		NotificationID: n.ID,
		BatchID:        n.BatchID,
		Channel:        n.Channel,
		Status:         n.Status,
		Attempts:       n.Attempts,
		Error:          n.LastError,
		CorrelationID:  n.CorrelationID,
		Timestamp:      time.Now().UTC(),
	}
}

// Publisher emits events. Implementations must not fail the caller's operation.
type Publisher interface {
	Publish(ctx context.Context, e Event)
}

// Nop discards events.
type Nop struct{}

// Publish does nothing.
func (Nop) Publish(context.Context, Event) {}

// RedisBus publishes to and subscribes from Redis Pub/Sub.
type RedisBus struct {
	client redis.UniversalClient
	log    *slog.Logger
}

// NewRedisBus creates a bus.
func NewRedisBus(client redis.UniversalClient, log *slog.Logger) *RedisBus {
	return &RedisBus{client: client, log: log}
}

// Publish sends e; errors are logged, never returned.
func (b *RedisBus) Publish(ctx context.Context, e Event) {
	payload, err := json.Marshal(e)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := b.client.Publish(ctx, Channel, payload).Err(); err != nil {
		b.log.WarnContext(ctx, "publish status event failed", "error", err, "notification_id", e.NotificationID)
	}
}

// Subscribe invokes fn for each event until ctx is done. It reconnects automatically
// (handled by go-redis).
func (b *RedisBus) Subscribe(ctx context.Context, fn func(Event)) error {
	sub := b.client.Subscribe(ctx, Channel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		return err
	}
	ch := sub.Channel(redis.WithChannelSize(1024))
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var e Event
			if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
				b.log.Warn("invalid status event", "error", err)
				continue
			}
			fn(e)
		}
	}
}
