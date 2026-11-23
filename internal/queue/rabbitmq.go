// Package queue is the RabbitMQ adapter.
//
// Topology:
//
//	exchange "notifications" (direct, durable)
//	  ├─ routing key "sms"   ─▶ queue "notifications.sms"   (x-max-priority=3)
//	  ├─ routing key "email" ─▶ queue "notifications.email" (x-max-priority=3)
//	  └─ routing key "push"  ─▶ queue "notifications.push"  (x-max-priority=3)
//	exchange "notifications.dlx" (fanout) ─▶ queue "notifications.dead"
//
// One queue per channel isolates channels from each other (a slow SMS provider never
// delays e-mail). Messages are persistent and published with publisher confirms.
// Only poison messages (undecodable) are dead-lettered by the broker: business
// retries are scheduled in PostgreSQL, which keeps the broker queues short.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// Names of broker objects.
const (
	Exchange      = "notifications"
	DLXExchange   = "notifications.dlx"
	DeadQueue     = "notifications.dead"
	MaxPriority   = 3
	headerCorrID  = "correlation_id"
	headerAttempt = "attempt"
)

// QueueName returns the queue for a channel.
func QueueName(ch domain.Channel) string { return "notifications." + string(ch) }

// Message is the payload placed on the broker. It deliberately carries only ids:
// the database row is the source of truth and is re-read (and claimed) on consume.
type Message struct {
	NotificationID uuid.UUID      `json:"notificationId"`
	Channel        domain.Channel `json:"channel"`
	Attempt        int            `json:"attempt"` // attempts already made when dispatched
}

// Delivery is a consumed message.
type Delivery struct {
	Message Message
	Headers map[string]any
	raw     amqp.Delivery
}

// Ack acknowledges the delivery.
func (d Delivery) Ack() error { return d.raw.Ack(false) }

// Reject dead-letters the delivery (no requeue).
func (d Delivery) Reject() error { return d.raw.Nack(false, false) }

// Requeue returns the delivery to its queue.
func (d Delivery) Requeue() error { return d.raw.Nack(false, true) }

// Client wraps one AMQP connection. A dedicated channel is used for publishing
// (with confirms) and one per consumer.
type Client struct {
	conn *amqp.Connection

	pubMu sync.Mutex
	pub   *amqp.Channel
	conf  chan amqp.Confirmation
}

// Dial connects (retrying while the broker starts) and declares the topology.
func Dial(ctx context.Context, url string) (*Client, error) {
	var conn *amqp.Connection
	var err error
	for i := 0; i < 30; i++ {
		conn, err = amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Properties: amqp.Table{"connection_name": "notification-worker"}})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	c := &Client{conn: conn}
	if err := c.declare(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	pub, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := pub.Confirm(false); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	c.pub = pub
	c.conf = pub.NotifyPublish(make(chan amqp.Confirmation, 1024))
	return c, nil
}

func (c *Client) declare() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(DLXExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(DeadQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead queue: %w", err)
	}
	if err := ch.QueueBind(DeadQueue, "", DLXExchange, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(Exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	for _, chn := range domain.Channels {
		args := amqp.Table{"x-max-priority": int32(MaxPriority), "x-dead-letter-exchange": DLXExchange}
		if _, err := ch.QueueDeclare(QueueName(chn), true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", chn, err)
		}
		if err := ch.QueueBind(QueueName(chn), string(chn), Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// Outgoing is a message to publish together with its metadata.
type Outgoing struct {
	Message       Message
	Priority      domain.Priority
	CorrelationID string
	Headers       map[string]any // e.g. trace context
}

// PublishBatch publishes all messages and waits until the broker confirmed every
// one of them. Any nack or timeout fails the whole batch, so the caller can roll back.
func (c *Client) PublishBatch(ctx context.Context, msgs []Outgoing) error {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	for _, m := range msgs {
		body, err := json.Marshal(m.Message)
		if err != nil {
			return err
		}
		headers := amqp.Table{headerCorrID: m.CorrelationID, headerAttempt: int32(m.Message.Attempt)}
		for k, v := range m.Headers {
			headers[k] = v
		}
		err = c.pub.PublishWithContext(ctx, Exchange, string(m.Message.Channel), false, false, amqp.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp.Persistent,
			Priority:      uint8(m.Priority),
			MessageId:     m.Message.NotificationID.String(),
			CorrelationId: m.CorrelationID,
			Timestamp:     time.Now(),
			Headers:       headers,
			Body:          body,
		})
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	}
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for range msgs {
		select {
		case conf, ok := <-c.conf:
			if !ok {
				return errors.New("publisher channel closed")
			}
			if !conf.Ack {
				return fmt.Errorf("broker nacked message (tag %d)", conf.DeliveryTag)
			}
		case <-timeout.C:
			return errors.New("timed out waiting for publisher confirms")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// QueueDepth returns the number of ready messages in a queue.
func (c *Client) QueueDepth(name string) (int, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, amqp.Table{"x-max-priority": int32(MaxPriority), "x-dead-letter-exchange": DLXExchange})
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// QueueDepthAny is like QueueDepth for queues declared without arguments (dead queue).
func (c *Client) QueueDepthAny(name string) (int, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// Consume starts a consumer on the channel's queue with the given prefetch.
// Decoding failures are dead-lettered immediately. The returned stop function
// must be called after every delivery has been acked/nacked: it closes the AMQP
// channel, so calling it earlier would make in-flight acks fail.
func (c *Client) Consume(ctx context.Context, ch domain.Channel, prefetch int, consumerTag string) (<-chan Delivery, func() error, error) {
	amqpCh, err := c.conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	// A small prefetch is essential for priorities to work: messages already pushed
	// to a consumer can no longer be overtaken by higher priority ones.
	if err := amqpCh.Qos(prefetch, 0, false); err != nil {
		_ = amqpCh.Close()
		return nil, nil, err
	}
	// The consumer is cancelled when ctx ends; already delivered messages stay ours to ack.
	raw, err := amqpCh.ConsumeWithContext(ctx, QueueName(ch), consumerTag, false, false, false, false, nil)
	if err != nil {
		_ = amqpCh.Close()
		return nil, nil, err
	}
	out := make(chan Delivery)
	go func() {
		defer close(out)
		for d := range raw {
			var m Message
			if err := json.Unmarshal(d.Body, &m); err != nil || m.NotificationID == uuid.Nil {
				_ = d.Nack(false, false)
				continue
			}
			out <- Delivery{Message: m, Headers: d.Headers, raw: d}
		}
	}()
	return out, amqpCh.Close, nil
}

// NotifyClose returns a channel that receives an error when the connection drops.
func (c *Client) NotifyClose() <-chan *amqp.Error {
	return c.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// IsClosed reports whether the connection is gone.
func (c *Client) IsClosed() bool { return c.conn.IsClosed() }

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn.IsClosed() {
		return nil
	}
	return c.conn.Close()
}

// CorrelationID extracts the correlation id header from a delivery.
func (d Delivery) CorrelationID() string {
	if v, ok := d.Headers[headerCorrID].(string); ok {
		return v
	}
	return d.raw.CorrelationId
}
