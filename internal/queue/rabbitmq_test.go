package queue

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// These tests need a real RabbitMQ (with the management plugin). They run when
// RABBITMQ_URL is set, e.g. `docker compose run --rm tests`.
//
// Every test gets its own temporary virtual host, created and deleted through the
// management API, so the tests never touch the queues of a running worker.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	raw := os.Getenv("RABBITMQ_URL")
	if raw == "" {
		t.Skip("RABBITMQ_URL not set; skipping integration test")
	}
	amqpURL, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse RABBITMQ_URL: %v", err)
	}
	mgmt := os.Getenv("RABBITMQ_MANAGEMENT_URL")
	if mgmt == "" {
		mgmt = "http://" + amqpURL.Hostname() + ":15672"
	}
	user := amqpURL.User.Username()
	pass, _ := amqpURL.User.Password()

	vhost := fmt.Sprintf("test-%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
	manage := func(method, path, body string) {
		t.Helper()
		req, err := http.NewRequest(method, strings.TrimRight(mgmt, "/")+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth(user, pass)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("rabbitmq management API: %v", err)
		}
		_ = resp.Body.Close()
		alreadyGone := method == http.MethodDelete && resp.StatusCode == http.StatusNotFound
		if resp.StatusCode >= 300 && !alreadyGone {
			t.Fatalf("rabbitmq management API %s %s: %s", method, path, resp.Status)
		}
	}
	manage(http.MethodPut, "/api/vhosts/"+url.PathEscape(vhost), "")
	manage(http.MethodPut, "/api/permissions/"+url.PathEscape(vhost)+"/"+url.PathEscape(user), `{"configure":".*","write":".*","read":".*"}`)
	t.Cleanup(func() { manage(http.MethodDelete, "/api/vhosts/"+url.PathEscape(vhost), "") })

	u := *amqpURL
	u.Path = "/" + vhost
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, u.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func depth(t *testing.T, c *Client, name string) int {
	t.Helper()
	var (
		n   int
		err error
	)
	if name == DeadQueue {
		n, err = c.QueueDepthAny(name)
	} else {
		n, err = c.QueueDepth(name)
	}
	if err != nil {
		t.Fatalf("queue depth %s: %v", name, err)
	}
	return n
}

func outgoing(ch domain.Channel, p domain.Priority, corr string) Outgoing {
	return Outgoing{
		Message:       Message{NotificationID: domain.NewID(), Channel: ch, Attempt: 1},
		Priority:      p,
		CorrelationID: corr,
		Headers:       map[string]any{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
}

func receive(t *testing.T, deliveries <-chan Delivery) Delivery {
	t.Helper()
	select {
	case d, ok := <-deliveries:
		if !ok {
			t.Fatal("delivery channel closed")
		}
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery within 5s")
		return Delivery{}
	}
}

func TestDialDeclaresTopology(t *testing.T) {
	c := newTestClient(t)
	for _, ch := range domain.Channels {
		if n := depth(t, c, QueueName(ch)); n != 0 {
			t.Fatalf("new queue %s should be empty, has %d", QueueName(ch), n)
		}
	}
	if n := depth(t, c, DeadQueue); n != 0 {
		t.Fatalf("dead letter queue should be empty, has %d", n)
	}
	if c.IsClosed() {
		t.Fatal("fresh client must be connected")
	}
}

func TestPublishRoutesByChannel(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	err := c.PublishBatch(ctx, []Outgoing{
		outgoing(domain.ChannelEmail, domain.PriorityNormal, "c-1"),
		outgoing(domain.ChannelEmail, domain.PriorityNormal, "c-2"),
		outgoing(domain.ChannelPush, domain.PriorityNormal, "c-3"),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Publisher confirms mean the broker has the messages once PublishBatch returns.
	if e, p, s := depth(t, c, QueueName(domain.ChannelEmail)), depth(t, c, QueueName(domain.ChannelPush)), depth(t, c, QueueName(domain.ChannelSMS)); e != 2 || p != 1 || s != 0 {
		t.Fatalf("routing by channel failed: email=%d push=%d sms=%d", e, p, s)
	}
}

func TestConsumeDeliversByPriorityWithMetadata(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	low := outgoing(domain.ChannelSMS, domain.PriorityLow, "corr-low")
	normal := outgoing(domain.ChannelSMS, domain.PriorityNormal, "corr-normal")
	high := outgoing(domain.ChannelSMS, domain.PriorityHigh, "corr-high")
	if err := c.PublishBatch(ctx, []Outgoing{low, normal, high}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deliveries, stop, err := c.Consume(ctx, domain.ChannelSMS, 1, "test-consumer")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer func() { _ = stop() }()

	for _, want := range []Outgoing{high, normal, low} {
		d := receive(t, deliveries)
		if d.Message.NotificationID != want.Message.NotificationID {
			t.Fatalf("expected priority %s first, got message for another notification", want.Priority)
		}
		if d.Message.Channel != domain.ChannelSMS || d.Message.Attempt != 1 {
			t.Fatalf("message body not round-tripped: %+v", d.Message)
		}
		if d.CorrelationID() != want.CorrelationID {
			t.Fatalf("correlation id %q, want %q", d.CorrelationID(), want.CorrelationID)
		}
		if d.Headers["traceparent"] != want.Headers["traceparent"] {
			t.Fatalf("trace header not propagated: %v", d.Headers)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	eventually(t, "queue to drain", func() bool { return depth(t, c, QueueName(domain.ChannelSMS)) == 0 })
}

func TestRequeueRedeliversTheMessage(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := outgoing(domain.ChannelEmail, domain.PriorityNormal, "corr")
	if err := c.PublishBatch(ctx, []Outgoing{msg}); err != nil {
		t.Fatal(err)
	}
	deliveries, stop, err := c.Consume(ctx, domain.ChannelEmail, 1, "requeue-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	first := receive(t, deliveries)
	if err := first.Requeue(); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	second := receive(t, deliveries)
	if second.Message.NotificationID != msg.Message.NotificationID {
		t.Fatal("requeued message must be delivered again")
	}
	_ = second.Ack()
}

func TestRejectSendsMessageToDeadLetterQueue(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.PublishBatch(ctx, []Outgoing{outgoing(domain.ChannelPush, domain.PriorityHigh, "corr")}); err != nil {
		t.Fatal(err)
	}
	deliveries, stop, err := c.Consume(ctx, domain.ChannelPush, 1, "reject-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	if err := receive(t, deliveries).Reject(); err != nil {
		t.Fatalf("reject: %v", err)
	}
	eventually(t, "message in dead letter queue", func() bool { return depth(t, c, DeadQueue) == 1 })
}

func TestPoisonMessageIsDeadLetteredWithoutReachingConsumers(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	raw, err := c.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	for _, body := range []string{"not json", `{"notificationId":"00000000-0000-0000-0000-000000000000"}`} {
		if err := raw.PublishWithContext(ctx, Exchange, string(domain.ChannelSMS), false, false,
			amqp.Publishing{ContentType: "application/json", Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	valid := outgoing(domain.ChannelSMS, domain.PriorityLow, "corr")
	if err := c.PublishBatch(ctx, []Outgoing{valid}); err != nil {
		t.Fatal(err)
	}

	deliveries, stop, err := c.Consume(ctx, domain.ChannelSMS, 5, "poison-consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	d := receive(t, deliveries)
	if d.Message.NotificationID != valid.Message.NotificationID {
		t.Fatal("undecodable messages must never reach the consumer")
	}
	_ = d.Ack()
	eventually(t, "both poison messages in the dead letter queue", func() bool { return depth(t, c, DeadQueue) == 2 })
}

func TestStopEndsConsumptionAndCloseDisconnects(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	deliveries, stop, err := c.Consume(ctx, domain.ChannelEmail, 1, "stop-consumer")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-deliveries:
		if ok {
			t.Fatal("no message was published, nothing should be delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the context must close the delivery channel")
	}
	_ = stop()

	closed := c.NotifyClose()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !c.IsClosed() {
		t.Fatal("client must report closed after Close")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("NotifyClose must fire on close")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("closing twice must be harmless, got %v", err)
	}
}
