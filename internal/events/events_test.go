package events

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

func TestRedisBusRoundTrip(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	bus := NewRedisBus(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan Event, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = bus.Subscribe(ctx, func(e Event) { got <- e })
	}()
	<-ready
	n := &domain.Notification{ID: domain.NewID(), Channel: domain.ChannelSMS, Status: domain.StatusSent, Attempts: 1}
	// Pub/Sub only delivers to active subscribers: publish until the subscriber is attached.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case e := <-got:
			if e.NotificationID != n.ID || e.Status != domain.StatusSent {
				t.Fatalf("unexpected event %+v", e)
			}
			return
		case <-tick.C:
			bus.Publish(ctx, FromNotification(n))
		case <-ctx.Done():
			t.Fatal("event not received")
		}
	}
}
