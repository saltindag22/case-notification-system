package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeBacklog struct {
	counts map[domain.Channel]map[domain.Status]int64
	err    error
}

func (f fakeBacklog) BacklogCounts(context.Context) (map[domain.Channel]map[domain.Status]int64, error) {
	return f.counts, f.err
}

func TestNewRegistersInstruments(t *testing.T) {
	m := New("test")
	m.Deliveries.WithLabelValues("sms", "sent").Add(3)
	m.BreakerState.WithLabelValues("push").Set(2)
	m.HTTPRequests.WithLabelValues("GET", "/x", "200").Inc()

	if got := testutil.ToFloat64(m.Deliveries.WithLabelValues("sms", "sent")); got != 3 {
		t.Fatalf("deliveries counter = %v", got)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{"test_notification_deliveries_total", "test_circuit_breaker_state", "test_http_requests_total", "go_goroutines"} {
		if !names[want] {
			t.Errorf("metric %s not registered (have %v)", want, names)
		}
	}
}

func TestBacklogCollectorExposesDatabaseCounts(t *testing.T) {
	m := New("test")
	m.RegisterBacklog("notification", fakeBacklog{counts: map[domain.Channel]map[domain.Status]int64{
		domain.ChannelSMS:   {domain.StatusPending: 7, domain.StatusQueued: 3},
		domain.ChannelEmail: {domain.StatusRetrying: 1},
	}}, discard)

	expected := `
# HELP notification_notifications_backlog Non-terminal notifications by channel and status (queue depth).
# TYPE notification_notifications_backlog gauge
notification_notifications_backlog{channel="email",status="retrying"} 1
notification_notifications_backlog{channel="sms",status="pending"} 7
notification_notifications_backlog{channel="sms",status="queued"} 3
`
	if err := testutil.GatherAndCompare(m.Registry, strings.NewReader(expected), "notification_notifications_backlog"); err != nil {
		t.Fatal(err)
	}
}

func TestBacklogCollectorSurvivesDatabaseErrors(t *testing.T) {
	m := New("test")
	m.RegisterBacklog("notification", fakeBacklog{err: errors.New("db down")}, discard)
	if n, err := testutil.GatherAndCount(m.Registry, "notification_notifications_backlog"); err != nil || n != 0 {
		t.Fatalf("a failing source must yield no samples and no scrape error, got n=%d err=%v", n, err)
	}
}
