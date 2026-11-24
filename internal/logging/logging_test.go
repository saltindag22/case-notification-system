package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerAddsCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "test", slog.LevelInfo)
	ctx := WithCorrelationID(context.Background(), "abc-123")
	log.InfoContext(ctx, "hello", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, buf.String())
	}
	if rec["correlation_id"] != "abc-123" || rec["service"] != "test" || rec["k"] != "v" {
		t.Fatalf("unexpected record %v", rec)
	}
}

func TestNormalizeCorrelationID(t *testing.T) {
	if got := NormalizeCorrelationID("req-1:abc"); got != "req-1:abc" {
		t.Fatalf("valid id changed: %s", got)
	}
	for _, bad := range []string{"", "has space", "new\nline", strings.Repeat("a", 129)} {
		if got := NormalizeCorrelationID(bad); got == bad || len(got) != 36 {
			t.Fatalf("invalid id %q should be replaced by uuid, got %q", bad, got)
		}
	}
}
