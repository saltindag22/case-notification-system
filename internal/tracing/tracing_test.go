package tracing

import (
	"context"
	"testing"
)

func TestTraceParentRoundTrip(t *testing.T) {
	shutdown, err := Init(context.Background(), "test", "dev", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	ctx, span := Tracer("test").Start(context.Background(), "root")
	defer span.End()
	tp := TraceParent(ctx)
	if tp == "" {
		t.Fatal("expected traceparent for a recording span")
	}

	remote := ContextWithTraceParent(context.Background(), tp)
	_, child := Tracer("test").Start(remote, "child")
	defer child.End()
	if child.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Fatal("child must continue the original trace")
	}
	if TraceParent(context.Background()) != "" {
		t.Fatal("no span => empty traceparent")
	}
}

func TestHeaderCarrier(t *testing.T) {
	c := HeaderCarrier{}
	c.Set("traceparent", "x")
	if c.Get("traceparent") != "x" || c.Get("missing") != "" || len(c.Keys()) != 1 {
		t.Fatal("carrier mismatch")
	}
}

func TestTracesURL(t *testing.T) {
	cases := map[string]string{
		"http://jaeger:4318":             "http://jaeger:4318/v1/traces",
		"http://jaeger:4318/":            "http://jaeger:4318/v1/traces",
		"https://otel.example/v1/traces": "https://otel.example/v1/traces",
		"http://collector:4318/otlp":     "http://collector:4318/otlp/v1/traces",
	}
	for in, want := range cases {
		if got := TracesURL(in); got != want {
			t.Errorf("TracesURL(%q) = %q, want %q", in, got, want)
		}
	}
}
