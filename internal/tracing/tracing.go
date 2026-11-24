// Package tracing configures OpenTelemetry distributed tracing.
//
// A single trace follows a notification end to end:
//
//	HTTP POST /notifications (api) ─▶ stored traceparent ─▶ dispatcher publish (worker)
//	  ─▶ AMQP headers ─▶ consumer "process notification" ─▶ provider HTTP call
//
// The W3C traceparent is persisted with the notification because the hop from the
// API to the broker is asynchronous (the dispatcher publishes later, maybe after a
// scheduled delay or a retry), so the request context is long gone by then.
package tracing

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns the named tracer from the global provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// Init installs the global tracer provider and W3C propagators. When endpoint is
// empty, spans are still created (so logs carry trace ids) but not exported.
func Init(ctx context.Context, service, version, endpoint string, sampleRatio float64) (func(context.Context) error, error) {
	// Schemaless so the SDK's default resource (whatever semconv version it uses) merges cleanly.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(service),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing resource: %w", err)
	}
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	}
	if endpoint != "" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(TracesURL(endpoint)))
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}

// TraceParent serialises the span context in ctx as a W3C traceparent ("" if none).
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextWithTraceParent returns ctx carrying the remote span context described by tp.
func ContextWithTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}

// HeaderCarrier adapts AMQP-style string headers to a TextMapCarrier.
type HeaderCarrier map[string]any

// Get returns the value for key.
func (c HeaderCarrier) Get(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}

// Set stores key=value.
func (c HeaderCarrier) Set(key, value string) { c[key] = value }

// Keys lists all keys.
func (c HeaderCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

// TracesURL turns an OTLP base endpoint (OTEL_EXPORTER_OTLP_ENDPOINT, e.g.
// http://jaeger:4318) into the traces URL. Per the OTLP spec the base endpoint
// gets "/v1/traces" appended; WithEndpointURL would otherwise POST to "/".
func TracesURL(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	if !strings.HasSuffix(u.Path, "/v1/traces") {
		u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/traces"
	}
	return u.String()
}
