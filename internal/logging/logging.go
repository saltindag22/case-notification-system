// Package logging provides structured JSON logging (log/slog) enriched with the
// correlation id and trace/span ids carried in the context.
package logging

import (
	"context"
	"io"
	"log/slog"
	"regexp"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

// HeaderCorrelationID is the HTTP / AMQP header carrying the correlation id.
const HeaderCorrelationID = "X-Correlation-ID"

var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

// WithCorrelationID stores id in ctx.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the id stored in ctx, or "".
func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKey{}).(string); ok {
		return v
	}
	return ""
}

// NormalizeCorrelationID returns id when it is safe to log/propagate, otherwise a new one.
func NormalizeCorrelationID(id string) string {
	if validCorrelationID.MatchString(id) {
		return id
	}
	return uuid.NewString()
}

// New returns a JSON logger that adds service, correlation_id, trace_id and span_id.
func New(w io.Writer, service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(&contextHandler{Handler: h}).With(slog.String("service", service))
}

type contextHandler struct{ slog.Handler }

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}
