package api

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/saltindag22/case-notification-system/internal/logging"
)

// statusRecorder captures status and size for logs and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Hijack keeps WebSocket upgrades working through the recorder.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacking not supported")
	}
	return h.Hijack()
}

// Unwrap supports http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// routerErrorsAsJSON turns the router's own plain-text 404 (unknown route) and
// 405 (known route, wrong method; the Allow header is kept) into the JSON error
// envelope used everywhere else. Handlers' own responses are never JSON-rewritten
// because they are not text/plain.
func routerErrorsAsJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&routerErrorWriter{ResponseWriter: w, r: r}, r)
	})
}

type routerErrorWriter struct {
	http.ResponseWriter
	r           *http.Request
	intercepted bool
}

func (w *routerErrorWriter) WriteHeader(code int) {
	if (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) &&
		strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		w.intercepted = true
		w.Header().Del("X-Content-Type-Options")
		if code == http.StatusNotFound {
			writeErrorCode(w.ResponseWriter, w.r, code, "not_found", "route not found")
		} else {
			writeErrorCode(w.ResponseWriter, w.r, code, "method_not_allowed", "method not allowed (see the Allow header)")
		}
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *routerErrorWriter) Write(b []byte) (int, error) {
	if w.intercepted {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

// Hijack keeps WebSocket upgrades working through the wrapper.
func (w *routerErrorWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacking not supported")
	}
	return h.Hijack()
}

// Unwrap supports http.ResponseController.
func (w *routerErrorWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// correlationMiddleware accepts X-Correlation-ID (or X-Request-ID), generates one
// when absent/invalid, echoes it in the response and stores it in the context.
func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(logging.HeaderCorrelationID)
		if id == "" {
			id = r.Header.Get("X-Request-ID")
		}
		id = logging.NormalizeCorrelationID(id)
		w.Header().Set(logging.HeaderCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				s.log.ErrorContext(r.Context(), "panic recovered", "panic", rec, "stack", string(debug.Stack()))
				writeErrorCode(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// route wraps a handler with tracing (span named after the route), metrics and an access log.
func (s *Server) route(pattern string, h http.HandlerFunc) {
	instrumented := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		h(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		elapsed := time.Since(start)
		route := r.Pattern
		if s.metrics != nil {
			s.metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
			s.metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())
		}
		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelError
		} else if r.URL.Path == "/metrics" || r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" {
			level = slog.LevelDebug
		}
		s.log.Log(r.Context(), level, "http request",
			slog.String("method", r.Method), slog.String("route", route), slog.String("path", r.URL.Path),
			slog.Int("status", rec.status), slog.Int("bytes", rec.bytes), slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
			slog.String("remote_addr", r.RemoteAddr))
	})
	s.mux.Handle(pattern, otelhttp.NewHandler(instrumented, pattern))
}
