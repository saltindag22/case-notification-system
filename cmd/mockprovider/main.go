// Command mockprovider simulates the external notification provider locally.
//
// It implements the same contract as the webhook.site endpoint described in the
// assessment (POST {to, channel, content} -> 202 {messageId, status, timestamp}),
// plus configurable latency and failures so retries, Retry-After handling and the
// circuit breaker can be observed without depending on a third-party service:
//
//	MOCK_FAILURE_RATE       fraction of requests answered with 500/503 (default 0.05)
//	MOCK_THROTTLE_RATE      fraction answered with 429 + Retry-After (default 0.01)
//	MOCK_LATENCY_MS         base latency (default 30)
//	MOCK_LATENCY_JITTER_MS  random extra latency (default 40)
//
// It honours the Idempotency-Key header: replays return the original messageId.
package main

import (
	"container/list"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/saltindag22/case-notification-system/internal/healthcheck"
)

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
	Subject string `json:"subject,omitempty"`
}

type response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// lru remembers idempotency keys with bounded memory.
type lru struct {
	mu    sync.Mutex
	max   int
	ll    *list.List
	items map[string]*list.Element
}

type entry struct{ key, value string }

func newLRU(max int) *lru { return &lru{max: max, ll: list.New(), items: map[string]*list.Element{}} }

func (c *lru) getOrSet(key, value string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*entry).value, true
	}
	c.items[key] = c.ll.PushFront(&entry{key, value})
	if c.ll.Len() > c.max {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*entry).key)
	}
	return value, false
}

func envFloat(k string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return v
	}
	return def
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck.Run(":8090"))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mock-provider")
	failureRate := envFloat("MOCK_FAILURE_RATE", 0.05)
	throttleRate := envFloat("MOCK_THROTTLE_RATE", 0.01)
	latency := time.Duration(envFloat("MOCK_LATENCY_MS", 30)) * time.Millisecond
	jitter := time.Duration(envFloat("MOCK_LATENCY_JITTER_MS", 40)) * time.Millisecond
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	reg := prometheus.NewRegistry()
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mock_provider_requests_total", Help: "Requests received by the mock provider.",
	}, []string{"channel", "code"})
	reg.MustRegister(requests)
	seen := newLRU(100_000)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		var req request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := dec.Decode(&req); err != nil || req.To == "" || req.Channel == "" || req.Content == "" {
			requests.WithLabelValues(req.Channel, "400").Inc()
			http.Error(w, `{"error":"to, channel and content are required"}`, http.StatusBadRequest)
			return
		}
		d := latency
		if jitter > 0 {
			d += time.Duration(rand.Int64N(int64(jitter)))
		}
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}

		roll := rand.Float64()
		switch {
		case roll < throttleRate:
			requests.WithLabelValues(req.Channel, "429").Inc()
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		case roll < throttleRate+failureRate:
			code := http.StatusInternalServerError
			if rand.IntN(2) == 0 {
				code = http.StatusServiceUnavailable
			}
			requests.WithLabelValues(req.Channel, strconv.Itoa(code)).Inc()
			http.Error(w, `{"error":"simulated provider failure"}`, code)
			return
		}

		id := uuid.NewString()
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			id, _ = seen.getOrSet(key, id)
		}
		requests.WithLabelValues(req.Channel, "202").Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(response{MessageID: id, Status: "accepted", Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	})

	log.Info("mock provider listening", "addr", addr, "failure_rate", failureRate, "throttle_rate", throttleRate, "latency", latency.String())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("mock provider stopped", "error", err)
		os.Exit(1)
	}
}
