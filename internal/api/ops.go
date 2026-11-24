package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/saltindag22/case-notification-system/internal/domain"
	"github.com/saltindag22/case-notification-system/internal/repository"
)

// MetricsResponse is the human friendly, real-time metrics snapshot.
type MetricsResponse struct {
	Timestamp  time.Time                                   `json:"timestamp"`
	Window     string                                      `json:"window"`
	QueueDepth map[domain.Channel]map[string]int64         `json:"queueDepth"`
	Channels   map[domain.Channel]*repository.ChannelStats `json:"channels"`
	Totals     Totals                                      `json:"totals"`
}

// Totals aggregates all channels.
type Totals struct {
	Backlog     int64   `json:"backlog"`
	Sent        int64   `json:"sent"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"successRate"`
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
	window := 5 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Minute || d > 24*time.Hour {
			s.writeError(w, r, badRequest("window must be a duration between 1m and 24h, e.g. 5m or 1h"))
			return
		}
		window = d
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	backlog, err := s.stats.BacklogCounts(ctx)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	stats, err := s.stats.DeliveryStats(ctx, window)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	resp := MetricsResponse{
		Timestamp:  time.Now().UTC(),
		Window:     window.String(),
		QueueDepth: map[domain.Channel]map[string]int64{},
		Channels:   stats,
	}
	for ch, byStatus := range backlog {
		m := map[string]int64{}
		var total int64
		for st, n := range byStatus {
			m[string(st)] = n
			total += n
		}
		m["total"] = total
		resp.QueueDepth[ch] = m
		resp.Totals.Backlog += total
	}
	for _, cs := range stats {
		resp.Totals.Sent += cs.Sent
		resp.Totals.Failed += cs.Failed
	}
	if t := resp.Totals.Sent + resp.Totals.Failed; t > 0 {
		resp.Totals.SuccessRate = float64(int64(float64(resp.Totals.Sent)/float64(t)*10000+0.5)) / 10000
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": s.version,
		"uptime":  time.Since(s.started).Round(time.Second).String(),
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	status, body := RunChecks(r.Context(), s.checks)
	writeJSON(w, status, body)
}

// RunChecks executes all checks concurrently with a timeout and builds the readiness body.
func RunChecks(ctx context.Context, checks []Check) (int, map[string]any) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	results := make(map[string]string, len(checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	healthy := true
	for _, c := range checks {
		wg.Add(1)
		go func(c Check) {
			defer wg.Done()
			res := "ok"
			if err := c.Fn(ctx); err != nil {
				res = "error: " + err.Error()
			}
			mu.Lock()
			results[c.Name] = res
			if res != "ok" {
				healthy = false
			}
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	if !healthy {
		return http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "checks": results}
	}
	return http.StatusOK, map[string]any{"status": "ok", "checks": results}
}

func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	_, _ = w.Write(s.spec)
}

const swaggerHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Notification System API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.ui = SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui', deepLinking: true });
  </script>
</body>
</html>`

func (s *Server) swaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerHTML))
}
