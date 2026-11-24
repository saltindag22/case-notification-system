package repository

import (
	"context"
	"time"

	"github.com/saltindag22/case-notification-system/internal/domain"
)

// BacklogCounts returns the number of non-terminal notifications per channel and status.
// Terminal states are excluded on purpose: they grow without bound and are covered
// by the windowed delivery statistics instead.
func (s *Store) BacklogCounts(ctx context.Context) (map[domain.Channel]map[domain.Status]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, status, count(*) FROM notifications
		WHERE status IN ('pending', 'scheduled', 'queued', 'processing', 'retrying')
		GROUP BY channel, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Channel]map[domain.Status]int64{}
	for _, ch := range domain.Channels {
		out[ch] = map[domain.Status]int64{}
		for _, st := range []domain.Status{domain.StatusPending, domain.StatusScheduled, domain.StatusQueued, domain.StatusProcessing, domain.StatusRetrying} {
			out[ch][st] = 0
		}
	}
	for rows.Next() {
		var ch, st string
		var c int64
		if err := rows.Scan(&ch, &st, &c); err != nil {
			return nil, err
		}
		if out[domain.Channel(ch)] == nil {
			out[domain.Channel(ch)] = map[domain.Status]int64{}
		}
		out[domain.Channel(ch)][domain.Status(st)] = c
	}
	return out, rows.Err()
}

// LatencyStats summarises a latency distribution in milliseconds.
type LatencyStats struct {
	Avg float64 `json:"avg"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// ChannelStats are windowed delivery statistics for one channel.
type ChannelStats struct {
	Attempts           int64        `json:"attempts"`
	AttemptSuccesses   int64        `json:"attemptSuccesses"`
	AttemptFailures    int64        `json:"attemptFailures"`
	Sent               int64        `json:"sent"`
	Failed             int64        `json:"failed"`
	SuccessRate        float64      `json:"successRate"`        // sent / (sent + failed)
	AttemptSuccessRate float64      `json:"attemptSuccessRate"` // successful attempts / attempts
	ProviderLatency    LatencyStats `json:"providerLatencyMs"`  // one HTTP call to the provider
	EndToEndLatency    LatencyStats `json:"endToEndLatencyMs"`  // created (or scheduled) -> sent
}

// DeliveryStats computes per-channel statistics over the trailing window.
func (s *Store) DeliveryStats(ctx context.Context, window time.Duration) (map[domain.Channel]*ChannelStats, error) {
	out := map[domain.Channel]*ChannelStats{}
	for _, ch := range domain.Channels {
		out[ch] = &ChannelStats{}
	}
	since := time.Now().Add(-window)

	rows, err := s.pool.Query(ctx, `
		SELECT n.channel,
		       count(*),
		       count(*) FILTER (WHERE a.outcome = 'success'),
		       COALESCE(avg(a.duration_ms), 0),
		       COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY a.duration_ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms), 0),
		       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms), 0)
		FROM notification_attempts a JOIN notifications n ON n.id = a.notification_id
		WHERE a.created_at >= $1
		GROUP BY n.channel`, since)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ch string
		var st ChannelStats
		if err := rows.Scan(&ch, &st.Attempts, &st.AttemptSuccesses, &st.ProviderLatency.Avg,
			&st.ProviderLatency.P50, &st.ProviderLatency.P95, &st.ProviderLatency.P99); err != nil {
			rows.Close()
			return nil, err
		}
		cs := ensure(out, ch)
		cs.Attempts, cs.AttemptSuccesses, cs.ProviderLatency = st.Attempts, st.AttemptSuccesses, st.ProviderLatency
		cs.AttemptFailures = st.Attempts - st.AttemptSuccesses
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `
		SELECT channel,
		       count(*),
		       COALESCE(avg(ms), 0),
		       COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 0),
		       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms), 0)
		FROM (
			SELECT channel, EXTRACT(EPOCH FROM (sent_at - COALESCE(scheduled_at, created_at))) * 1000 AS ms
			FROM notifications WHERE sent_at >= $1
		) t GROUP BY channel`, since)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ch string
		var sent int64
		var l LatencyStats
		if err := rows.Scan(&ch, &sent, &l.Avg, &l.P50, &l.P95, &l.P99); err != nil {
			rows.Close()
			return nil, err
		}
		cs := ensure(out, ch)
		cs.Sent, cs.EndToEndLatency = sent, l
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `SELECT channel, count(*) FROM notifications WHERE failed_at >= $1 GROUP BY channel`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ch string
		var failed int64
		if err := rows.Scan(&ch, &failed); err != nil {
			return nil, err
		}
		ensure(out, ch).Failed = failed
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, cs := range out {
		if total := cs.Sent + cs.Failed; total > 0 {
			cs.SuccessRate = round(float64(cs.Sent) / float64(total))
		}
		if cs.Attempts > 0 {
			cs.AttemptSuccessRate = round(float64(cs.AttemptSuccesses) / float64(cs.Attempts))
		}
		cs.ProviderLatency = roundLatency(cs.ProviderLatency)
		cs.EndToEndLatency = roundLatency(cs.EndToEndLatency)
	}
	return out, nil
}

func ensure(m map[domain.Channel]*ChannelStats, ch string) *ChannelStats {
	c := domain.Channel(ch)
	if m[c] == nil {
		m[c] = &ChannelStats{}
	}
	return m[c]
}

func round(f float64) float64 { return float64(int64(f*10000+0.5)) / 10000 }

func roundLatency(l LatencyStats) LatencyStats {
	r := func(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
	return LatencyStats{Avg: r(l.Avg), P50: r(l.P50), P95: r(l.P95), P99: r(l.P99)}
}
