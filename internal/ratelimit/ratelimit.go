// Package ratelimit enforces the per-channel send rate (100 msg/s by default).
//
// The limit must hold globally, no matter how many worker replicas run, so the
// production implementation keeps its state in Redis. It uses GCRA (generic cell
// rate algorithm) in "reservation" mode: every caller atomically reserves the next
// free slot and is told how long to wait for it. Compared to "try, fail, sleep,
// retry" this has no thundering herd between competing workers, needs exactly one
// Redis round trip per message, and spaces messages evenly (one every 10ms at 100/s),
// which is what downstream providers prefer during bursts.
//
// With burst=1 there are never more than `rate` sends in any 1-second window.
// Redis' own clock (TIME) is used so that replicas with skewed clocks agree.
package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// Limiter blocks until the caller may perform one operation for key.
type Limiter interface {
	// Wait returns how long the caller waited, or an error if ctx ended first.
	Wait(ctx context.Context, key string) (time.Duration, error)
}

// gcraScript reserves the next slot and returns the wait time in microseconds.
//
// KEYS[1]  limiter key
// ARGV[1]  emission interval in microseconds (1e6 / rate)
// ARGV[2]  burst (>= 1)
const gcraScript = `
local interval = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
if tat < now then tat = now end
local allow_at = tat - (burst - 1) * interval
local wait = allow_at - now
if wait < 0 then wait = 0 end
local new_tat = tat + interval
local ttl_ms = math.ceil((new_tat - now) / 1000) + 1000
redis.call('SET', KEYS[1], string.format('%.0f', new_tat), 'PX', ttl_ms)
return wait
`

// Redis is a distributed GCRA limiter.
type Redis struct {
	client   redis.UniversalClient
	script   *redis.Script
	interval time.Duration
	burst    int
	prefix   string
}

// NewRedis creates a limiter allowing ratePerSecond operations per key.
func NewRedis(client redis.UniversalClient, ratePerSecond, burst int) *Redis {
	if burst < 1 {
		burst = 1
	}
	return &Redis{
		client:   client,
		script:   redis.NewScript(gcraScript),
		interval: time.Second / time.Duration(ratePerSecond),
		burst:    burst,
		prefix:   "ratelimit:",
	}
}

// Wait reserves a slot and sleeps until it starts.
func (r *Redis) Wait(ctx context.Context, key string) (time.Duration, error) {
	res, err := r.script.Run(ctx, r.client, []string{r.prefix + key}, r.interval.Microseconds(), r.burst).Int64()
	if err != nil {
		return 0, fmt.Errorf("rate limiter: %w", err)
	}
	wait := time.Duration(res) * time.Microsecond
	if wait <= 0 {
		return 0, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return wait, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Local is an in-process limiter (one token bucket per key). It is used in unit
// tests and as a fallback; it does NOT coordinate between replicas.
type Local struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	rate     rate.Limit
	burst    int
}

// NewLocal creates an in-memory limiter.
func NewLocal(ratePerSecond, burst int) *Local {
	if burst < 1 {
		burst = 1
	}
	return &Local{limiters: map[string]*rate.Limiter{}, rate: rate.Limit(ratePerSecond), burst: burst}
}

// Wait blocks until a token is available.
func (l *Local) Wait(ctx context.Context, key string) (time.Duration, error) {
	l.mu.Lock()
	lim, ok := l.limiters[key]
	if !ok {
		lim = rate.NewLimiter(l.rate, l.burst)
		l.limiters[key] = lim
	}
	l.mu.Unlock()
	start := time.Now()
	if err := lim.Wait(ctx); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}
