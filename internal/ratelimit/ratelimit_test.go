package ratelimit

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// measure fires n concurrent callers and returns the elapsed time.
func measure(t *testing.T, l Limiter, key string, n int) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var errs atomic.Int32
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := l.Wait(ctx, key); err != nil {
				errs.Add(1)
			}
		}()
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("%d waits failed", errs.Load())
	}
	return time.Since(start)
}

func TestLocalLimiterPacesCalls(t *testing.T) {
	l := NewLocal(100, 1)
	// 51 calls at 100/s with burst 1: the first is immediate, the remaining 50 need ~500ms.
	elapsed := measure(t, l, "sms", 51)
	if elapsed < 450*time.Millisecond {
		t.Fatalf("limiter too permissive: 51 calls took %v", elapsed)
	}
}

func TestLocalLimiterKeysAreIndependent(t *testing.T) {
	l := NewLocal(10, 1)
	ctx := context.Background()
	_, _ = l.Wait(ctx, "sms")
	start := time.Now()
	if _, err := l.Wait(ctx, "email"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Fatal("email must not be throttled by sms traffic")
	}
}

func TestLocalLimiterHonoursContext(t *testing.T) {
	l := NewLocal(1, 1)
	_, _ = l.Wait(context.Background(), "k")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l.Wait(ctx, "k"); err == nil {
		t.Fatal("expected context error")
	}
}

// TestRedisLimiter runs against a real Redis when REDIS_URL is set
// (docker compose run --rm tests sets it).
func TestRedisLimiter(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	key := "test-" + time.Now().Format("150405.000000")

	// Two independent limiter instances simulate two worker replicas sharing the budget.
	a := NewRedis(client, 100, 1)
	b := NewRedis(client, 100, 1)
	var wg sync.WaitGroup
	start := time.Now()
	for _, l := range []Limiter{a, b} {
		wg.Add(1)
		go func(l Limiter) {
			defer wg.Done()
			measure(t, l, key, 50)
		}(l)
	}
	wg.Wait()
	elapsed := time.Since(start)
	// 100 calls shared by both replicas at 100/s => ~990ms.
	if elapsed < 900*time.Millisecond {
		t.Fatalf("global limit violated: 100 calls across 2 replicas took %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("limiter too slow: %v", elapsed)
	}
}
