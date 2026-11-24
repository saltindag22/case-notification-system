package retry

import (
	"testing"
	"time"
)

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := Policy{MaxAttempts: 10, BaseDelay: time.Second, MaxDelay: 10 * time.Second, Multiplier: 2, Jitter: 0}
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := p.Backoff(i+1, nil); got != w {
			t.Errorf("attempt %d: got %v want %v", i+1, got, w)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	p := Policy{MaxAttempts: 5, BaseDelay: 4 * time.Second, MaxDelay: time.Minute, Multiplier: 2, Jitter: 0.5}
	if got := p.Backoff(1, func() float64 { return 0 }); got != 2*time.Second {
		t.Fatalf("min jitter: got %v want 2s", got)
	}
	if got := p.Backoff(1, func() float64 { return 0.999999 }); got < 3990*time.Millisecond || got > 4*time.Second {
		t.Fatalf("max jitter: got %v want ~4s", got)
	}
}

func TestBackoffHugeAttemptDoesNotOverflow(t *testing.T) {
	p := DefaultPolicy()
	if got := p.Backoff(10_000, func() float64 { return 1 }); got > p.MaxDelay || got <= 0 {
		t.Fatalf("got %v, want (0, %v]", got, p.MaxDelay)
	}
}

func TestDecide(t *testing.T) {
	p := Policy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second, Multiplier: 2, Jitter: 0}

	if d := p.Decide(1, false, 0); d.Retry {
		t.Fatal("permanent errors must not retry")
	}
	if d := p.Decide(3, true, 0); d.Retry {
		t.Fatal("exhausted attempts must not retry")
	}
	if d := p.Decide(2, true, 0); !d.Retry || d.Delay != 2*time.Second {
		t.Fatalf("unexpected decision %+v", d)
	}
	if d := p.Decide(1, true, 7*time.Second); d.Delay != 7*time.Second {
		t.Fatalf("retry-after should win, got %v", d.Delay)
	}
	if d := p.Decide(1, true, time.Hour); d.Delay != 30*time.Second {
		t.Fatalf("retry-after must be capped, got %v", d.Delay)
	}
}

func TestIsRetryableStatus(t *testing.T) {
	retryable := []int{408, 425, 429, 500, 502, 503, 504}
	permanent := []int{400, 401, 403, 404, 409, 413, 422}
	for _, c := range retryable {
		if !IsRetryableStatus(c) {
			t.Errorf("%d should be retryable", c)
		}
	}
	for _, c := range permanent {
		if IsRetryableStatus(c) {
			t.Errorf("%d should be permanent", c)
		}
	}
}

func TestNormalize(t *testing.T) {
	// Jitter 0 is a legitimate explicit choice (deterministic delays) and is kept.
	p := Policy{Jitter: 0.5}.Normalize()
	if p != DefaultPolicy() {
		t.Fatalf("zero policy should normalize to default, got %+v", p)
	}
	if got := (Policy{Jitter: 0}).Normalize().Jitter; got != 0 {
		t.Fatalf("explicit zero jitter must be kept, got %v", got)
	}
	if got := (Policy{BaseDelay: time.Minute, MaxDelay: time.Second}).Normalize().MaxDelay; got != time.Minute {
		t.Fatalf("max delay must not be below base delay, got %v", got)
	}
}
