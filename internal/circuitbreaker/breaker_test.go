package circuitbreaker

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestBreaker(onChange func(State)) (*Breaker, *clock) {
	c := &clock{t: time.Unix(0, 0)}
	b := New(Config{FailureThreshold: 3, OpenTimeout: 10 * time.Second, HalfOpenMaxCalls: 1}, onChange)
	b.now = c.now
	return b, c
}

func TestBreakerLifecycle(t *testing.T) {
	var transitions []State
	b, c := newTestBreaker(func(s State) { transitions = append(transitions, s) })

	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if b.State() != StateClosed {
		t.Fatal("should stay closed below threshold")
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatal("should open at threshold")
	}
	if ok, wait := b.Allow(); ok || wait != 10*time.Second {
		t.Fatalf("open breaker must reject, got ok=%v wait=%v", ok, wait)
	}

	c.advance(10 * time.Second)
	if ok, _ := b.Allow(); !ok {
		t.Fatal("first probe after timeout must be allowed")
	}
	if b.State() != StateHalfOpen {
		t.Fatal("should be half-open while probing")
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("only one probe allowed in half-open")
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatal("failed probe must re-open")
	}

	c.advance(10 * time.Second)
	if ok, _ := b.Allow(); !ok {
		t.Fatal("probe should be allowed again")
	}
	b.Success()
	if b.State() != StateClosed {
		t.Fatal("successful probe must close")
	}

	want := []State{StateOpen, StateHalfOpen, StateOpen, StateHalfOpen, StateClosed}
	if len(transitions) != len(want) {
		t.Fatalf("transitions %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions %v, want %v", transitions, want)
		}
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b, _ := newTestBreaker(nil)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if b.State() != StateClosed {
		t.Fatal("success should reset consecutive failures")
	}
}

func TestReleaseReturnsProbeSlot(t *testing.T) {
	b, c := newTestBreaker(nil)
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	c.advance(10 * time.Second)
	if ok, _ := b.Allow(); !ok {
		t.Fatal("probe expected")
	}
	b.Release()
	if ok, _ := b.Allow(); !ok {
		t.Fatal("released slot should be reusable")
	}
}
