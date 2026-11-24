// Package circuitbreaker protects a downstream provider (and our retry budget)
// when it is clearly unhealthy.
//
// Closed:    requests flow; consecutive failures are counted.
// Open:      after FailureThreshold consecutive failures nothing is sent for OpenTimeout.
//
//	Work is deferred without consuming an attempt.
//
// Half-open: after OpenTimeout up to HalfOpenMaxCalls probe requests are allowed.
//
//	A success closes the breaker, a failure re-opens it.
//
// Only retryable/provider-side failures should be reported as failures;
// a 400 caused by bad input says nothing about provider health.
package circuitbreaker

import (
	"sync"
	"time"
)

// State of a breaker.
type State int

// Breaker states. Values are exported as a Prometheus gauge.
const (
	StateClosed   State = 0
	StateHalfOpen State = 1
	StateOpen     State = 2
)

func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Config tunes a breaker.
type Config struct {
	FailureThreshold int
	OpenTimeout      time.Duration
	HalfOpenMaxCalls int
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg      Config
	now      func() time.Time
	onChange func(State)

	mu          sync.Mutex
	state       State
	failures    int
	openedAt    time.Time
	halfOpenUse int
}

// New creates a closed breaker. onChange (optional) is invoked on transitions.
func New(cfg Config, onChange func(State)) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMaxCalls <= 0 {
		cfg.HalfOpenMaxCalls = 1
	}
	return &Breaker{cfg: cfg, now: time.Now, onChange: onChange}
}

// Allow reports whether a call may proceed. When it returns false, the second
// value is how long until the breaker will consider probing again.
func (b *Breaker) Allow() (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateOpen:
		elapsed := b.now().Sub(b.openedAt)
		if elapsed < b.cfg.OpenTimeout {
			return false, b.cfg.OpenTimeout - elapsed
		}
		b.setState(StateHalfOpen)
		b.halfOpenUse = 0
		fallthrough
	case StateHalfOpen:
		if b.halfOpenUse >= b.cfg.HalfOpenMaxCalls {
			return false, b.cfg.OpenTimeout / 4
		}
		b.halfOpenUse++
		return true, 0
	default:
		return true, 0
	}
}

// Success records a healthy call.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	if b.state != StateClosed {
		b.setState(StateClosed)
	}
}

// Failure records a provider-side failure.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.trip()
	case StateClosed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.trip()
		}
	}
}

// Release gives back a half-open probe slot whose call never reached the provider.
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateHalfOpen && b.halfOpenUse > 0 {
		b.halfOpenUse--
	}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.openedAt = b.now()
	b.failures = 0
	b.setState(StateOpen)
}

func (b *Breaker) setState(s State) {
	if b.state == s {
		return
	}
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}
