// Package retry decides whether and when a failed delivery is attempted again.
//
// Design:
//   - Errors are classified as retryable (timeouts, connection errors, 408, 425, 429, 5xx)
//     or permanent (other 4xx: the request itself is wrong, retrying cannot help).
//   - Retryable failures back off exponentially with jitter so that a provider outage
//     during a burst does not turn into a synchronized retry storm when it recovers.
//   - A provider supplied Retry-After always wins over the computed delay (bounded by MaxDelay).
//   - When attempts are exhausted the notification moves to the terminal "failed" state
//     (our dead letter state) and can be requeued manually through the API.
package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy configures exponential backoff.
type Policy struct {
	MaxAttempts int           // total attempts including the first one
	BaseDelay   time.Duration // delay after the first failure
	MaxDelay    time.Duration // upper bound for any single delay
	Multiplier  float64       // growth factor per attempt
	// Jitter in [0,1]: the fraction of the delay that is randomized.
	// 0.5 means "equal jitter": delay/2 + rand(delay/2).
	Jitter float64
}

// DefaultPolicy is used when no explicit configuration is provided:
// ~2s, 4s, 8s, 16s between 5 attempts (plus jitter), never more than 10 minutes.
func DefaultPolicy() Policy {
	return Policy{MaxAttempts: 5, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Minute, Multiplier: 2, Jitter: 0.5}
}

// Normalize fills invalid fields with defaults.
func (p Policy) Normalize() Policy {
	d := DefaultPolicy()
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = d.BaseDelay
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = d.MaxDelay
	}
	if p.MaxDelay < p.BaseDelay {
		p.MaxDelay = p.BaseDelay
	}
	if p.Multiplier < 1 {
		p.Multiplier = d.Multiplier
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		p.Jitter = d.Jitter
	}
	return p
}

// Backoff returns the delay before the next attempt, given the number of attempts
// already made (1 after the first failure). rnd returns a value in [0,1); pass nil
// to use the global source.
func (p Policy) Backoff(attemptsMade int, rnd func() float64) time.Duration {
	if attemptsMade < 1 {
		attemptsMade = 1
	}
	if rnd == nil {
		rnd = rand.Float64
	}
	exp := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attemptsMade-1))
	if exp > float64(p.MaxDelay) || math.IsInf(exp, 0) || math.IsNaN(exp) {
		exp = float64(p.MaxDelay)
	}
	fixed := exp * (1 - p.Jitter)
	random := exp * p.Jitter * rnd()
	return time.Duration(fixed + random)
}

// Decision is the outcome of evaluating a failed attempt.
type Decision struct {
	Retry bool
	Delay time.Duration
}

// Decide combines classification, remaining attempts and Retry-After.
func (p Policy) Decide(attemptsMade int, retryable bool, retryAfter time.Duration) Decision {
	if !retryable || attemptsMade >= p.MaxAttempts {
		return Decision{Retry: false}
	}
	delay := p.Backoff(attemptsMade, nil)
	if retryAfter > 0 {
		delay = retryAfter
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}
	return Decision{Retry: true, Delay: delay}
}

// IsRetryableStatus classifies an HTTP status code returned by a provider.
func IsRetryableStatus(code int) bool {
	switch {
	case code == 408, code == 425, code == 429:
		return true
	case code >= 500:
		return true
	default:
		return false
	}
}
