package errorpkg

import (
	"math"
	"time"
)

// ExponentialBackoff calculates retry delays that grow exponentially with
// each attempt, capped at a maximum value.
type ExponentialBackoff struct {
	InitialMs  int
	MaxMs      int
	Multiplier float64
}

// NewExponentialBackoff creates a new backoff strategy.
//   - initialMs: delay for the first attempt
//   - maxMs:     upper bound on the delay
//   - multiplier: factor by which the delay grows each attempt
func NewExponentialBackoff(initialMs, maxMs int, multiplier float64) *ExponentialBackoff {
	return &ExponentialBackoff{InitialMs: initialMs, MaxMs: maxMs, Multiplier: multiplier}
}

// Backoff returns the delay for the given attempt number (1-based).
// Attempt 0 or negative is treated as attempt 1.
func (b *ExponentialBackoff) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delayMs := float64(b.InitialMs) * math.Pow(b.Multiplier, float64(attempt-1))
	if delayMs > float64(b.MaxMs) {
		delayMs = float64(b.MaxMs)
	}
	return time.Duration(delayMs) * time.Millisecond
}
