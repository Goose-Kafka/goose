package errorpkg

import (
	"sync"
	"time"
)

// CircuitBreaker implements a sliding-window circuit breaker. It tracks the
// outcome of the last windowSize requests, opens when the failure rate reaches
// failureThreshold percent, and auto-closes after resetTimeout (via a probe
// request through Allow) or when the sliding window's failure rate drops back
// below the threshold.
type CircuitBreaker struct {
	mu               sync.Mutex
	failureThreshold int           // percentage 0-100 at which the circuit opens
	windowSize       int           // number of recent requests to consider
	results          []bool        // true = success, false = failure (oldest first)
	openedAt         time.Time     // when the circuit transitioned to open
	resetTimeout     time.Duration // how long to wait before allowing a probe
	isOpen           bool
}

// NewCircuitBreaker creates a circuit breaker that opens when the failure rate
// over the last windowSize requests reaches failureThreshold percent, and
// attempts to recover after resetTimeout.
func NewCircuitBreaker(failureThreshold, windowSize int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		windowSize:       windowSize,
		resetTimeout:     resetTimeout,
	}
}

// Allow reports whether a request may proceed. It returns true when the
// circuit is closed. When the circuit is open and the reset timeout has
// elapsed, it transitions to half-open (clearing the window so the probe is
// evaluated against a fresh sample) and returns true for a single probe
// request. Otherwise it returns false.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if !cb.isOpen {
		return true
	}
	if time.Since(cb.openedAt) >= cb.resetTimeout {
		cb.isOpen = false
		cb.results = nil
		return true
	}
	return false
}

// IsOpen reports whether the circuit is currently open.
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.isOpen
}

// RecordSuccess records a successful request and re-evaluates the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.recordResult(true)
}

// RecordFailure records a failed request and re-evaluates the circuit.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.recordResult(false)
}

// recordResult appends an outcome to the sliding window and re-evaluates the
// circuit state. The caller must hold cb.mu.
func (cb *CircuitBreaker) recordResult(success bool) {
	cb.results = append(cb.results, success)
	// Evict the oldest entry once the window is full, shifting elements in
	// place to avoid unbounded backing-array growth.
	if len(cb.results) > cb.windowSize {
		copy(cb.results, cb.results[1:])
		cb.results = cb.results[:cb.windowSize]
	}
	// Only evaluate once we have a full window of samples.
	if len(cb.results) == cb.windowSize {
		failures := 0
		for _, r := range cb.results {
			if !r {
				failures++
			}
		}
		rate := failures * 100 / cb.windowSize
		if rate >= cb.failureThreshold {
			if !cb.isOpen {
				cb.isOpen = true
				cb.openedAt = time.Now()
			}
		} else {
			cb.isOpen = false
		}
	}
}
