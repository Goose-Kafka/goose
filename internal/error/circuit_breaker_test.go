package errorpkg

import (
	"testing"
	"time"
)

func TestCircuitBreakerStartsClosed(t *testing.T) {
	cb := NewCircuitBreaker(80, 100, 10*time.Second)
	if !cb.Allow() {
		t.Error("Allow() = false, want true (circuit should start closed)")
	}
	if cb.IsOpen() {
		t.Error("IsOpen() = true, want false (circuit should start closed)")
	}
}

func TestCircuitBreakerOpensOnHighFailureRate(t *testing.T) {
	cb := NewCircuitBreaker(80, 100, 10*time.Second)
	// 81 failures + 19 successes = 81% failure rate > 80% threshold
	for i := 0; i < 81; i++ {
		cb.RecordFailure()
	}
	for i := 0; i < 19; i++ {
		cb.RecordSuccess()
	}
	if !cb.IsOpen() {
		t.Error("IsOpen() = false, want true (81% > 80% threshold)")
	}
	if cb.Allow() {
		t.Error("Allow() = true, want false (circuit is open)")
	}
}

func TestCircuitBreakerStaysClosedOnLowFailureRate(t *testing.T) {
	cb := NewCircuitBreaker(80, 100, 10*time.Second)
	// 79 failures + 21 successes = 79% failure rate < 80% threshold
	for i := 0; i < 79; i++ {
		cb.RecordFailure()
	}
	for i := 0; i < 21; i++ {
		cb.RecordSuccess()
	}
	if cb.IsOpen() {
		t.Error("IsOpen() = true, want false (79% < 80% threshold)")
	}
}

func TestCircuitBreakerClosesAfterResetTimeout(t *testing.T) {
	cb := NewCircuitBreaker(80, 100, 50*time.Millisecond)
	// 100 failures → opens
	for i := 0; i < 100; i++ {
		cb.RecordFailure()
	}
	if !cb.IsOpen() {
		t.Fatal("IsOpen() = false, want true (100% failure rate should open)")
	}
	// Wait for reset timeout to elapse
	time.Sleep(60 * time.Millisecond)
	// Allow() should now permit a probe request
	if !cb.Allow() {
		t.Error("Allow() = false, want true (reset timeout elapsed, probe allowed)")
	}
	// Record a success on the probe → circuit closes
	cb.RecordSuccess()
	if cb.IsOpen() {
		t.Error("IsOpen() = true, want false (probe success should close circuit)")
	}
}

func TestCircuitBreakerSlidingWindow(t *testing.T) {
	cb := NewCircuitBreaker(80, 100, 10*time.Second)
	// 100 failures → opens
	for i := 0; i < 100; i++ {
		cb.RecordFailure()
	}
	if !cb.IsOpen() {
		t.Fatal("IsOpen() = false, want true (100% failure rate should open)")
	}
	// 100 successes → sliding window evicts old failures
	for i := 0; i < 100; i++ {
		cb.RecordSuccess()
	}
	if cb.IsOpen() {
		t.Error("IsOpen() = true, want false (sliding window evicted all failures)")
	}
}
