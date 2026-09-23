package errorpkg

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	b := NewExponentialBackoff(100, 10000, 2.0)
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
	}
	for _, tt := range tests {
		got := b.Backoff(tt.attempt)
		if got != tt.want {
			t.Errorf("Backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestExponentialBackoffMaxCap(t *testing.T) {
	b := NewExponentialBackoff(100, 500, 2.0)
	got := b.Backoff(5)
	want := 500 * time.Millisecond
	if got != want {
		t.Errorf("Backoff(5) = %v, want %v (capped at max)", got, want)
	}
}

func TestExponentialBackoffZeroAttempt(t *testing.T) {
	b := NewExponentialBackoff(100, 10000, 2.0)
	got := b.Backoff(0)
	want := 100 * time.Millisecond
	if got != want {
		t.Errorf("Backoff(0) = %v, want %v (same as attempt 1)", got, want)
	}
}
