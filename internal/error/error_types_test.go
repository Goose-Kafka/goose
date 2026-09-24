package errorpkg

import (
	"testing"
)

func TestErrorTypeFromStatusCode(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       ErrorType
	}{
		{"200 OK", 200, ErrorTypeNone},
		{"404 Not Found", 404, ErrorTypeSink4xx},
		{"400 Bad Request", 400, ErrorTypeSink4xx},
		{"499 Client Closed", 499, ErrorTypeSink4xx},
		{"500 Internal Server Error", 500, ErrorTypeSink5xx},
		{"503 Service Unavailable", 503, ErrorTypeSink5xx},
		{"599 Network Timeout", 599, ErrorTypeSink5xx},
		{"429 Too Many Requests is 4xx", 429, ErrorTypeSink4xx},
		{"200 lower bound", 200, ErrorTypeNone},
		{"299 upper bound of success", 299, ErrorTypeNone},
		{"600 unknown", 600, ErrorTypeSinkUnknown},
		{"100 unknown", 100, ErrorTypeSinkUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrorTypeFromStatusCode(tt.statusCode)
			if got != tt.want {
				t.Errorf("ErrorTypeFromStatusCode(%d) = %q, want %q", tt.statusCode, got, tt.want)
			}
		})
	}
}

func TestFailedMessageErrorInfo(t *testing.T) {
	fm := FailedMessage{
		Topic:     "events",
		Partition: 2,
		Offset:    42,
		Key:       []byte("order-123"),
		Value:     []byte(`{"event":"created"}`),
		ErrorInfo: ErrorInfo{
			ErrorType:  ErrorTypeSink5xx,
			StatusCode: 503,
			Message:    "service unavailable",
		},
		Retried: true,
	}

	if fm.Topic != "events" {
		t.Errorf("Topic = %q, want %q", fm.Topic, "events")
	}
	if fm.Partition != 2 {
		t.Errorf("Partition = %d, want %d", fm.Partition, 2)
	}
	if fm.Offset != 42 {
		t.Errorf("Offset = %d, want %d", fm.Offset, 42)
	}
	if string(fm.Key) != "order-123" {
		t.Errorf("Key = %q, want %q", string(fm.Key), "order-123")
	}
	if string(fm.Value) != `{"event":"created"}` {
		t.Errorf("Value = %q, want %q", string(fm.Value), `{"event":"created"}`)
	}
	if fm.ErrorInfo.ErrorType != ErrorTypeSink5xx {
		t.Errorf("ErrorInfo.ErrorType = %q, want %q", fm.ErrorInfo.ErrorType, ErrorTypeSink5xx)
	}
	if fm.ErrorInfo.StatusCode != 503 {
		t.Errorf("ErrorInfo.StatusCode = %d, want %d", fm.ErrorInfo.StatusCode, 503)
	}
	if fm.ErrorInfo.Message != "service unavailable" {
		t.Errorf("ErrorInfo.Message = %q, want %q", fm.ErrorInfo.Message, "service unavailable")
	}
	if !fm.Retried {
		t.Errorf("Retried = false, want true")
	}
}

func TestErrorInfoString(t *testing.T) {
	ei := ErrorInfo{
		ErrorType:  ErrorTypeSink5xx,
		StatusCode: 503,
		Message:    "service unavailable",
	}
	got := ei.String()
	if got == "" {
		t.Errorf("String() returned empty string, expected non-empty representation")
	}
	// Should contain the status code and message for useful debugging
	if !contains(got, "503") {
		t.Errorf("String() = %q, expected to contain status code 503", got)
	}
	if !contains(got, "service unavailable") {
		t.Errorf("String() = %q, expected to contain message", got)
	}
	if !contains(got, string(ErrorTypeSink5xx)) {
		t.Errorf("String() = %q, expected to contain error type %q", got, ErrorTypeSink5xx)
	}
}

// contains is a minimal helper to avoid importing strings in the test.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
