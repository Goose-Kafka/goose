package sink

import "testing"

func TestGrpcSinkImplementsSink(t *testing.T) {
	var _ Sink = (*GrpcSink)(nil)
}

func TestGrpcSinkConfigValidation(t *testing.T) {
	// Missing ServiceURL
	_, err := NewGrpcSink(GrpcSinkConfig{
		Method:    "events.Service/Process",
		TimeoutMs: 5000,
	})
	if err == nil {
		t.Fatal("expected error for missing service URL")
	}

	// Missing Method
	_, err = NewGrpcSink(GrpcSinkConfig{
		ServiceURL: "localhost:9090",
		TimeoutMs:  5000,
	})
	if err == nil {
		t.Fatal("expected error for missing method")
	}
}

func TestGrpcSinkEmptyPush(t *testing.T) {
	s := &GrpcSink{config: GrpcSinkConfig{Method: "test/Test"}}
	failed, err := s.Push(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Errorf("expected nil for empty push")
	}
}
