package filter

import (
	"testing"
)

func TestNoOpFilter(t *testing.T) {
	f := NewNoOpFilter()
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte(`{"status":"created"}`)},
		{Topic: "t", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"status":"shipped"}`)},
	}
	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Fatalf("expected passed=2, got %d", len(passed))
	}
	if len(dropped) != 0 {
		t.Fatalf("expected dropped=0, got %d", len(dropped))
	}
}

func TestJSONPathFilter(t *testing.T) {
	f, err := NewJSONPathFilter(FilterConfig{Expression: "$..status", MatchValue: "created"})
	if err != nil {
		t.Fatalf("unexpected error creating filter: %v", err)
	}
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte(`{"status":"created"}`)},
		{Topic: "t", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"status":"shipped"}`)},
		{Topic: "t", Partition: 0, Offset: 2, Key: []byte("k2"), Value: []byte(`{"status":"created"}`)},
		{Topic: "t", Partition: 0, Offset: 3, Key: []byte("k3"), Value: []byte(`{"other":"data"}`)},
	}
	passed, dropped := f.Apply(msgs)
	if len(passed) != 2 {
		t.Fatalf("expected passed=2, got %d (passed: %+v)", len(passed), passed)
	}
	if len(dropped) != 2 {
		t.Fatalf("expected dropped=2, got %d (dropped: %+v)", len(dropped), dropped)
	}
}

func TestJSONPathFilterInvalidJSON(t *testing.T) {
	f, err := NewJSONPathFilter(FilterConfig{Expression: "$..status", MatchValue: "created"})
	if err != nil {
		t.Fatalf("unexpected error creating filter: %v", err)
	}
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte(`not json`)},
	}
	passed, dropped := f.Apply(msgs)
	if len(passed) != 0 {
		t.Fatalf("expected passed=0, got %d", len(passed))
	}
	if len(dropped) != 1 {
		t.Fatalf("expected dropped=1, got %d", len(dropped))
	}
}

func TestJSONPathFilterComplexPath(t *testing.T) {
	f, err := NewJSONPathFilter(FilterConfig{Expression: "$.order.status", MatchValue: "pending"})
	if err != nil {
		t.Fatalf("unexpected error creating filter: %v", err)
	}
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte(`{"order":{"status":"pending"}}`)},
		{Topic: "t", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{"order":{"status":"completed"}}`)},
	}
	passed, dropped := f.Apply(msgs)
	if len(passed) != 1 {
		t.Fatalf("expected passed=1, got %d (passed: %+v)", len(passed), passed)
	}
	if len(dropped) != 1 {
		t.Fatalf("expected dropped=1, got %d (dropped: %+v)", len(dropped), dropped)
	}
}
