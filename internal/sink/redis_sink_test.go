package sink

import (
	"testing"
)

func TestRedisSinkImplementsSink(t *testing.T) {
	var _ Sink = (*RedisSink)(nil)
}

func TestRedisSinkConfigValidation(t *testing.T) {
	// Bad address should fail
	_, err := NewRedisSink(RedisSinkConfig{
		Addresses:        "localhost:1",
		ConnectTimeoutMs: 2000,
	})
	if err == nil {
		t.Fatal("expected error for bad Redis connection")
	}
}

func TestRedisSinkEmptyPush(t *testing.T) {
	s := &RedisSink{config: RedisSinkConfig{DataType: "keyvalue"}}
	failed, err := s.Push(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Errorf("expected nil for empty push")
	}
}

func TestRedisSinkBuildKey(t *testing.T) {
	s := &RedisSink{config: RedisSinkConfig{
		KeyTemplate: "payment:%s",
		KeyField:    "payment_id",
	}}

	data := map[string]interface{}{"payment_id": "PAY-123"}
	key := s.buildKey(data, Message{})
	if key != "payment:PAY-123" {
		t.Errorf("expected payment:PAY-123, got %s", key)
	}
}

func TestRedisSinkBuildKeyNoField(t *testing.T) {
	s := &RedisSink{config: RedisSinkConfig{
		KeyField: "missing_field",
	}}

	data := map[string]interface{}{"other": "value"}
	key := s.buildKey(data, Message{Offset: 42})
	// Should fall back to goose:42 since field not found and no template
	if key != "goose:42" {
		t.Errorf("expected goose:42, got %s", key)
	}
}

func TestRedisSinkGetTTL(t *testing.T) {
	s1 := &RedisSink{config: RedisSinkConfig{TTLType: "DISABLE", TTLValue: 100}}
	if s1.getTTL() != 0 {
		t.Error("DISABLE should return 0 TTL")
	}

	s2 := &RedisSink{config: RedisSinkConfig{TTLType: "EX", TTLValue: 3600}}
	if s2.getTTL() != 3600*1e9 {
		t.Errorf("EX 3600 should return 3600s, got %v", s2.getTTL())
	}
}
