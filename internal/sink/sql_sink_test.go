package sink

import (
	"testing"
)

func TestSqlSinkImplementsSink(t *testing.T) {
	// Compile-time check that SqlSink implements Sink interface.
	var _ Sink = (*SqlSink)(nil)
}

func TestSqlSinkConfigValidation(t *testing.T) {
	// Bad connection URL should return error.
	_, err := NewSqlSink(SqlSinkConfig{
		DriverName:       "postgres",
		ConnectionURL:    "postgres://invalid:invalid@localhost:1/invalid?sslmode=disable&connect_timeout=1",
		TableName:        "test",
		ConnectTimeoutMs: 2000,
	})
	if err == nil {
		t.Fatal("expected error for bad connection")
	}
}

func TestSqlSinkEmptyPush(t *testing.T) {
	// Verify Push returns nil/nil for an empty slice without requiring a DB.
	s := &SqlSink{
		config: SqlSinkConfig{TableName: "test", Mode: "insert"},
	}
	failed, err := s.Push(nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Errorf("expected nil failed for empty push")
	}
}
