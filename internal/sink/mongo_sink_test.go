package sink

import (
	"testing"

	"go.mongodb.org/mongo-driver/mongo"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// TestMongoSinkImplementsSink verifies that *MongoSink satisfies the Sink
// interface at compile time without requiring a live MongoDB instance.
func TestMongoSinkImplementsSink(t *testing.T) {
	var _ Sink = (*MongoSink)(nil)
}

// TestMongoSinkConfigValidation verifies that NewMongoSink returns an error
// when given a connection URL that cannot be reached.
func TestMongoSinkConfigValidation(t *testing.T) {
	// Non-routable address guarantees a fast connect/ping failure.
	_, err := NewMongoSink(MongoSinkConfig{
		ConnectionURL:    "mongodb://255.255.255.255:1",
		Database:         "testdb",
		Collection:       "testcol",
		Mode:             "insert",
		ConnectTimeoutMs: 500,
	})
	if err == nil {
		t.Fatal("expected error connecting to bad URL, got nil")
	}
}

// TestMongoSinkDefaultConnectTimeout ensures that ConnectTimeoutMs <= 0 is
// defaulted to 5000ms and does not panic.
func TestMongoSinkDefaultConnectTimeout(t *testing.T) {
	_, err := NewMongoSink(MongoSinkConfig{
		ConnectionURL:    "mongodb://255.255.255.255:1",
		Database:         "testdb",
		Collection:       "testcol",
		Mode:             "insert",
		ConnectTimeoutMs: 0, // should be defaulted to 5000
	})
	if err == nil {
		t.Fatal("expected error connecting to bad URL, got nil")
	}
}

// TestMongoSinkDuplicateKeyClassification verifies that a MongoDB duplicate-key
// error (code 11000) is classified as non-retryable: ErrorTypeSink4xx,
// StatusCode 409, Retried=true (→ DLQ).
func TestMongoSinkDuplicateKeyClassification(t *testing.T) {
	// A WriteException wrapping a WriteError with code 11000 is the shape
	// InsertOne returns on a duplicate-key violation.
	dupErr := mongo.WriteException{
		WriteErrors: mongo.WriteErrors{
			{Code: 11000, Message: "E11000 duplicate key error"},
		},
	}

	if !isBlacklistedOrDuplicate(dupErr, nil) {
		t.Fatal("expected isBlacklistedOrDuplicate to return true for duplicate key error")
	}

	if !mongo.IsDuplicateKeyError(dupErr) {
		t.Fatal("expected mongo.IsDuplicateKeyError to return true for WriteException with code 11000")
	}
}

// TestMongoSinkRetryableErrorClassification verifies that a non-duplicate MongoDB
// error (e.g. command error with a non-11000 code) is classified as retryable
// (not in the blacklist → returns false → retry with backoff).
func TestMongoSinkRetryableErrorClassification(t *testing.T) {
	// A command error with code 133 (FailedToSatisfy) — not a duplicate key,
	// not in an empty blacklist → should be retryable.
	cmdErr := mongo.CommandError{
		Code:    133,
		Message: "query failed",
		Name:    "FailedToSatisfy",
	}

	if isBlacklistedOrDuplicate(cmdErr, nil) {
		t.Fatal("expected isBlacklistedOrDuplicate to return false for non-duplicate, non-blacklisted error")
	}
	if mongo.IsDuplicateKeyError(cmdErr) {
		t.Fatal("expected IsDuplicateKeyError to return false for non-duplicate command error")
	}
}

// TestMongoSinkBlacklistMatch verifies that an error whose code appears in the
// configured RetryBlacklistCodes is classified as non-retryable (→ DLQ).
func TestMongoSinkBlacklistMatch(t *testing.T) {
	cmdErr := mongo.CommandError{
		Code:    13, // Unauthorized — configured as blacklisted
		Message: "not authorized",
		Name:    "Unauthorized",
	}

	if !isBlacklistedOrDuplicate(cmdErr, []int{13}) {
		t.Fatal("expected isBlacklistedOrDuplicate to return true when code is in blacklist")
	}
}

// TestMongoSinkBlacklistNoMatch verifies that an error whose code does NOT
// appear in the blacklist is classified as retryable.
func TestMongoSinkBlacklistNoMatch(t *testing.T) {
	cmdErr := mongo.CommandError{
		Code:    6, // HostUnreachable
		Message: "connection refused",
		Name:    "HostUnreachable",
	}

	if isBlacklistedOrDuplicate(cmdErr, []int{13, 11000}) {
		t.Fatal("expected isBlacklistedOrDuplicate to return false when code is not in blacklist")
	}
}

// TestMongoSinkPushEmpty verifies that Push on an empty message slice is a
// no-op and returns no failures or error.
func TestMongoSinkPushEmpty(t *testing.T) {
	// Construct a MongoSink without connecting — Push only reads the
	// (empty) slice in the early-return path, so no live DB is needed.
	s := &MongoSink{config: MongoSinkConfig{Mode: "insert"}}
	failed, err := s.Push(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Fatalf("expected nil failed for empty push, got %d", len(failed))
	}
}

// TestMongoSinkPushDeserialization verifies that a message with invalid JSON is
// reported as a deserialization failure (ErrorTypeDeserialization) and is not
// retried (Retried=false → retry).
func TestMongoSinkPushDeserialization(t *testing.T) {
	s := &MongoSink{config: MongoSinkConfig{Mode: "insert"}}

	msgs := []Message{
		{Topic: "t1", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte(`{invalid-json`)},
	}

	failed, err := s.Push(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed message, got %d", len(failed))
	}

	fm := failed[0]
	if fm.ErrorInfo.ErrorType != errorpkg.ErrorTypeDeserialization {
		t.Errorf("expected error type %s, got %s", errorpkg.ErrorTypeDeserialization, fm.ErrorInfo.ErrorType)
	}
	if fm.ErrorInfo.StatusCode != 0 {
		t.Errorf("expected status code 0, got %d", fm.ErrorInfo.StatusCode)
	}
	if fm.Retried {
		t.Error("expected Retried=false for deserialization error (so it gets retried)")
	}
}
