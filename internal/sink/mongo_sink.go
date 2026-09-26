package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// MongoSinkConfig holds configuration for the MongoDB sink.
type MongoSinkConfig struct {
	ConnectionURL       string
	Database            string
	Collection          string
	AuthEnabled         bool
	AuthUsername        string
	AuthPassword        string
	AuthDB              string
	Mode                string // "insert" or "upsert"
	PrimaryKey          string // field name for upsert key
	ConnectTimeoutMs    int
	RetryBlacklistCodes []int // MongoDB error codes that go to DLQ (not retry)
}

// MongoSink writes messages to a MongoDB collection.
type MongoSink struct {
	client     *mongo.Client
	collection *mongo.Collection
	config     MongoSinkConfig
}

// NewMongoSink creates a MongoDB sink and connects to the database.
func NewMongoSink(cfg MongoSinkConfig) (*MongoSink, error) {
	if cfg.ConnectTimeoutMs <= 0 {
		cfg.ConnectTimeoutMs = 5000
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectTimeoutMs)*time.Millisecond)
	defer cancel()

	clientOpts := options.Client().ApplyURI(cfg.ConnectionURL)
	if cfg.AuthEnabled {
		clientOpts.SetAuth(options.Credential{
			Username:   cfg.AuthUsername,
			Password:   cfg.AuthPassword,
			AuthSource: cfg.AuthDB,
		})
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	collection := client.Database(cfg.Database).Collection(cfg.Collection)
	log.Printf("mongo sink connected: db=%s collection=%s mode=%s", cfg.Database, cfg.Collection, cfg.Mode)

	return &MongoSink{
		client:     client,
		collection: collection,
		config:     cfg,
	}, nil
}

// Push writes messages to MongoDB. For each message:
//   - insert mode: inserts the JSON document into the collection
//   - upsert mode: upserts based on PrimaryKey field
//
// Returns a list of failed messages (those that got an error from MongoDB).
// Duplicate-key errors (code 11000) are classified as non-retryable (→ DLQ):
// ErrorTypeSink4xx, StatusCode 409, Retried=true. All other errors are
// classified as retryable (→ retry with backoff): ErrorTypeSink5xx,
// StatusCode 500.
func (s *MongoSink) Push(msgs []Message) ([]errorpkg.FailedMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	var failed []errorpkg.FailedMessage

	for _, msg := range msgs {
		var doc bson.M
		if err := json.Unmarshal(msg.Value, &doc); err != nil {
			failed = append(failed, s.toFailedMessage(msg, errorpkg.ErrorInfo{
				ErrorType:  errorpkg.ErrorTypeDeserialization,
				StatusCode: 0,
				Message:    fmt.Sprintf("JSON unmarshal: %v", err),
			}, false))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var err error

		switch s.config.Mode {
		case "upsert":
			if s.config.PrimaryKey != "" {
				filter := bson.M{s.config.PrimaryKey: doc[s.config.PrimaryKey]}
				update := bson.M{"$set": doc}
				_, err = s.collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
			} else {
				_, err = s.collection.InsertOne(ctx, doc)
			}
		default:
			_, err = s.collection.InsertOne(ctx, doc)
		}
		cancel()

		if err != nil {
			if isBlacklistedOrDuplicate(err, s.config.RetryBlacklistCodes) {
				failed = append(failed, s.toFailedMessage(msg, errorpkg.ErrorInfo{
					ErrorType:  errorpkg.ErrorTypeSink4xx,
					StatusCode: 409,
					Message:    fmt.Sprintf("duplicate key: %v", err),
				}, true)) // Retried=true → DLQ, not retry
			} else {
				failed = append(failed, s.toFailedMessage(msg, errorpkg.ErrorInfo{
					ErrorType:  errorpkg.ErrorTypeSink5xx,
					StatusCode: 500,
					Message:    fmt.Sprintf("mongo error: %v", err),
				}, false)) // Retried=false → retry with backoff
			}
		}
	}

	return failed, nil
}

// Close disconnects the MongoDB client.
func (s *MongoSink) Close() error {
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.client.Disconnect(ctx)
	}
	return nil
}

// toFailedMessage converts a Message and ErrorInfo into a FailedMessage,
// preserving topic/partition/offset/key/value metadata.
func (s *MongoSink) toFailedMessage(msg Message, info errorpkg.ErrorInfo, retried bool) errorpkg.FailedMessage {
	return errorpkg.FailedMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		ErrorInfo: info,
		Retried:   retried,
	}
}

// isBlacklistedOrDuplicate reports whether a MongoDB error represents a
// duplicate-key error (code 11000) or matches one of the configured
// blacklist error codes. Such errors are non-retryable and should go to DLQ.
func isBlacklistedOrDuplicate(err error, blacklistCodes []int) bool {
	if mongo.IsDuplicateKeyError(err) {
		return true
	}
	if len(blacklistCodes) == 0 {
		return false
	}
	// ServerError is implemented by CommandError, WriteException, etc. Its
	// HasErrorCode method reliably reports the raw server error code
	// regardless of the concrete type wrapping it.
	var se mongo.ServerError
	if errors.As(err, &se) {
		for _, code := range blacklistCodes {
			if se.HasErrorCode(code) {
				return true
			}
		}
	}
	return false
}
