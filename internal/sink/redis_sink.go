package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// RedisSinkConfig holds configuration for the Redis sink.
type RedisSinkConfig struct {
	Addresses        string // comma-separated Redis addresses
	Password         string
	Database         int    // Redis database number
	DataType         string // "keyvalue" or "hashset" or "list"
	KeyTemplate      string // e.g., "payment:%s" where %s is replaced by the key field value
	KeyField         string // JSON field name to extract the key from (e.g., "payment_id")
	HashField        string // for hashset mode: field name (or extracted from message)
	DeploymentType   string // "standalone" or "cluster"
	TTLType          string // "DISABLE" or "EX" or "EXAT"
	TTLValue         int    // TTL in seconds
	ConnectTimeoutMs int
}

// RedisSink writes messages to Redis.
type RedisSink struct {
	client redis.Cmdable
	config RedisSinkConfig
}

// NewRedisSink creates a Redis sink and verifies the connection.
func NewRedisSink(cfg RedisSinkConfig) (*RedisSink, error) {
	addrs := strings.Split(cfg.Addresses, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	var client redis.Cmdable
	if cfg.DeploymentType == "cluster" {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addrs,
			Password: cfg.Password,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:     addrs[0],
			Password: cfg.Password,
			DB:       cfg.Database,
		})
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectTimeoutMs)*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	log.Printf("redis sink connected: addrs=%s type=%s mode=%s", cfg.Addresses, cfg.DeploymentType, cfg.DataType)

	return &RedisSink{client: client, config: cfg}, nil
}

// Push writes each message to Redis based on the configured data type.
func (s *RedisSink) Push(msgs []Message) ([]errorpkg.FailedMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	var failed []errorpkg.FailedMessage

	for _, msg := range msgs {
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			failed = append(failed, errorpkg.FailedMessage{
				Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
				Key: msg.Key, Value: msg.Value,
				ErrorInfo: errorpkg.ErrorInfo{
					ErrorType:  errorpkg.ErrorTypeDeserialization,
					StatusCode: 0,
					Message:    fmt.Sprintf("JSON unmarshal: %v", err),
				},
			})
			continue
		}

		// Build the Redis key
		key := s.buildKey(data, msg)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error

		switch s.config.DataType {
		case "hashset":
			// Store message as a hash: key → field → value
			field := s.config.HashField
			if field == "" {
				field = "data"
			}
			// Extract field value from data if it's a reference to a JSON field
			if strings.HasPrefix(field, "$.") {
				fieldName := strings.TrimPrefix(field, "$.")
				if v, ok := data[fieldName]; ok {
					field = fmt.Sprintf("%v", v)
				}
			}
			err = s.client.HSet(ctx, key, field, string(msg.Value)).Err()

		case "list":
			// Push message value to a list
			err = s.client.LPush(ctx, key, msg.Value).Err()

		default: // keyvalue
			// Simple key-value: key → message JSON
			err = s.client.Set(ctx, key, msg.Value, s.getTTL()).Err()
		}
		cancel()

		if err != nil {
			failed = append(failed, errorpkg.FailedMessage{
				Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
				Key: msg.Key, Value: msg.Value,
				ErrorInfo: errorpkg.ErrorInfo{
					ErrorType:  errorpkg.ErrorTypeSink5xx,
					StatusCode: 500,
					Message:    fmt.Sprintf("redis error: %v", err),
				},
			})
		}
	}

	return failed, nil
}

// buildKey constructs the Redis key from the template and message data.
func (s *RedisSink) buildKey(data map[string]interface{}, msg Message) string {
	if s.config.KeyTemplate == "" {
		// Default: use the message key or "goose:{offset}"
		if len(msg.Key) > 0 {
			return string(msg.Key)
		}
		return fmt.Sprintf("goose:%d", msg.Offset)
	}

	// Extract key field value from the message JSON
	keyValue := ""
	if s.config.KeyField != "" {
		if v, ok := data[s.config.KeyField]; ok {
			keyValue = fmt.Sprintf("%v", v)
		}
	}

	// Replace %s in template with the key value
	key := strings.ReplaceAll(s.config.KeyTemplate, "%s", keyValue)
	key = strings.ReplaceAll(key, "%%s", keyValue) // handle double-escaped
	return key
}

// getTTL returns the Redis TTL duration based on config.
func (s *RedisSink) getTTL() time.Duration {
	if s.config.TTLType == "DISABLE" || s.config.TTLType == "" || s.config.TTLValue <= 0 {
		return 0 // no expiration
	}
	return time.Duration(s.config.TTLValue) * time.Second
}

// Close closes the Redis client.
func (s *RedisSink) Close() error {
	if c, ok := s.client.(*redis.Client); ok {
		return c.Close()
	}
	if c, ok := s.client.(*redis.ClusterClient); ok {
		return c.Close()
	}
	return nil
}
