package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	errorpkg "github.com/Goose-Kafka/goose/internal/error"
)

// SqlSinkConfig holds configuration for the PostgreSQL/SQL sink.
type SqlSinkConfig struct {
	DriverName        string // "postgres" (future: "mysql", "sqlite")
	ConnectionURL     string // postgres://user:pass@host:5432/dbname?sslmode=disable
	TableName         string // e.g., "payment_records"
	Mode              string // "insert" or "upsert"
	PrimaryKeys       string // comma-separated key fields for upsert (e.g., "payment_id")
	ConnectionPoolMax int    // max DB connections
	ConnectTimeoutMs  int    // connection timeout
}

// SqlSink writes messages to a SQL database (PostgreSQL).
type SqlSink struct {
	db     *sql.DB
	config SqlSinkConfig
}

// NewSqlSink creates a SQL sink and verifies the database connection.
func NewSqlSink(cfg SqlSinkConfig) (*SqlSink, error) {
	db, err := sql.Open(cfg.DriverName, cfg.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}

	// Configure connection pool
	if cfg.ConnectionPoolMax > 0 {
		db.SetMaxOpenConns(cfg.ConnectionPoolMax)
		db.SetMaxIdleConns(cfg.ConnectionPoolMax / 2)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectTimeoutMs)*time.Millisecond)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("sql ping: %w", err)
	}

	log.Printf("sql sink connected: driver=%s table=%s mode=%s", cfg.DriverName, cfg.TableName, cfg.Mode)

	return &SqlSink{db: db, config: cfg}, nil
}

// Push writes messages to the SQL database.
// Each message's JSON is parsed into a map[string]interface{}, then:
//   - insert mode: INSERT INTO table (col1, col2, ...) VALUES ($1, $2, ...)
//   - upsert mode: INSERT ... ON CONFLICT (primary_keys) DO UPDATE SET ...
//
// Duplicate-key errors (SQL state 23505) in insert mode are classified as
// non-retryable (→ DLQ): ErrorTypeSink4xx, StatusCode 409, Retried=true.
// All other SQL errors (connection, timeout, etc.) are classified as
// retryable (→ retry with backoff): ErrorTypeSink5xx, StatusCode 500.
func (s *SqlSink) Push(msgs []Message) ([]errorpkg.FailedMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	var failed []errorpkg.FailedMessage

	for _, msg := range msgs {
		// Parse message JSON into a map
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

		// Build and execute SQL
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := s.executeMessage(ctx, data)
		cancel()

		if err != nil {
			// Classify error
			isUniqueViolation := strings.Contains(err.Error(), "unique constraint") ||
				strings.Contains(err.Error(), "duplicate key") ||
				strings.Contains(err.Error(), "23505")

			if isUniqueViolation {
				// Duplicate key in insert mode = non-retryable → DLQ
				failed = append(failed, errorpkg.FailedMessage{
					Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
					Key: msg.Key, Value: msg.Value,
					ErrorInfo: errorpkg.ErrorInfo{
						ErrorType:  errorpkg.ErrorTypeSink4xx,
						StatusCode: 409,
						Message:    fmt.Sprintf("duplicate key: %v", err),
					},
					Retried: true, // → DLQ, not retry
				})
			} else {
				// Other SQL errors = retryable (connection, timeout, etc.)
				failed = append(failed, errorpkg.FailedMessage{
					Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
					Key: msg.Key, Value: msg.Value,
					ErrorInfo: errorpkg.ErrorInfo{
						ErrorType:  errorpkg.ErrorTypeSink5xx,
						StatusCode: 500,
						Message:    fmt.Sprintf("sql error: %v", err),
					},
				})
			}
		}
	}

	return failed, nil
}

// executeMessage builds and executes the SQL statement for one message.
// Columns are sorted alphabetically for consistent placeholder/value ordering,
// since map iteration order is non-deterministic in Go.
func (s *SqlSink) executeMessage(ctx context.Context, data map[string]interface{}) error {
	// Collect column names and sort them for deterministic ordering.
	keys := make([]string, 0, len(data))
	for col := range data {
		keys = append(keys, col)
	}
	sort.Strings(keys)

	// Build placeholder list and values in the same sorted order.
	placeholders := make([]string, 0, len(keys))
	values := make([]interface{}, 0, len(keys))
	for i, col := range keys {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		values = append(values, data[col])
	}

	colList := strings.Join(keys, ", ")
	valList := strings.Join(placeholders, ", ")

	var query string
	switch s.config.Mode {
	case "upsert":
		pks := strings.Split(s.config.PrimaryKeys, ",")
		for i, pk := range pks {
			pks[i] = strings.TrimSpace(pk)
		}
		pkList := strings.Join(pks, ", ")

		// Build ON CONFLICT DO UPDATE SET
		setClauses := make([]string, 0, len(keys))
		for _, col := range keys {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
		setList := strings.Join(setClauses, ", ")

		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			s.config.TableName, colList, valList, pkList, setList)

	default: // insert
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			s.config.TableName, colList, valList)
	}

	_, err := s.db.ExecContext(ctx, query, values...)
	return err
}

// Close closes the database connection.
func (s *SqlSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
