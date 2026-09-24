package config

import (
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	// Required fields with no defaults: provide valid values so validation
	// passes. Brokers has a default, so clear it to exercise the default path.
	t.Setenv("SOURCE_KAFKA_TOPIC", "default-test-topic")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://default:8080/api")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "default-test-group")
	t.Setenv("SINK_HTTP_DLQ_KAFKA_BROKERS", "localhost:9092")

	// Clear optional fields so defaults are exercised.
	for _, key := range []string{
		"SOURCE_KAFKA_BROKERS",
		"SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS",
		"SOURCE_KAFKA_POLL_TIMEOUT_MS",
		"SOURCE_KAFKA_MAX_POLL_INTERVAL_MS",
		"SOURCE_KAFKA_SESSION_TIMEOUT_MS",
		"SOURCE_KAFKA_AUTO_OFFSET_RESET",
		"SOURCE_KAFKA_COMMIT_INTERVAL_MS",
		"SINK_HTTP_REQUEST_METHOD",
		"SINK_HTTP_REQUEST_TIMEOUT_MS",
		"SINK_HTTP_MAX_CONNECTIONS",
		"SINK_HTTP_CONNECTION_TTL_MS",
		"SINK_HTTP_CONNECTION_IDLE_EVICT_MS",
		"SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS",
		"SINK_HTTP_DATA_FORMAT",
		"SINK_WORKER_POOL_SIZE",
		"SINK_WORKER_POOL_BUFFER",
		"SINK_HTTP_RETRY_MAX_ATTEMPTS",
		"SINK_HTTP_RETRY_BACKOFF_INITIAL_MS",
		"SINK_HTTP_RETRY_BACKOFF_MAX_MS",
		"SINK_HTTP_RETRY_BACKOFF_MULTIPLIER",
		"SINK_HTTP_CIRCUIT_BREAKER_ENABLED",
		"SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
		"SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE",
		"SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS",
		"SINK_HTTP_DLQ_ENABLED",
		"SINK_HTTP_DLQ_TYPE",
	} {
		t.Setenv(key, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Kafka.Brokers != "localhost:9092" {
		t.Errorf("Kafka.Brokers = %q, want %q", cfg.Kafka.Brokers, "localhost:9092")
	}
	if cfg.HTTP.WorkerPoolSize != 10 {
		t.Errorf("HTTP.WorkerPoolSize = %d, want 10", cfg.HTTP.WorkerPoolSize)
	}
	if cfg.HTTP.WorkerPoolBuffer != 10 {
		t.Errorf("HTTP.WorkerPoolBuffer = %d, want 10", cfg.HTTP.WorkerPoolBuffer)
	}
	if cfg.HTTP.RequestTimeoutMs != 10000 {
		t.Errorf("HTTP.RequestTimeoutMs = %d, want 10000", cfg.HTTP.RequestTimeoutMs)
	}
	if cfg.HTTP.MaxConnections != 10 {
		t.Errorf("HTTP.MaxConnections = %d, want 10", cfg.HTTP.MaxConnections)
	}
	if cfg.HTTP.ConnectionTtlMs != 30000 {
		t.Errorf("HTTP.ConnectionTtlMs = %d, want 30000", cfg.HTTP.ConnectionTtlMs)
	}
	if cfg.HTTP.ConnectionIdleEvictMs != 30000 {
		t.Errorf("HTTP.ConnectionIdleEvictMs = %d, want 30000", cfg.HTTP.ConnectionIdleEvictMs)
	}
	if cfg.HTTP.ConnectionValidateInactivityMs != 2000 {
		t.Errorf("HTTP.ConnectionValidateInactivityMs = %d, want 2000", cfg.HTTP.ConnectionValidateInactivityMs)
	}
	if cfg.HTTP.RetryMaxAttempts != 3 {
		t.Errorf("HTTP.RetryMaxAttempts = %d, want 3", cfg.HTTP.RetryMaxAttempts)
	}
	if cfg.HTTP.RetryBackoffInitialMs != 100 {
		t.Errorf("HTTP.RetryBackoffInitialMs = %d, want 100", cfg.HTTP.RetryBackoffInitialMs)
	}
	if cfg.HTTP.RetryBackoffMaxMs != 10000 {
		t.Errorf("HTTP.RetryBackoffMaxMs = %d, want 10000", cfg.HTTP.RetryBackoffMaxMs)
	}
	if cfg.HTTP.RetryBackoffMultiplier != 2.0 {
		t.Errorf("HTTP.RetryBackoffMultiplier = %v, want 2.0", cfg.HTTP.RetryBackoffMultiplier)
	}
	if !cfg.HTTP.CircuitBreakerEnabled {
		t.Errorf("HTTP.CircuitBreakerEnabled = false, want true")
	}
	if cfg.HTTP.CircuitBreakerFailureThreshold != 80 {
		t.Errorf("HTTP.CircuitBreakerFailureThreshold = %d, want 80", cfg.HTTP.CircuitBreakerFailureThreshold)
	}
	if cfg.HTTP.CircuitBreakerWindowSize != 100 {
		t.Errorf("HTTP.CircuitBreakerWindowSize = %d, want 100", cfg.HTTP.CircuitBreakerWindowSize)
	}
	if cfg.HTTP.CircuitBreakerResetTimeoutMs != 10000 {
		t.Errorf("HTTP.CircuitBreakerResetTimeoutMs = %d, want 10000", cfg.HTTP.CircuitBreakerResetTimeoutMs)
	}
	if !cfg.HTTP.DLQEnabled {
		t.Errorf("HTTP.DLQEnabled = false, want true")
	}
	if cfg.HTTP.DLQType != "kafka" {
		t.Errorf("HTTP.DLQType = %q, want %q", cfg.HTTP.DLQType, "kafka")
	}
	if cfg.HTTP.DataFormat != "json" {
		t.Errorf("HTTP.DataFormat = %q, want %q", cfg.HTTP.DataFormat, "json")
	}
	if cfg.HTTP.RequestMethod != "POST" {
		t.Errorf("HTTP.RequestMethod = %q, want %q", cfg.HTTP.RequestMethod, "POST")
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC", "test-events")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "test-group")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://test:8080/api")
	t.Setenv("SINK_WORKER_POOL_SIZE", "20")
	t.Setenv("SINK_HTTP_REQUEST_METHOD", "PUT")
	t.Setenv("SINK_HTTP_DLQ_KAFKA_BROKERS", "broker1:9092,broker2:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.Kafka.Brokers != "broker1:9092,broker2:9092" {
		t.Errorf("Kafka.Brokers = %q, want %q", cfg.Kafka.Brokers, "broker1:9092,broker2:9092")
	}
	if cfg.Kafka.Topic != "test-events" {
		t.Errorf("Kafka.Topic = %q, want %q", cfg.Kafka.Topic, "test-events")
	}
	if cfg.Kafka.ConsumerGroupID != "test-group" {
		t.Errorf("Kafka.ConsumerGroupID = %q, want %q", cfg.Kafka.ConsumerGroupID, "test-group")
	}
	if cfg.HTTP.ServiceURL != "http://test:8080/api" {
		t.Errorf("HTTP.ServiceURL = %q, want %q", cfg.HTTP.ServiceURL, "http://test:8080/api")
	}
	if cfg.HTTP.WorkerPoolSize != 20 {
		t.Errorf("HTTP.WorkerPoolSize = %d, want 20", cfg.HTTP.WorkerPoolSize)
	}
	if cfg.HTTP.RequestMethod != "PUT" {
		t.Errorf("HTTP.RequestMethod = %q, want %q", cfg.HTTP.RequestMethod, "PUT")
	}
}

func TestLoadConfigValidationMissingRequired(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "")
	t.Setenv("SOURCE_KAFKA_TOPIC", "")
	t.Setenv("SINK_HTTP_SERVICE_URL", "")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "")

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() returned nil error, want error for missing required fields")
	}
}

func TestLoadConfigValidationGroupIDRequired(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC", "test")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://test:8080/api")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "")

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() returned nil error, want error for missing consumer group ID")
	}
}

func TestLoadConfigValidationWorkerPoolSizeRequired(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC", "test")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://test:8080/api")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "test-group")
	t.Setenv("SINK_WORKER_POOL_SIZE", "0")

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() returned nil error, want error for WorkerPoolSize <= 0")
	}
}

func TestLoadConfigValidationMaxConnectionsRequired(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC", "test")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://test:8080/api")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "test-group")
	t.Setenv("SINK_HTTP_MAX_CONNECTIONS", "0")

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() returned nil error, want error for MaxConnections <= 0")
	}
}

func TestLoadConfigValidationDLQBrokersRequired(t *testing.T) {
	t.Setenv("SOURCE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC", "test")
	t.Setenv("SINK_HTTP_SERVICE_URL", "http://test:8080/api")
	t.Setenv("SOURCE_KAFKA_CONSUMER_GROUP_ID", "test-group")
	t.Setenv("SINK_HTTP_DLQ_ENABLED", "true")
	t.Setenv("SINK_HTTP_DLQ_TYPE", "kafka")
	t.Setenv("SINK_HTTP_DLQ_KAFKA_BROKERS", "")

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() returned nil error, want error for missing DLQ Kafka brokers")
	}
}

func TestStatusRangeParsing(t *testing.T) {
	r := ParseStatusRange("500-599")
	if !r.Matches(503) {
		t.Errorf("ParseStatusRange(\"500-599\").Matches(503) = false, want true")
	}
	if r.Matches(404) {
		t.Errorf("ParseStatusRange(\"500-599\").Matches(404) = true, want false")
	}

	single := ParseStatusRange("429")
	if !single.Matches(429) {
		t.Errorf("ParseStatusRange(\"429\").Matches(429) = false, want true")
	}
	if single.Matches(430) {
		t.Errorf("ParseStatusRange(\"429\").Matches(430) = true, want false")
	}
}

func TestStatusRangeListParsing(t *testing.T) {
	list := ParseStatusRangeList("500-599,429,408")
	for _, code := range []int{503, 429, 408} {
		if !list.Matches(code) {
			t.Errorf("ParseStatusRangeList(\"500-599,429,408\").Matches(%d) = false, want true", code)
		}
	}
	if list.Matches(404) {
		t.Errorf("ParseStatusRangeList(\"500-599,429,408\").Matches(404) = true, want false")
	}
}
