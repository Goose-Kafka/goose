package config

import (
	"errors"
	"strconv"
	"strings"
)

// Config is the top-level configuration for the goose firehose, assembled from
// environment variables.
type Config struct {
	Kafka    KafkaConfig
	HTTP     HTTPConfig
	Schema   SchemaConfig
	Metrics  MetricsConfig
	Tracing  TracingConfig
}

// KafkaConfig holds source Kafka consumer settings.
type KafkaConfig struct {
	Brokers            string
	Topic              string
	ConsumerGroupID    string
	MaxPollRecords     int
	PollTimeoutMs      int
	MaxPollIntervalMs  int
	SessionTimeoutMs   int
	AutoOffsetReset    string
	CommitIntervalMs   int
}

// HTTPConfig holds sink HTTP client settings including retry, circuit breaker,
// DLQ, and OAuth2 configuration.
type HTTPConfig struct {
	ServiceURL                       string
	RequestMethod                    string
	RequestTimeoutMs                 int
	MaxConnections                   int
	ConnectionTtlMs                  int
	ConnectionIdleEvictMs            int
	ConnectionValidateInactivityMs   int
	Headers                          map[string]string
	DataFormat                       string
	JSONBodyTemplate                 string
	WorkerPoolSize                   int
	WorkerPoolBuffer                 int
	ErrorRetryStatusCodes            StatusRangeList
	ErrorDLQStatusCodes              StatusRangeList
	ErrorFailStatusCodes             StatusRangeList
	RetryMaxAttempts                 int
	RetryBackoffInitialMs            int
	RetryBackoffMaxMs                int
	RetryBackoffMultiplier            float64
	CircuitBreakerEnabled            bool
	CircuitBreakerFailureThreshold   int
	CircuitBreakerWindowSize         int
	CircuitBreakerResetTimeoutMs     int
	DLQEnabled                       bool
	DLQType                          string
	DLQKafkaTopic                    string
	DLQKafkaBrokers                  string
	OAuth2Enabled                    bool
	OAuth2AccessTokenURL             string
	OAuth2ClientName                 string
	OAuth2ClientSecret               string
	OAuth2Scope                      string
	FilterEnabled                    bool
	FilterJSONPath                   string
}

// SchemaConfig holds schema registry and input data type settings.
type SchemaConfig struct {
	InputSchemaDataType      string
	SchemaRegistryEnabled    bool
	SchemaRegistryURL        string
	SchemaRegistryProtoClass string
	RefreshStrategy          string
	RefreshIntervalMs        int
	FetchTimeoutMs           int
	AuthBearerToken          string
}

// MetricsConfig holds Prometheus metrics settings.
type MetricsConfig struct {
	PrometheusEnabled bool
	PrometheusPort    int
}

// TracingConfig holds OpenTelemetry tracing settings.
type TracingConfig struct {
	Enabled     bool
	OTLPEndpoint string
	ServiceName string
}

// StatusRange represents an inclusive range of HTTP status codes.
type StatusRange struct {
	Start int
	End   int
}

// Matches reports whether the given HTTP status code falls within this range.
func (r StatusRange) Matches(code int) bool {
	return code >= r.Start && code <= r.End
}

// StatusRangeList is a slice of StatusRange values that supports membership
// testing for an HTTP status code.
type StatusRangeList []StatusRange

// Matches reports whether the given HTTP status code matches any of the ranges
// in the list.
func (l StatusRangeList) Matches(code int) bool {
	for _, r := range l {
		if r.Matches(code) {
			return true
		}
	}
	return false
}

// ParseStatusRange parses a single status-code range such as "500-599" or a
// single status code such as "429".
func ParseStatusRange(s string) StatusRange {
	s = strings.TrimSpace(s)
	if s == "" {
		return StatusRange{Start: 0, End: -1} // matches nothing
	}
	if idx := strings.Index(s, "-"); idx >= 0 {
		start, _ := strconv.Atoi(strings.TrimSpace(s[:idx]))
		end, _ := strconv.Atoi(strings.TrimSpace(s[idx+1:]))
		return StatusRange{Start: start, End: end}
	}
	code, _ := strconv.Atoi(s)
	return StatusRange{Start: code, End: code}
}

// ParseStatusRangeList parses a comma-separated list of status-code ranges
// such as "500-599,429,408".
func ParseStatusRangeList(s string) StatusRangeList {
	s = strings.TrimSpace(s)
	if s == "" {
		return StatusRangeList{}
	}
	parts := strings.Split(s, ",")
	list := make(StatusRangeList, 0, len(parts))
	for _, part := range parts {
		list = append(list, ParseStatusRange(part))
	}
	return list
}

// Load reads all configuration from environment variables, applying defaults,
// and validates the resulting configuration.
func Load() (*Config, error) {
	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers:           getEnv("SOURCE_KAFKA_BROKERS", "localhost:9092"),
			Topic:             getEnv("SOURCE_KAFKA_TOPIC", ""),
			ConsumerGroupID:   getEnv("SOURCE_KAFKA_CONSUMER_GROUP_ID", ""),
			MaxPollRecords:    getEnvInt("SOURCE_KAFKA_MAX_POLL_RECORDS", 100),
			PollTimeoutMs:     getEnvInt("SOURCE_KAFKA_POLL_TIMEOUT_MS", 1000),
			MaxPollIntervalMs: getEnvInt("SOURCE_KAFKA_MAX_POLL_INTERVAL_MS", 300000),
			SessionTimeoutMs:  getEnvInt("SOURCE_KAFKA_SESSION_TIMEOUT_MS", 10000),
			AutoOffsetReset:   getEnv("SOURCE_KAFKA_AUTO_OFFSET_RESET", "latest"),
			CommitIntervalMs:  getEnvInt("SOURCE_KAFKA_COMMIT_INTERVAL_MS", 5000),
		},
		HTTP: HTTPConfig{
			ServiceURL:                       getEnv("SINK_HTTP_SERVICE_URL", ""),
			RequestMethod:                    getEnv("SINK_HTTP_REQUEST_METHOD", "POST"),
			RequestTimeoutMs:                 getEnvInt("SINK_HTTP_REQUEST_TIMEOUT_MS", 10000),
			MaxConnections:                   getEnvInt("SINK_HTTP_MAX_CONNECTIONS", 10),
			ConnectionTtlMs:                  getEnvInt("SINK_HTTP_CONNECTION_TTL_MS", 30000),
			ConnectionIdleEvictMs:            getEnvInt("SINK_HTTP_CONNECTION_IDLE_EVICT_MS", 30000),
			ConnectionValidateInactivityMs:   getEnvInt("SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS", 2000),
			Headers:                          nil,
			DataFormat:                       getEnv("SINK_HTTP_DATA_FORMAT", "json"),
			JSONBodyTemplate:                 getEnv("SINK_HTTP_JSON_BODY_TEMPLATE", ""),
			WorkerPoolSize:                   getEnvInt("SINK_WORKER_POOL_SIZE", 10),
			WorkerPoolBuffer:                 getEnvInt("SINK_WORKER_POOL_BUFFER", 10),
			ErrorRetryStatusCodes:            ParseStatusRangeList(getEnv("SINK_HTTP_ERROR_RETRY_STATUS_CODES", "500-599,429,408")),
			ErrorDLQStatusCodes:              ParseStatusRangeList(getEnv("SINK_HTTP_ERROR_DLQ_STATUS_CODES", "400-428,430-499,500-599")),
			ErrorFailStatusCodes:             ParseStatusRangeList(getEnv("SINK_HTTP_ERROR_FAIL_STATUS_CODES", "")),
			RetryMaxAttempts:                 getEnvInt("SINK_HTTP_RETRY_MAX_ATTEMPTS", 3),
			RetryBackoffInitialMs:            getEnvInt("SINK_HTTP_RETRY_BACKOFF_INITIAL_MS", 100),
			RetryBackoffMaxMs:                getEnvInt("SINK_HTTP_RETRY_BACKOFF_MAX_MS", 10000),
			RetryBackoffMultiplier:            getEnvFloat("SINK_HTTP_RETRY_BACKOFF_MULTIPLIER", 2.0),
			CircuitBreakerEnabled:            getEnvBool("SINK_HTTP_CIRCUIT_BREAKER_ENABLED", true),
			CircuitBreakerFailureThreshold:   getEnvInt("SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD", 80),
			CircuitBreakerWindowSize:         getEnvInt("SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE", 100),
			CircuitBreakerResetTimeoutMs:     getEnvInt("SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS", 10000),
			DLQEnabled:                       getEnvBool("SINK_HTTP_DLQ_ENABLED", true),
			DLQType:                          getEnv("SINK_HTTP_DLQ_TYPE", "kafka"),
			DLQKafkaTopic:                    getEnv("SINK_HTTP_DLQ_KAFKA_TOPIC", "goose-dlq"),
			DLQKafkaBrokers:                  getEnv("SINK_HTTP_DLQ_KAFKA_BROKERS", ""),
			OAuth2Enabled:                    getEnvBool("SINK_HTTP_OAUTH2_ENABLED", false),
			OAuth2AccessTokenURL:             getEnv("SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL", ""),
			OAuth2ClientName:                 getEnv("SINK_HTTP_OAUTH2_CLIENT_NAME", ""),
			OAuth2ClientSecret:               getEnv("SINK_HTTP_OAUTH2_CLIENT_SECRET", ""),
			OAuth2Scope:                      getEnv("SINK_HTTP_OAUTH2_SCOPE", ""),
			FilterEnabled:                    getEnvBool("SINK_HTTP_FILTER_ENABLED", false),
			FilterJSONPath:                   getEnv("SINK_HTTP_FILTER_JSONPATH", ""),
		},
		Schema: SchemaConfig{
			InputSchemaDataType:      getEnv("SCHEMA_INPUT_DATA_TYPE", "json"),
			SchemaRegistryEnabled:    getEnvBool("SCHEMA_REGISTRY_ENABLED", false),
			SchemaRegistryURL:        getEnv("SCHEMA_REGISTRY_URL", ""),
			SchemaRegistryProtoClass: getEnv("SCHEMA_REGISTRY_PROTO_CLASS", ""),
			RefreshStrategy:          getEnv("SCHEMA_REGISTRY_REFRESH_STRATEGY", "long_polling"),
			RefreshIntervalMs:        getEnvInt("SCHEMA_REGISTRY_REFRESH_INTERVAL_MS", 300000),
			FetchTimeoutMs:           getEnvInt("SCHEMA_REGISTRY_FETCH_TIMEOUT_MS", 10000),
			AuthBearerToken:          getEnv("SCHEMA_REGISTRY_AUTH_BEARER_TOKEN", ""),
		},
		Metrics: MetricsConfig{
			PrometheusEnabled: getEnvBool("METRICS_PROMETHEUS_ENABLED", true),
			PrometheusPort:    getEnvInt("METRICS_PROMETHEUS_PORT", 9090),
		},
		Tracing: TracingConfig{
			Enabled:      getEnvBool("TRACING_ENABLED", false),
			OTLPEndpoint: getEnv("TRACING_OTLP_ENDPOINT", ""),
			ServiceName:  getEnv("TRACING_SERVICE_NAME", "goose"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate ensures all required configuration fields are present.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Kafka.Brokers) == "" {
		return errors.New("SOURCE_KAFKA_BROKERS is required")
	}
	if strings.TrimSpace(c.Kafka.Topic) == "" {
		return errors.New("SOURCE_KAFKA_TOPIC is required")
	}
	if strings.TrimSpace(c.HTTP.ServiceURL) == "" {
		return errors.New("SINK_HTTP_SERVICE_URL is required")
	}
	return nil
}

// getEnv returns the value of the environment variable named by the key, or the
// provided fallback if the variable is empty or not present.
func getEnv(key, fallback string) string {
	if v, ok := lookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// getEnvInt returns the integer value of the environment variable named by the
// key, or the provided fallback if the variable is empty, not present, or not a
// valid integer.
func getEnvInt(key string, fallback int) int {
	if v, ok := lookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// getEnvFloat returns the float64 value of the environment variable named by
// the key, or the provided fallback if the variable is empty, not present, or
// not a valid float.
func getEnvFloat(key string, fallback float64) float64 {
	if v, ok := lookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

// getEnvBool returns the boolean value of the environment variable named by the
// key, or the provided fallback if the variable is empty, not present, or not a
// valid boolean.
func getEnvBool(key string, fallback bool) bool {
	if v, ok := lookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
