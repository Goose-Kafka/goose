# Goose Configuration Reference

All configuration is via environment variables. Goose validates required fields at startup and fails fast if anything is missing.

## Quick Reference

### Minimal config (JSON messages, no filtering, no DLQ)

```properties
SOURCE_KAFKA_BROKERS=kafka:9092
SOURCE_KAFKA_TOPIC=events
SOURCE_KAFKA_CONSUMER_GROUP_ID=goose
SINK_HTTP_SERVICE_URL=http://service:8080/api
INPUT_SCHEMA_DATA_TYPE=json
```

### Production config (with everything enabled)

```properties
# Kafka
SOURCE_KAFKA_BROKERS=kafka:9092
SOURCE_KAFKA_TOPIC=events
SOURCE_KAFKA_CONSUMER_GROUP_ID=goose-prod
SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS=500
SOURCE_KAFKA_COMMIT_INTERVAL_MS=5000

# Worker Pool
SINK_WORKER_POOL_SIZE=50
SINK_WORKER_POOL_BUFFER=50

# HTTP Sink
SINK_HTTP_SERVICE_URL=http://order-service:8080/events
SINK_HTTP_REQUEST_METHOD=POST
SINK_HTTP_REQUEST_TIMEOUT_MS=10000
SINK_HTTP_CONNECTION_TTL_MS=30000
SINK_HTTP_CONNECTION_IDLE_EVICT_MS=30000
SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS=2000
SINK_HTTP_HEADERS=Authorization:Bearer token123

# Error Handling
SINK_HTTP_ERROR_RETRY_STATUS_CODES=500-599,429,408
SINK_HTTP_ERROR_DLQ_STATUS_CODES=400-428,430-499,500-599
SINK_HTTP_RETRY_MAX_ATTEMPTS=3
SINK_HTTP_RETRY_BACKOFF_INITIAL_MS=100
SINK_HTTP_RETRY_BACKOFF_MAX_MS=10000
SINK_HTTP_RETRY_BACKOFF_MULTIPLIER=2.0

# Circuit Breaker
SINK_HTTP_CIRCUIT_BREAKER_ENABLED=true
SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD=80
SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE=100
SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS=10000

# DLQ
SINK_HTTP_DLQ_ENABLED=true
SINK_HTTP_DLQ_KAFKA_TOPIC=goose-dlq
SINK_HTTP_DLQ_KAFKA_BROKERS=kafka:9092

# Filtering (CEL)
SINK_HTTP_FILTER_ENABLED=true
SINK_HTTP_FILTER_JSONPATH=message.status == "created" && message.amount > 100

# Schema (Protobuf with Stencil)
INPUT_SCHEMA_DATA_TYPE=protobuf
SCHEMA_REGISTRY_ENABLED=true
SCHEMA_REGISTRY_URL=http://stencil:8080/descriptors/events.OrderEvent/latest
SCHEMA_REGISTRY_PROTO_CLASS=events.OrderEvent
SCHEMA_REGISTRY_REFRESH_STRATEGY=long_polling

# Observability
METRICS_PROMETHEUS_ENABLED=true
METRICS_PROMETHEUS_PORT=9090
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=goose
```

## Full Reference

### Kafka Consumer

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SOURCE_KAFKA_BROKERS` | `localhost:9092` | ✅ | Comma-separated Kafka brokers |
| `SOURCE_KAFKA_TOPIC` | _(empty)_ | ✅ | Kafka topic to consume |
| `SOURCE_KAFKA_CONSUMER_GROUP_ID` | _(empty)_ | ✅ | Consumer group ID |
| `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS` | `500` | | Max messages per poll |
| `SOURCE_KAFKA_POLL_TIMEOUT_MS` | `1000` | | Poll blocking timeout |
| `SOURCE_KAFKA_MAX_POLL_INTERVAL_MS` | `300000` | | Max time between polls (5 min) |
| `SOURCE_KAFKA_SESSION_TIMEOUT_MS` | `10000` | | Heartbeat timeout |
| `SOURCE_KAFKA_AUTO_OFFSET_RESET` | `latest` | | `latest` or `earliest` |
| `SOURCE_KAFKA_COMMIT_INTERVAL_MS` | `5000` | | Offset commit interval |

### Worker Pool

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_WORKER_POOL_SIZE` | `50` | | Number of worker goroutines |
| `SINK_WORKER_POOL_BUFFER` | `50` | | Batch channel buffer size |

> `SINK_HTTP_MAX_CONNECTIONS` automatically matches `SINK_WORKER_POOL_SIZE` at startup. You only need to set one.

### HTTP Sink

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_SERVICE_URL` | _(empty)_ | ✅ | HTTP endpoint URL |
| `SINK_HTTP_REQUEST_METHOD` | `POST` | | HTTP method |
| `SINK_HTTP_REQUEST_TIMEOUT_MS` | `10000` | | Call timeout (connect + read) |
| `SINK_HTTP_MAX_CONNECTIONS` | `50` | | Max pooled connections (auto-matched) |
| `SINK_HTTP_CONNECTION_TTL_MS` | `30000` | | Connection TTL |
| `SINK_HTTP_CONNECTION_IDLE_EVICT_MS` | `30000` | | Idle eviction timeout |
| `SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS` | `2000` | | Stale validation threshold |
| `SINK_HTTP_HEADERS` | _(empty)_ | | Comma-separated `Key:Value` headers |
| `SINK_HTTP_DATA_FORMAT` | `json` | | `json` or `protobuf` |
| `SINK_HTTP_JSON_BODY_TEMPLATE` | _(empty)_ | | Template → individual mode |

### Error Handling

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_ERROR_RETRY_STATUS_CODES` | `500-599,429,408` | | Status codes for retry |
| `SINK_HTTP_ERROR_DLQ_STATUS_CODES` | `400-428,430-499,500-599` | | Status codes for DLQ |
| `SINK_HTTP_ERROR_FAIL_STATUS_CODES` | _(empty)_ | | Status codes for crash |
| `SINK_HTTP_RETRY_MAX_ATTEMPTS` | `3` | | Max retry attempts |
| `SINK_HTTP_RETRY_BACKOFF_INITIAL_MS` | `100` | | Initial backoff |
| `SINK_HTTP_RETRY_BACKOFF_MAX_MS` | `10000` | | Max backoff |
| `SINK_HTTP_RETRY_BACKOFF_MULTIPLIER` | `2.0` | | Backoff multiplier |

### Circuit Breaker

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_CIRCUIT_BREAKER_ENABLED` | `true` | | Enable/disable |
| `SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `80` | | Failure rate % to open |
| `SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE` | `100` | | Sliding window size |
| `SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | `10000` | | Probe interval |

### DLQ

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_DLQ_ENABLED` | `true` | | Enable DLQ |
| `SINK_HTTP_DLQ_TYPE` | `kafka` | | DLQ type |
| `SINK_HTTP_DLQ_KAFKA_TOPIC` | `goose-dlq` | | DLQ topic |
| `SINK_HTTP_DLQ_KAFKA_BROKERS` | _(empty)_ | ✅ if DLQ enabled | DLQ brokers |

### Filtering

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_FILTER_ENABLED` | `false` | | Enable filtering |
| `SINK_HTTP_FILTER_JSONPATH` | _(empty)_ | | Filter expression |

### Schema

| Env Var | Default | Required | Description |
|---|---|---|---|
| `INPUT_SCHEMA_DATA_TYPE` | `json` | | `json` or `protobuf` |
| `SCHEMA_REGISTRY_ENABLED` | `false` | | Enable descriptor fetching |
| `SCHEMA_REGISTRY_URL` | _(empty)_ | ✅ if registry enabled | Registry URL |
| `SCHEMA_REGISTRY_PROTO_CLASS` | _(empty)_ | ✅ if registry enabled | Proto class name |
| `SCHEMA_REGISTRY_REFRESH_STRATEGY` | `long_polling` | | `long_polling`, `periodic`, `none` |
| `SCHEMA_REGISTRY_REFRESH_INTERVAL_MS` | `300000` | | Periodic refresh interval |
| `SCHEMA_REGISTRY_FETCH_TIMEOUT_MS` | `10000` | | Fetch timeout |
| `SCHEMA_REGISTRY_AUTH_BEARER_TOKEN` | _(empty)_ | | Bearer token |

### Batch-Poll Consumer

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS` | `500` | | Max messages per poll cycle |

> Goose uses `ReadMessage` (not `FetchMessage`) with a 100ms timeout for subsequent reads within a poll cycle. `FetchMessage` with `GroupID` blocks forever in segmentio/kafka-go — this is a known quirk. Messages are split into chunks of `WORKER_POOL_SIZE` and dispatched as separate batches.

**Ordered mode** (`POOL=1`, `POLL=1`): Strict per-message ordering, ~800 msg/s at 0ms delay. Use when messages are dependent (m10 needs m1 to succeed first).

**Multi-worker mode** (`POOL>1`): Parallel processing within chunks, ~6,000 msg/s at 0ms delay, ~4,000 msg/s at 50ms delay. Use when messages are independent.

### HTTP Batch Mode

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_BATCH_MODE` | `none` | | `none` (individual), `all_or_nothing`, `with_response` |
| `SINK_HTTP_BATCH_MAX_SIZE` | `500` | | Max messages per batch POST |
| `SINK_HTTP_BATCH_SEQ_FIELD` | `_goose_seq` | | Sequence field name |
| `SINK_HTTP_BATCH_RESPONSE_PATH` | `results` | | JSON path to results array |
| `SINK_HTTP_BATCH_RESPONSE_SEQ_FIELD` | `seq` | | Seq field in response |
| `SINK_HTTP_BATCH_RESPONSE_STATUS_FIELD` | `status` | | Status field in response |
| `SINK_HTTP_BATCH_RESPONSE_ERROR_FIELD` | `error` | | Error field in response |
| `SINK_HTTP_BATCH_RESPONSE_RETRY_FLAG` | `is_retryable` | | Retryable flag field |
| `SINK_HTTP_BATCH_RESPONSE_SUCCESS_VALUE` | `success` | | Value meaning success |

### Sink Type Selection

| `SINK_TYPE` | Sink | Key Env Vars |
|---|---|---|
| `http` (default) | HTTP REST | `SINK_HTTP_SERVICE_URL` |
| `grpc` | gRPC | `SINK_GRPC_SERVICE_URL`, `SINK_GRPC_METHOD` |
| `mongodb` | MongoDB | `SINK_MONGO_CONNECTION_URL`, `SINK_MONGO_DATABASE`, `SINK_MONGO_COLLECTION` |
| `postgresql` | PostgreSQL | `SINK_SQL_CONNECTION_URL`, `SINK_SQL_TABLE_NAME` |
| `redis` | Redis | `SINK_REDIS_ADDRESSES` |

See Mintlify docs for full gRPC, MongoDB, PostgreSQL, and Redis configuration.

### Observability

| Env Var | Default | Required | Description |
|---|---|---|---|
| `METRICS_PROMETHEUS_ENABLED` | `true` | | Enable Prometheus metrics |
| `METRICS_PROMETHEUS_PORT` | `9090` | | Metrics port |
| `OTEL_TRACING_ENABLED` | `false` | | Enable OTel traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty)_ | | OTLP gRPC endpoint |
| `OTEL_SERVICE_NAME` | `goose` | | OTel service name |

### OAuth2

| Env Var | Default | Required | Description |
|---|---|---|---|
| `SINK_HTTP_OAUTH2_ENABLED` | `false` | | Enable OAuth2 |
| `SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL` | _(empty)_ | | Token endpoint |
| `SINK_HTTP_OAUTH2_CLIENT_NAME` | _(empty)_ | | Client ID |
| `SINK_HTTP_OAUTH2_CLIENT_SECRET` | _(empty)_ | | Client secret |
| `SINK_HTTP_OAUTH2_SCOPE` | _(empty)_ | | OAuth2 scopes |

## Environment Variable Naming Convention

Variables follow the raystack firehose naming convention for drop-in compatibility:

- `SOURCE_KAFKA_*` — Kafka consumer settings
- `SINK_HTTP_*` — HTTP sink settings
- `SINK_WORKER_*` — Worker pool settings
- `SINK_HTTP_ERROR_*` — Error handling
- `SINK_HTTP_CIRCUIT_BREAKER_*` — Circuit breaker
- `SINK_HTTP_DLQ_*` — Dead letter queue
- `SINK_HTTP_FILTER_*` — Message filtering
- `INPUT_SCHEMA_*` — Input schema
- `SCHEMA_REGISTRY_*` — Schema registry
- `METRICS_*` — Metrics
- `OTEL_*` — OpenTelemetry tracing
- `SINK_HTTP_OAUTH2_*` — OAuth2 authentication

## Validation

Goose validates config at startup and fails fast:

1. **Required fields**: `SOURCE_KAFKA_BROKERS`, `SOURCE_KAFKA_TOPIC`, `SINK_HTTP_SERVICE_URL`, `SOURCE_KAFKA_CONSUMER_GROUP_ID`
2. **Positive values**: `SINK_WORKER_POOL_SIZE > 0`, `SINK_HTTP_MAX_CONNECTIONS > 0`
3. **Conditional requirements**: `SINK_HTTP_DLQ_KAFKA_BROKERS` required when DLQ enabled
4. **Auto-match**: `SINK_HTTP_MAX_CONNECTIONS` set to `SINK_WORKER_POOL_SIZE` if different
5. **Schema**: `SCHEMA_REGISTRY_URL` and `SCHEMA_REGISTRY_PROTO_CLASS` required when registry enabled
