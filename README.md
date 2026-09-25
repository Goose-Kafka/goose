# Goose

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
![Docker](https://img.shields.io/badge/Docker-distroless%20~26MB-2496ED?logo=docker)

> _"Talk to me, Goose."_

Goose is a lightweight, cloud-native Kafka-to-HTTP firehose inspired by **Goose** from _Top Gun_ — the trusted RIO (Radar Intercept Officer) who feeds critical data to Maverick in real-time, never misses a callout, and always has his pilot's back.

Goose does the same for your services: it consumes messages from Kafka and delivers them to HTTP endpoints — fast, reliably, and with full observability.

## Key Features

- **Sinks:** HTTP REST (POST/PUT) with batch or individual mode
- **Scale:** Horizontally scalable — one pod per Kafka partition, auto-scales with HPA
- **Extensibility:** Pluggable Sink interface (add gRPC, Redis, etc. without changing core)
- **Runtime:** Runs in containers, VMs, or Kubernetes — single ~26MB binary
- **Metrics:** Native Prometheus metrics (14 metrics) + OpenTelemetry traces
- **Logs:** Loki-ready structured logging via Promtail
- **Schema:** JSON passthrough + Protobuf→JSON conversion via Stencil/schema registry
- **Filters:** JSONPath (simple) + CEL expressions (complex, JEXL-equivalent)
- **Error Handling:** Config-driven retry → DLQ → circuit breaker
- **Connection Management:** TTL + idle eviction + stale validation (fixes connection pinning)

## Goose vs Raystack Firehose

| Metric | Raystack (Java) | Goose (Go) |
|---|---|---|
| Image size | 786 MB | ~26 MB |
| RAM at idle | ~200–300 MB | ~30 MB |
| CPU at 2,500/s | ~2–3 vCPU | ~0.6 vCPU |
| Startup time | 5–10s | <1s |
| Dependencies | 50+ | 6 |
| Connection pinning | ❌ Yes | ✅ Fixed (TTL) |
| Spin-loop backpressure | ❌ Yes | ✅ No (channels) |
| CEL/JEXL filtering | JEXL (Java only) | CEL (Google standard) |
| Offset commits | ✅ | ✅ |
| Circuit breaker | ❌ | ✅ |
| Integration tests | ❌ | ✅ (12 tests) |

## Quick Start

### Run with Docker

```bash
docker run -e SOURCE_KAFKA_BROKERS=kafka:9092 \
           -e SOURCE_KAFKA_TOPIC=events \
           -e SOURCE_KAFKA_CONSUMER_GROUP_ID=goose \
           -e SINK_HTTP_SERVICE_URL=http://service:8080/api \
           -e INPUT_SCHEMA_DATA_TYPE=json \
           goose:latest
```

### Run with Kubernetes (Helm)

```bash
helm install goose ./helm \
  --set env.SOURCE_KAFKA_BROKERS=kafka:9092 \
  --set env.SOURCE_KAFKA_TOPIC=events \
  --set env.SINK_HTTP_SERVICE_URL=http://service:8080/api
```

### Run Locally

```sh
# Clone the repo
git clone https://github.com/ArelliGoutham/goose.git
cd goose

# Build the binary
go build -o bin/goose ./cmd/goose

# Configure env vars and run
export SOURCE_KAFKA_BROKERS=localhost:9092
export SOURCE_KAFKA_TOPIC=events
export SOURCE_KAFKA_CONSUMER_GROUP_ID=goose
export SINK_HTTP_SERVICE_URL=http://localhost:8080/api
export INPUT_SCHEMA_DATA_TYPE=json
./bin/goose
```

## Architecture

```
Kafka Topic
    │
    ▼
┌───────────────────────────────────────────────────────────┐
│  Consumer Goroutine (1)                                   │
│  • Polls Kafka (up to MAX_POLL_RECORDS per poll)          │
│  • Schema deserialize (protobuf→JSON or JSON passthrough)  │
│  • Apply filter (JSONPath or CEL expression)              │
│  • Create Batch with UUID                                 │
│  • Register offsets in OffsetManager                       │
│  • batchChan <- batch  ← BLOCKS if full (backpressure)    │
└─────────────────────┬─────────────────────────────────────┘
                      │  chan *Batch (buffered)
                      ▼
┌───────────────────────────────────────────────────────────┐
│  Worker Pool (N goroutines)                               │
│  • Worker reads <- batchChan                              │
│  • Check circuit breaker                                  │
│  • HTTP POST to sink (batch or individual mode)           │
│  • On failure: retry → DLQ → ignore (config-driven)      │
│  • Signal batch done → OffsetManager                      │
└─────────────────────┬─────────────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────────────┐
│  Offset Manager                                           │
│  • Contiguity-gated commit (same logic as raystack)       │
│  • Commit to Kafka every COMMIT_INTERVAL_MS              │
│  • Prunes committed offsets (no memory leak)              │
└───────────────────────────────────────────────────────────┘
```

### Key design decisions

1. **Channel-based backpressure** — `batchChan <- batch` blocks the consumer when workers are busy. No spin loops, no CPU waste.
2. **Worker pool model** — N goroutines, each making HTTP calls. N is config-driven, can exceed partition count.
3. **Contiguity-gated offsets** — offset N is committed only when all messages up to N are processed (at-least-once delivery).
4. **Connection TTL** — connections are closed after TTL and re-opened, redistributing traffic across backend pods (fixes raystack's connection pinning bug).
5. **Auto-matched pool size** — `SINK_HTTP_MAX_CONNECTIONS` auto-matches `SINK_WORKER_POOL_SIZE` to prevent port exhaustion.

## Configuration

All configuration is via environment variables. Sane defaults are provided — only `SOURCE_KAFKA_BROKERS`, `SOURCE_KAFKA_TOPIC`, and `SINK_HTTP_SERVICE_URL` are required.

### Kafka Consumer

| Env Var | Default | Description |
|---|---|---|
| `SOURCE_KAFKA_BROKERS` | _(required)_ | Comma-separated Kafka broker addresses |
| `SOURCE_KAFKA_TOPIC` | _(required)_ | Kafka topic to consume |
| `SOURCE_KAFKA_CONSUMER_GROUP_ID` | _(required)_ | Consumer group ID |
| `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS` | `500` | Max messages returned per `poll()` |
| `SOURCE_KAFKA_POLL_TIMEOUT_MS` | `1000` | How long `poll()` blocks for data |
| `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_INTERVAL_MS` | `300000` | Max time between polls before Kafka evicts consumer (5 min) |
| `SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS` | `10000` | Heartbeat timeout |
| `SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET` | `latest` | `latest` or `earliest` |
| `SOURCE_KAFKA_COMMIT_INTERVAL_MS` | `5000` | How often to commit offsets to Kafka |

### Worker Pool

| Env Var | Default | Description |
|---|---|---|
| `SINK_WORKER_POOL_SIZE` | `50` | Number of worker goroutines (concurrent HTTP calls) |
| `SINK_WORKER_POOL_BUFFER` | `50` | Channel buffer size (batches queued while workers busy) |

> **Important:** `SINK_HTTP_MAX_CONNECTIONS` auto-matches `SINK_WORKER_POOL_SIZE` at startup to prevent port exhaustion. You only need to set `SINK_WORKER_POOL_SIZE`.

### HTTP Sink

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_SERVICE_URL` | _(required)_ | HTTP endpoint URL |
| `SINK_HTTP_REQUEST_METHOD` | `POST` | HTTP method (`POST` or `PUT`) |
| `SINK_HTTP_REQUEST_TIMEOUT_MS` | `10000` | HTTP call timeout (connect + read) |
| `SINK_HTTP_MAX_CONNECTIONS` | `50` | Max pooled connections (auto-matched to worker pool) |
| `SINK_HTTP_CONNECTION_TTL_MS` | `30000` | Connection max age — closed & reopened after TTL |
| `SINK_HTTP_CONNECTION_IDLE_EVICT_MS` | `30000` | Idle connection eviction timeout |
| `SINK_HTTP_CONNECTION_VALIDATE_INACTIVITY_MS` | `2000` | Validate stale connections before reuse |
| `SINK_HTTP_HEADERS` | _(empty)_ | Comma-separated `Key:Value` header pairs |
| `SINK_HTTP_DATA_FORMAT` | `json` | `json` or `protobuf` (sets Content-Type header) |
| `SINK_HTTP_JSON_BODY_TEMPLATE` | _(empty)_ | If set → individual mode (one POST per message). If empty → batch mode (all messages in one POST) |

### Batch Mode vs Individual Mode

| Mode | When | Behavior |
|---|---|---|
| **Batch** | `SINK_HTTP_JSON_BODY_TEMPLATE` is empty | All messages in a batch are serialized into one JSON array and sent as a single POST |
| **Individual** | `SINK_HTTP_JSON_BODY_TEMPLATE` is set | One HTTP POST per message (for endpoints that handle one message at a time) |

### Error Handling

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_ERROR_RETRY_STATUS_CODES` | `500-599,429,408` | HTTP status codes that trigger retry |
| `SINK_HTTP_ERROR_DLQ_STATUS_CODES` | `400-428,430-499,500-599` | HTTP status codes that go to DLQ |
| `SINK_HTTP_ERROR_FAIL_STATUS_CODES` | _(empty)_ | HTTP status codes that crash consumer (use sparingly — prefer circuit breaker) |
| `SINK_HTTP_RETRY_MAX_ATTEMPTS` | `3` | Max retry attempts |
| `SINK_HTTP_RETRY_BACKOFF_INITIAL_MS` | `100` | Initial backoff delay |
| `SINK_HTTP_RETRY_BACKOFF_MAX_MS` | `10000` | Max backoff delay cap |
| `SINK_HTTP_RETRY_BACKOFF_MULTIPLIER` | `2.0` | Exponential backoff multiplier |

#### Error routing decision tree

```
HTTP response received
    │
    ├── 2xx (200-299) → SUCCESS (offset committable)
    │
    ├── In RETRY range? → retry with exponential backoff (up to MAX_ATTEMPTS)
    │       └── retry exhausts? → In DLQ range? → DLQ : IGNORE
    │
    ├── In DLQ range (not retry)? → DLQ immediately
    │
    ├── In FAIL range? → crash consumer (ONLY for infrastructure failures)
    │
    └── Not in any range? → IGNORE (message dropped, offset committable)
```

#### When to crash vs not crash

| Scenario | Crash? | Action |
|---|---|---|
| HTTP sink returns 503 | No | Retry → DLQ → circuit breaker |
| HTTP sink returns 404 | No | DLQ immediately |
| Can't connect to Kafka | Yes | K8s restart → healthy broker |
| Can't write to DLQ | Yes | Data loss risk → restart |
| Failure rate > 80% sustained | No | Circuit breaker (pause + alert) |

### Circuit Breaker

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_CIRCUIT_BREAKER_ENABLED` | `true` | Enable/disable circuit breaker |
| `SINK_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `80` | Failure rate % to open circuit (0–100) |
| `SINK_HTTP_CIRCUIT_BREAKER_WINDOW_SIZE` | `100` | Sliding window size (last N requests) |
| `SINK_HTTP_CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | `10000` | Time to wait before probing (half-open) |

### DLQ (Dead Letter Queue)

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_DLQ_ENABLED` | `true` | Enable DLQ |
| `SINK_HTTP_DLQ_TYPE` | `kafka` | DLQ type (`kafka` only for v1) |
| `SINK_HTTP_DLQ_KAFKA_TOPIC` | `goose-dlq` | DLQ Kafka topic |
| `SINK_HTTP_DLQ_KAFKA_BROKERS` | _(required if DLQ enabled)_ | DLQ Kafka brokers |

DLQ messages include metadata headers:
- `original_topic` — original Kafka topic
- `original_partition` — original partition
- `original_offset` — original offset
- `error_type` — error classification (SINK_4XX_ERROR, SINK_5XX_ERROR, etc.)
- `error_status_code` — HTTP status code that caused the failure
- `error_message` — error description

### Filtering

Goose supports two filter engines, automatically selected based on the expression format:

- **JSONPath** — expressions starting with `$` (e.g., `$.order.status:created`)
- **CEL** — any other expression (e.g., `message.order.status == "created" && message.order.amount > 100`)

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_FILTER_ENABLED` | `false` | Enable message filtering |
| `SINK_HTTP_FILTER_JSONPATH` | _(empty)_ | Filter expression (JSONPath or CEL — auto-detected) |

#### JSONPath examples (simple field match)

```properties
# Only forward messages where $.status equals "created"
SINK_HTTP_FILTER_ENABLED=true
SINK_HTTP_FILTER_JSONPATH=$.status:created

# Nested path with match value
SINK_HTTP_FILTER_JSONPATH=$.order.status:pending
```

#### CEL examples (complex expressions, like JEXL)

```properties
# AND condition
SINK_HTTP_FILTER_JSONPATH=message.status == "created" && message.amount > 100

# OR + NOT
SINK_HTTP_FILTER_JSONPATH=message.status != "cancelled" && (message.priority == "high" || message.priority == "critical")

# IN operator
SINK_HTTP_FILTER_JSONPATH=message.city in ["Mumbai", "Delhi", "Bangalore"]

# Field presence (has macro)
SINK_HTTP_FILTER_JSONPATH=has(message.customer_id)

# String methods
SINK_HTTP_FILTER_JSONPATH=message.email.contains("@company.com") && message.name.startsWith("user-")

# Nested fields
SINK_HTTP_FILTER_JSONPATH=message.order.status == "created" && message.order.payment.amount > 500

# Collection size
SINK_HTTP_FILTER_JSONPATH=message.items.size() > 0
```

### Schema

| Env Var | Default | Description |
|---|---|---|
| `INPUT_SCHEMA_DATA_TYPE` | `json` | `json` or `protobuf` |
| `SCHEMA_REGISTRY_ENABLED` | `false` | Enable proto descriptor fetching |
| `SCHEMA_REGISTRY_URL` | _(empty)_ | Schema registry URL (e.g., Stencil) |
| `SCHEMA_REGISTRY_PROTO_CLASS` | _(empty)_ | Proto class name (e.g., `events.OrderEvent`) |
| `SCHEMA_REGISTRY_REFRESH_STRATEGY` | `long_polling` | `long_polling`, `periodic`, or `none` |
| `SCHEMA_REGISTRY_REFRESH_INTERVAL_MS` | `300000` | Refresh interval for periodic strategy (5 min) |
| `SCHEMA_REGISTRY_FETCH_TIMEOUT_MS` | `10000` | Descriptor fetch timeout |
| `SCHEMA_REGISTRY_AUTH_BEARER_TOKEN` | _(empty)_ | Optional bearer token for registry auth |

#### Schema conversion matrix

| `INPUT_SCHEMA_DATA_TYPE` | `SINK_HTTP_DATA_FORMAT` | What goose does |
|---|---|---|
| `json` | `json` | Passthrough (raw JSON bytes sent to sink) |
| `protobuf` | `json` | Fetch descriptor → parse proto → convert to JSON → send JSON |
| `protobuf` | `protobuf` | Passthrough (raw proto bytes sent with `Content-Type: application/x-protobuf`) |

#### Protobuf workflow

```
Startup:
  1. Fetch FileDescriptorSet from SCHEMA_REGISTRY_URL
  2. Parse descriptor, find SCHEMA_REGISTRY_PROTO_CLASS
  3. Build DynamicMessage parser
  4. Start background refresh goroutine (long-polling or periodic)

On each Kafka message:
  5. proto.Unmarshal(rawBytes) → DynamicMessage
  6. protojson.Marshal(DynamicMessage) → JSON bytes
  7. Send JSON to HTTP sink

Schema update (zero downtime):
  8. Background goroutine detects new descriptor
  9. Atomic parser swap (no restart needed)
  10. Log + metric: firehose_schema_updates_total
```

### Observability

| Env Var | Default | Description |
|---|---|---|
| `METRICS_PROMETHEUS_ENABLED` | `true` | Enable Prometheus `/metrics` endpoint |
| `METRICS_PROMETHEUS_PORT` | `9090` | Metrics server port |
| `OTEL_TRACING_ENABLED` | `false` | Enable OpenTelemetry traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(empty)_ | OTLP gRPC endpoint (Jaeger/Tempo) |
| `OTEL_SERVICE_NAME` | `goose` | OTel service name |

#### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `firehose_messages_consumed_total` | counter | topic, partition | Messages consumed from Kafka |
| `firehose_messages_filtered_total` | counter | topic | Messages dropped by filter |
| `firehose_messages_delivered_total` | counter | sink | Messages delivered to sink |
| `firehose_messages_retried_total` | counter | error_type | Messages that entered retry |
| `firehose_messages_dlq_total` | counter | error_type | Messages sent to DLQ |
| `firehose_messages_ignored_total` | counter | error_type | Messages dropped (no DLQ) |
| `firehose_sink_latency_seconds` | histogram | sink | HTTP call latency (p50/p95/p99) |
| `firehose_consumer_lag_messages` | gauge | topic, partition | Kafka consumer lag |
| `firehose_worker_pool_queue_depth` | gauge | — | Current batch channel buffer depth |
| `firehose_worker_pool_size` | gauge | — | Configured worker pool size |
| `firehose_circuit_breaker_open` | gauge | sink | Circuit breaker state (1=open, 0=closed) |
| `firehose_http_response_code_total` | counter | status_code | HTTP response code distribution |
| `firehose_offset_commit_total` | counter | topic, partition | Offset commits to Kafka |
| `firehose_schema_updates_total` | counter | proto_class | Schema registry update count |

### OAuth2 (optional)

| Env Var | Default | Description |
|---|---|---|
| `SINK_HTTP_OAUTH2_ENABLED` | `false` | Enable OAuth2 authentication |
| `SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL` | _(empty)_ | OAuth2 token endpoint |
| `SINK_HTTP_OAUTH2_CLIENT_NAME` | _(empty)_ | Client ID |
| `SINK_HTTP_OAUTH2_CLIENT_SECRET` | _(empty)_ | Client secret |
| `SINK_HTTP_OAUTH2_SCOPE` | _(empty)_ | Space-delimited OAuth2 scopes |

## Scaling Guide

### Formula

```
Max concurrent HTTP requests = goose_pods × workers_per_pod
                                (capped by Kafka partitions)

Max throughput = concurrent_requests × (1000 / HTTP_latency_ms)

Example:
  4 pods × 50 workers × (1000 / 100ms) = 2,000 msg/s
```

### Production defaults

```properties
SINK_WORKER_POOL_SIZE=50          # 50 workers per pod
SINK_WORKER_POOL_BUFFER=50        # 50 batch buffer
SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS=500
```

### K8s resource limits (per pod)

```yaml
resources:
  requests:
    cpu: 200m
    memory: 64Mi
  limits:
    cpu: 500m      # 50 workers with 100ms latency uses ~500m
    memory: 128Mi
```

### Scaling checklist

- [ ] Set Kafka partitions ≥ desired max pods (can't exceed partitions)
- [ ] Set `SINK_WORKER_POOL_SIZE` to match CPU limits (50 workers ≈ 500m CPU)
- [ ] Enable HPA with `targetCPUUtilizationPercentage: 80`
- [ ] Ensure `SINK_HTTP_MAX_CONNECTIONS` = `SINK_WORKER_POOL_SIZE` (auto-matched)
- [ ] Set `max.poll.interval.ms` high enough: `MAX_POLL_RECORDS × HTTP_latency < max.poll.interval`

## Project Structure

```
goose/
├── cmd/goose/main.go              ← Entry point — wires all components
├── internal/
│   ├── config/                    ← Env-var configuration + validation
│   ├── consumer/                  ← Kafka consumer goroutine
│   ├── worker/                    ← Worker pool goroutines (N workers)
│   ├── offset/offsetmanager/      ← Contiguity-gated offset commit
│   ├── filter/                    ← JSONPath + CEL filtering
│   ├── sink/                      ← HTTP sink (batch/individual, connection TTL)
│   ├── error/                     ← Error types, retry, circuit breaker, DLQ, error routing
│   ├── metrics/                   ← Prometheus metrics (14 metrics)
│   ├── tracing/                   ← OpenTelemetry tracing (OTLP)
│   └── schema/                    ← Schema manager (JSON passthrough + protobuf→JSON)
├── helm/                          ← Helm chart (Deployment, Service, ConfigMap)
├── docs/                          ← Documentation + future tasks
├── Dockerfile                     ← Multi-stage build (distroless, ~26MB)
├── Makefile                       ← build, test, run, lint, docker targets
└── go.mod                         ← 6 dependencies
```

## Testing

```sh
# Unit tests (with race detector)
go test -v -race ./...

# Integration tests (requires Docker — starts Kafka + MockServer)
cd ../goose-integration-test
docker compose up -d
go test -v -timeout 300s ./...
docker compose down
```

### Integration test coverage

| Test | Duration | What it verifies |
|---|---|---|
| TestE2E_EndToEndDelivery | ~5s | 5 JSON messages → Kafka → goose → HTTP sink |
| TestRetryOn5xx | ~5s | 503 twice then 200 — retry behavior confirmed |
| TestDLQOn4xx | ~33s | 404 always — message sent to DLQ Kafka topic |
| TestCircuitBreaker | ~23s | Sustained 503s — circuit opens, goose survives |
| TestCELFilter_E2E | ~5s | CEL: `status == "created" && amount > 100` |
| TestCELFilter_InOperator | ~5s | CEL: `city in ["Mumbai", "Delhi"]` |
| TestCELFilter_HasMacro | ~5s | CEL: `has(customer_id)` |
| TestCELFilter_NestedFields | ~5s | CEL: nested JSON `order.status == "created"` |
| TestCELFilter_StringMethods | ~5s | CEL: `email.contains() && name.startsWith()` |
| TestCELFilter_NotEqualAndOr | ~5s | CEL: `!= "cancelled" && (high \|\| critical)` |
| TestCELFilter_InvalidExpression | ~5s | Invalid CEL → NoOp fallback |
| TestCELFilter_LargeDataset | ~5s | 100 messages with complex filter |

## Roadmap

See [docs/future-tasks.md](docs/future-tasks.md) for planned features:

- ✅ Proto→JSON conversion (via Stencil schema registry)
- ✅ Schema registry long-polling refresh
- ✅ CEL filtering (JEXL-equivalent)
- 🔲 gRPC sink support
- 🔲 Batch-poll consumer (500x throughput boost)
- 🔲 Avro support (Confluent Schema Registry)
- 🔲 Schema validation
- 🔲 HPA in Helm chart
- 🔲 OTel spans in worker/consumer
- 🔲 Consumer lag metrics from Kafka

## Credits

Built as a replacement for [raystack/firehose](https://github.com/raystack/firehose) — inspired by its architecture, rewritten in Go for performance and simplicity.

## License

Apache 2.0
