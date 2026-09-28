# Goose Future Tasks & Architecture Analysis

**Created:** 2024-09-24
**Last Updated:** 2026-09-28
**Status:** v1.0.0 released — batch-poll consumer load tested

---

## Task 1: Proto→JSON Conversion for HTTP Sink

**Status:** ✅ Complete
**Completed:** 2024-09-24
**Priority:** High — most common use case
**Effort:** Medium

### Problem

When `INPUT_SCHEMA_DATA_TYPE=protobuf`, goose currently passes raw protobuf bytes to the HTTP sink unchanged. Most REST endpoints expect JSON, not raw protobuf binary.

### Scenario

```
Kafka: [08 41 12 05 63 72 65 61 74 65 64]  (raw protobuf bytes)
Goose: fetches descriptor from Stencil → parses proto → converts to JSON
HTTP:  POST /api  Content-Type: application/json  Body: {"id":"A","status":"created"}
```

### Implementation

1. Add `SINK_HTTP_DATA_FORMAT` config (values: `json` or `protobuf`, default: `json`)
2. On startup with `INPUT_SCHEMA_DATA_TYPE=protobuf` + `SINK_HTTP_DATA_FORMAT=json`:
   - Call `RegistryClient.Fetch(URL)` to get `FileDescriptorSet`
   - Parse descriptor, find `SCHEMA_REGISTRY_PROTO_CLASS` message type
   - Build `DynamicMessage` parser using `google.golang.org/protobuf`
3. On each Kafka message:
   - `DynamicMessage msg = parser.Parse(rawBytes)`
   - `jsonBytes = protojson.Marshal(msg)`
   - Send `jsonBytes` to HTTP sink with `Content-Type: application/json`
4. Set `Content-Type` header based on `SINK_HTTP_DATA_FORMAT`:
   - `json` → `application/json`
   - `protobuf` → `application/x-protobuf`

### Config

```properties
INPUT_SCHEMA_DATA_TYPE=protobuf
SINK_HTTP_DATA_FORMAT=json
SCHEMA_REGISTRY_ENABLED=true
SCHEMA_REGISTRY_URL=http://stencil:8080/descriptors/events.OrderEvent/latest
SCHEMA_REGISTRY_PROTO_CLASS=events.OrderEvent
```

### Conversion Matrix

| INPUT_SCHEMA_DATA_TYPE | SINK_HTTP_DATA_FORMAT | What goose does |
|---|---|---|
| json | json | Passthrough (works now) |
| protobuf | json | Proto→JSON conversion (THIS TASK) |
| protobuf | protobuf | Passthrough (works now — raw bytes) |

### Dependencies

- `google.golang.org/protobuf` (already in go.mod)
- `google.golang.org/protobuf/encoding/protojson` (for JSON conversion)
- `google.golang.org/protobuf/types/descriptorpb` (for FileDescriptorSet)
- `google.golang.org/protobuf/reflect/protodesc` (for building DynamicMessage type)

---

## Task 2: Schema Registry Long-Polling Refresh

**Status:** ✅ Complete
**Completed:** 2024-09-24
**Priority:** High — required for production proto support
**Effort:** Medium

### Problem

When a Protobuf schema changes (new field added), goose must pick up the new descriptor without restarting. Current code has no background refresh goroutine.

### Implementation

1. Background goroutine that long-polls the schema registry URL
2. When a new descriptor is returned:
   - Build new `DynamicMessage` parser from new descriptor
   - Atomic swap: `atomic.StorePointer(&sm.parser, newParser)`
   - Log: `"Schema updated: events.OrderEvent v2"`
   - Increment `firehose_schema_updates_total` metric
3. Consumer and workers always read the current parser atomically:
   ```go
   func (sm *ProtobufSchemaManager) Parse(rawBytes []byte) ([]byte, error) {
       parser := atomic.LoadPointer(&sm.currentParser)
       return parser.Parse(rawBytes)
   }
   ```
4. Zero downtime — no restart needed when proto changes

### Config

```properties
SCHEMA_REGISTRY_REFRESH_STRATEGY=long_polling   # long_polling | periodic | none
SCHEMA_REGISTRY_REFRESH_INTERVAL_MS=300000      # for periodic (5 min)
```

### Stencil Long-Polling Protocol

- Client sends HTTP GET to registry URL with a long timeout (e.g., 60s)
- Server holds the connection until either:
  - A new version is available → returns new descriptor → client swaps parser
  - Timeout expires → client reconnects and polls again
- This is the same protocol raystack/Stencil uses

---

## Task 3: gRPC Sink Support

**Status:** ✅ Complete
**Completed:** 2026-09-25
**Priority:** Medium — needed if downstream services are gRPC
**Effort:** Medium-High

### Problem

Goose currently only supports HTTP REST sinks. If downstream services use gRPC, a new `GrpcSink` implementation is needed.

### Scenario

```
Goose → gRPC call to service:9090
        Method: events.OrderService/ProcessOrder
        Message: OrderEvent{id:"123", status:"created"}  (raw protobuf bytes)
        No Stencil needed — gRPC uses protobuf natively
```

### Implementation

1. New `GrpcSink` struct implementing the `Sink` interface:
   ```go
   type GrpcSink struct {
       conn     *grpc.ClientConn
       method   string
       protoClass string
       // ...
   }
   ```
2. Create `grpc.ClientConn` with:
   - Target address (`service:9090`)
   - Connection pooling (gRPC manages this automatically)
   - TLS optional
   - Interceptors for auth (bearer token, OAuth2)
3. `Push()` method:
   - For each message: create proto message from raw bytes
   - Call gRPC method via `conn.Invoke(ctx, method, request, response)`
   - Check gRPC status code
   - Route errors based on gRPC status codes
4. Error routing — map gRPC codes to retry/DLQ/ignore:
   | gRPC Code | Action |
   |---|---|
   | `OK` | Success |
   | `UNAVAILABLE` | Retry (transient) |
   | `DEADLINE_EXCEEDED` | Retry (transient) |
   | `INVALID_ARGUMENT` | DLQ (permanent) |
   | `NOT_FOUND` | DLQ (permanent) |
   | `PERMISSION_DENIED` | Fail (crash — auth issue) |

### Config

```properties
SINK_TYPE=grpc
SINK_GRPC_SERVICE_URL=service:9090
SINK_GRPC_METHOD=events.OrderService/ProcessOrder
SINK_GRPC_PROTO_CLASS=events.OrderEvent
SINK_GRPC_TLS_ENABLED=false
SINK_GRPC_TLS_CA_CERT=/path/to/ca.pem
SINK_GRPC_AUTH_TYPE=bearer           # bearer | oauth2 | none
SINK_GRPC_AUTH_TOKEN=optional_token
SINK_GRPC_ERROR_RETRY_CODES=UNAVAILABLE,DEADLINE_EXCEEDED
SINK_GRPC_ERROR_DLQ_CODES=INVALID_ARGUMENT,NOT_FOUND
SINK_GRPC_ERROR_FAIL_CODES=PERMISSION_DENIED
SINK_GRPC_TIMEOUT_MS=10000
```

### Why gRPC is simpler than HTTP for protobuf

| Aspect | HTTP + Protobuf | gRPC + Protobuf |
|---|---|---|
| Descriptor needed? | Yes (to convert proto→JSON for REST) | **No** (gRPC uses proto natively) |
| Stencil/schema registry? | Yes (for proto→JSON) | **No** (raw bytes are the proto message) |
| Conversion? | Proto→JSON | **None** (passthrough) |
| Connection pooling | Manual (http.Transport) | Automatic (grpc.ClientConn) |
| Status codes | HTTP 200/400/500 | gRPC status codes |
| Streaming | No | Yes (gRPC streaming built-in) |

### Dependencies

- `google.golang.org/grpc` (gRPC client library)
- `google.golang.org/protobuf` (for proto message creation from raw bytes)

### Sink Interface (already pluggable)

```go
// In main.go:
switch cfg.SinkType {
case "http":
    sink = sink.NewHTTPSink(...)
case "grpc":
    sink = sink.NewGrpcSink(...)
}
```

---

## Task 4: Avro Support (Confluent Schema Registry)

**Status:** 🔲 Not started
**Priority:** Low — only if company uses Avro
**Effort:** High

### Problem

If Kafka producers use Avro serialization (with Confluent Schema Registry), goose needs to:
1. Extract schema ID from the Confluent wire format (first 5 bytes)
2. Fetch Avro schema from registry by ID
3. Decode Avro bytes using the schema
4. Convert to JSON for HTTP sink

### Confluent Wire Format

```
Kafka message: [magic byte (0x00)] [4-byte schema ID] [Avro payload]
                \/                \/                    \/
                1 byte            4 bytes              variable
```

### Config

```properties
INPUT_SCHEMA_DATA_TYPE=avro
SCHEMA_REGISTRY_TYPE=confluent
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

### Implementation

1. New `AvroSchemaManager` implementing `SchemaManager` interface
2. Extract schema ID from first 5 bytes
3. Cache schemas by ID (fetch from `GET /schemas/ids/{id}`)
4. Decode Avro using `github.com/linkedin/goavro2` or `github.com/hamba/avro`
5. Convert decoded Avro record to JSON

### Dependencies

- `github.com/hamba/avro` or `github.com/linkedin/goavro2` (Avro library)

---

## Task 5: Schema Validation

**Status:** ✅ Complete
**Completed:** 2024-09-25
**Priority:** Low — data quality enforcement
**Effort:** Medium

### Problem

Goose currently doesn't validate messages against their schema. Invalid messages (missing required fields, unknown fields from schema evolution) are passed through.

### Implementation

1. After parsing a message with the schema, validate:
   - Required fields are present
   - Field types match schema
   - Unknown fields detection (proto: `proto.MarshalOptions.DiscardUnknown`)
2. Config: `SCHEMA_VALIDATION_ENABLED=true`
3. On validation failure:
   - Log error with details
   - Route to DLQ (not retry — validation failures are permanent)
   - Increment `firehose_messages_validation_failed_total` metric

### Config

```properties
SCHEMA_VALIDATION_ENABLED=false
SCHEMA_VALIDATION_ON_FAILURE=dlq           # dlq | ignore | drop
```

---

## Task 6: Batch-Poll Consumer (Throughput Optimization)

**Status:** ✅ Complete
**Completed:** 2026-09-28
**Priority:** Medium — major throughput improvement
**Effort:** Medium

### Problem

Goose's consumer goroutine reads **one message at a time** using `reader.ReadMessage(ctx)`. Each message becomes a batch of 1. This means one HTTP call per message — high connection churn and limited by single-goroutine read rate.

### Solution

Instead of reading 1 message at a time, read up to `MAX_POLL_RECORDS` messages at once, then send the entire batch to one worker:

```
Current:
  ReadMessage → batch{1 msg} → worker → 1 HTTP call per message
  500 messages = 500 HTTP calls = 500 TCP connections

Batch-poll:
  ReadMessages(500) → batch{500 msgs} → worker → 1 HTTP call (batch mode)
  500 messages = 1 HTTP call = 1 TCP connection
  → 500x reduction in connection churn
  → eliminates port exhaustion
  → matches raystack firehose behavior
```

### Implementation

1. Replace `reader.ReadMessage(ctx)` with a loop calling `reader.FetchMessage(ctx)` to fill a batch up to `MAX_POLL_RECORDS`
2. Create one `Batch` with all fetched messages
3. Send the batch to `batchChan` (one entry, not 500)
4. Workers process the batch:
   - **Batch mode** (no template): all 500 messages in one POST body as JSON array
   - **Individual mode** (template): 500 separate POSTs (same as now, but consumer reads faster)

### Caveat

Batch mode requires the HTTP endpoint to accept JSON arrays. If the endpoint handles one message per request, individual mode is still needed — but the consumer reading 500 at a time still helps by reducing Kafka poll overhead.

### Config

```properties
# Already exists — just needs to be used by consumer
SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS=500
```

---

## Task 7: OpenTelemetry Spans in Worker and Consumer

**Status:** ✅ Complete
**Completed:** 2024-09-25
**Priority:** Low — observability enhancement
**Effort:** Low

### Problem

TracerProvider is configured but no spans are created during message processing. The spec requires `firehose.process_batch` and `firehose.consume` spans with attributes.

### Implementation

1. In consumer's `Run()`:
   ```go
   ctx, span := tracer.Start(ctx, "firehose.consume")
   span.SetAttributes(
       attribute.String("topic", msg.Topic),
       attribute.Int("partition", msg.Partition),
       attribute.Int64("offset", msg.Offset),
   )
   defer span.End()
   ```

2. In worker's `processBatch()`:
   ```go
   ctx, span := tracer.Start(ctx, "firehose.process_batch")
   span.SetAttributes(
       attribute.String("batch.id", batch.ID),
       attribute.Int("batch.size", len(batch.Messages)),
       attribute.String("sink", "http"),
   )
   // ... after sink.Push:
   span.SetAttributes(
       attribute.Int("http.status", statusCode),
       attribute.Int("failed.count", len(failed)),
       attribute.Int("retry.count", retryCount),
   )
   defer span.End()
   ```

### Config

```properties
OTEL_TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_SERVICE_NAME=goose
```

---

## Task 8: Horizontal Pod Autoscaler (HPA) in Helm Chart

**Status:** ✅ Complete
**Completed:** 2026-09-26
**Priority:** Medium — production scaling
**Effort:** Low

### Problem

Helm chart has no HPA template — pods can't auto-scale based on CPU or consumer lag.

### Implementation

Add `templates/hpa.yaml`:

```yaml
{{- if .Values.autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ .Release.Name }}-goose
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ .Release.Name }}-goose
  minReplicas: {{ .Values.autoscaling.minReplicas }}
  maxReplicas: {{ .Values.autoscaling.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .Values.autoscaling.targetCPUUtilizationPercentage }}
{{- end }}
```

### Config in values.yaml

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 20
  targetCPUUtilizationPercentage: 80
```

### Important

HPA scaling is capped by Kafka partitions — can't have more pods than partitions. Set `maxReplicas ≤ partition count`.

---

## Task 9: Consumer Lag Metrics (from Kafka)

**Status:** ✅ Complete
**Completed:** 2024-09-25
**Priority:** Low — dashboard enhancement
**Effort:** Low

### Problem

`firehose_consumer_lag_messages` metric is defined but never populated. No way to see how far behind goose is from the Kafka head.

### Implementation

In consumer's `Run()` loop, periodically check lag:
```go
// Every N messages, check lag
stats := c.reader.Stats()
c.metrics.ConsumerLag.WithLabelValues(msg.Topic, strconv.Itoa(msg.Partition)).Set(float64(stats.Lag))
```

Or use `reader.Lag(ctx, partition)` API (if available in kafka-go version).

---

## Task 10: OAuth2 Token Refresh for HTTP Sink

**Status:** 🔲 Not started
**Priority:** Low — only if sink requires OAuth2
**Effort:** Low

### Problem

OAuth2 config exists but the token is likely fetched once and not refreshed. OAuth2 tokens typically expire after 1 hour.

### Implementation

1. Background goroutine that refreshes the token periodically
2. Token stored atomically (`atomic.Pointer[string]`)
3. HTTP sink reads current token before each request
4. Refresh when token is 80% through its lifetime

---

## Summary — Priority Order

| # | Task | Priority | Effort | Status |
|---|---|---|---|---|
| 1 | Proto→JSON conversion | High | Medium | ✅ Done |
| 2 | Schema registry long-polling | High | Medium | ✅ Done |
| 6 | Batch-poll consumer | Medium | Medium | ✅ Done |
| 3 | gRPC sink | Medium | Medium-High | ✅ Done |
| 8 | HPA in Helm chart | Medium | Low | ✅ Done |
| 7 | OTel spans | Low | Low | ✅ Done |
| 9 | Consumer lag metrics | Low | Low | ✅ Done |
| 5 | Schema validation | Low | Medium | ✅ Done |
| 4 | Avro support | Low | High | 🔲 Not started |
| 10 | OAuth2 token refresh | Low | Low | 🔲 Not started |

## Completed Tasks

| # | Task | Completed Date | Notes |
|---|---|---|---|
| — | Auto-match MaxConnections to WorkerPoolSize | 2024-09-24 | Validates that pool size = workers to prevent port exhaustion |
| — | Kafka offset commits | 2024-09-24 | commitLoop now calls reader.CommitMessages() |
| — | Offset manager pruning | 2024-09-24 | PruneCommitted() prevents unbounded memory growth |
| — | ActionFail crashes consumer | 2024-09-24 | log.Panicf instead of just logging |
| — | Circuit breaker | 2024-09-24 | Sliding window with open/close/half-open states |
| — | DLQ to Kafka | 2024-09-24 | KafkaDLQWriter with metadata headers |
| — | Prometheus metrics (15) | 2024-09-24 | All defined and wired |
| — | Connection TTL + idle eviction | 2024-09-24 | Fixes raystack's connection pinning bug |
| — | JSON-path filter | 2024-09-24 | NoOpFilter + JSONPathFilter |
| — | CEL filter | 2026-09-25 | cel-go engine with auto-detection |
| — | CEL schema validation | 2026-09-25 | Semicolon-separated expressions |
| — | Helm chart | 2024-09-24 | Deployment, Service, ConfigMap, HPA |
| — | Dockerfile (distroless, ~33MB) | 2024-09-24 | Multi-stage build |
| — | E2E integration tests (15) | 2024-09-24 | E2E, retry, DLQ, circuit breaker, CEL, proto |
| — | Load tested to 2,500/s (single-poll) | 2026-07-13 | 600K msgs, 100ms delay, 250 workers |
| — | Port exhaustion fix | 2026-07-13 | Auto-match connections to workers (25x improvement) |
| — | Network error retry (zero drops) | 2026-07-13 | Status code 0 always retried |
| — | Batch-poll consumer load test | 2026-09-28 | 6,017/s @0ms, ~4,000/s @50ms, 0 drops, 13-23 MiB |
| — | Batch-with-response HTTP mode | 2026-09-25 | Per-message results from batch endpoint |
| — | MongoDB sink | 2026-09-25 | Insert + upsert with duplicate key → DLQ |
| — | PostgreSQL sink | 2026-09-25 | Auto column mapping, INSERT + ON CONFLICT upsert |
| — | Redis sink | 2026-09-25 | Keyvalue, hashset, list with key templating |
| — | Readiness/liveness probes | 2026-09-26 | Helm deployment with probes |
| — | v1.0.0 release | 2026-09-26 | Docker image on GHCR, Helm chart release asset |
