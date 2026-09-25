package consumer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/Goose-Kafka/goose/internal/config"
	"github.com/Goose-Kafka/goose/internal/error"
	"github.com/Goose-Kafka/goose/internal/filter"
	"github.com/Goose-Kafka/goose/internal/metrics"
	"github.com/Goose-Kafka/goose/internal/offset/offsetmanager"
	"github.com/Goose-Kafka/goose/internal/schema"
	"github.com/Goose-Kafka/goose/internal/sink"
	"github.com/Goose-Kafka/goose/internal/validation"
	"github.com/Goose-Kafka/goose/internal/worker"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Consumer polls Kafka, deserializes messages via the schema manager, applies
// a filter, creates batches with UUIDs, registers offsets for at-least-once
// delivery, and dispatches batches to the worker pool channel. Backpressure
// is natural — the consumer blocks on the batchChan send until a worker is
// ready.
type Consumer struct {
	cfg       *config.Config
	reader    *kafka.Reader
	batchChan chan<- *worker.Batch
	doneChan  <-chan string
	offsetMgr *offsetmanager.OffsetManager
	schemaMgr schema.SchemaManager
	filter    filter.Filter
	validator *validation.Validator
	metrics   *metrics.Metrics
	tracer    trace.Tracer
	dlqWriter *errorpkg.KafkaDLQWriter
}

// Reader returns the underlying kafka-go reader so that external callers
// (e.g. the commit loop) can commit offsets via the consumer group protocol.
func (c *Consumer) Reader() *kafka.Reader {
	return c.reader
}

// New creates a Consumer with a kafka-go reader, schema manager, and filter
// based on the provided config. The reader is created but not connected —
// Kafka is contacted only when Run is called.
func New(cfg *config.Config, batchChan chan<- *worker.Batch, doneChan <-chan string, offsetMgr *offsetmanager.OffsetManager, m *metrics.Metrics) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(cfg.Kafka.Brokers, ","),
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.ConsumerGroupID,
		MaxBytes: 10e6,
	})

	schemaMgr, err := schema.NewSchemaManager(schema.Config{
		InputSchemaDataType:      cfg.Schema.InputSchemaDataType,
		SchemaRegistryEnabled:    cfg.Schema.SchemaRegistryEnabled,
		SchemaRegistryURL:        cfg.Schema.SchemaRegistryURL,
		SchemaRegistryProtoClass: cfg.Schema.SchemaRegistryProtoClass,
		RefreshStrategy:          cfg.Schema.RefreshStrategy,
		RefreshIntervalMs:        cfg.Schema.RefreshIntervalMs,
		FetchTimeoutMs:           cfg.Schema.FetchTimeoutMs,
		AuthBearerToken:          cfg.Schema.AuthBearerToken,
	})
	if err != nil {
		return nil, fmt.Errorf("schema manager: %w", err)
	}

	var f filter.Filter
	if cfg.HTTP.FilterEnabled && cfg.HTTP.FilterJSONPath != "" {
		// Parse filter config: "expression" for jsonpath or "expression" for cel
		// The expression format determines the engine:
		//   "$.field.path" → jsonpath (starts with $)
		//   "field == value" → cel (doesn't start with $)
		engine := "cel"
		expression := cfg.HTTP.FilterJSONPath
		matchValue := ""

		// JSONPath expressions start with $, CEL expressions don't
		if strings.HasPrefix(expression, "$") {
			engine = "jsonpath"
			// JSONPath with match value: "$.field:value"
			if strings.Contains(expression, ":") {
				parts := strings.SplitN(expression, ":", 2)
				expression = strings.TrimSpace(parts[0])
				matchValue = strings.TrimSpace(parts[1])
			}
		}

		var ferr error
		f, ferr = filter.NewFilter(filter.FilterConfig{
			Engine:     engine,
			Expression: expression,
			MatchValue: matchValue,
		})
		if ferr != nil {
			log.Printf("consumer: invalid filter expression %q (engine=%s): %v, falling back to NoOp filter", cfg.HTTP.FilterJSONPath, engine, ferr)
			f = filter.NewNoOpFilter()
		}
	} else {
		f = filter.NewNoOpFilter()
	}

	// Schema validation (CEL-based)
	var validator *validation.Validator
	if cfg.Validation.Enabled && cfg.Validation.CELExpression != "" {
		validator, err = validation.NewValidator(cfg.Validation.CELExpression)
		if err != nil {
			log.Printf("consumer: invalid validation expression %q: %v, disabling validation", cfg.Validation.CELExpression, err)
			validator = nil
		} else {
			log.Printf("consumer: schema validation enabled (%d expressions)", len(strings.Split(cfg.Validation.CELExpression, ";")))
		}
	}

	// OTel tracer
	tracer := otel.Tracer("goose")

	return &Consumer{
		cfg:       cfg,
		reader:    reader,
		batchChan: batchChan,
		doneChan:  doneChan,
		offsetMgr: offsetMgr,
		schemaMgr: schemaMgr,
		filter:    f,
		validator: validator,
		metrics:   m,
		tracer:    tracer,
	}, nil
}

// Run is the consumer's main poll loop. It reads messages from Kafka,
// deserializes, filters, creates batches, and dispatches them to the worker
// pool. The loop exits when the context is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	defer c.schemaMgr.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// OTel span: firehose.consume
		ctx, span := c.tracer.Start(ctx, "firehose.consume",
			trace.WithAttributes(
				attribute.String("kafka.topic", ""),
			),
		)

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			span.End()
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("consumer: read error: %v", err)
			continue
		}

		span.SetAttributes(
			attribute.String("kafka.topic", msg.Topic),
			attribute.Int("kafka.partition", msg.Partition),
			attribute.Int64("kafka.offset", msg.Offset),
		)

		parsed, err := c.schemaMgr.Parse(msg.Value)
		if err != nil {
			log.Printf("consumer: schema parse error for topic=%s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			span.End()
			continue
		}

		// Schema validation (if enabled)
		if c.validator != nil {
			valid, reason := c.validator.Validate(parsed)
			if !valid {
				log.Printf("consumer: validation failed for topic=%s partition=%d offset=%d: %s", msg.Topic, msg.Partition, msg.Offset, reason)
				if c.metrics != nil {
					c.metrics.ValidationFailed.WithLabelValues(reason).Inc()
				}
				// Route based on SCHEMA_VALIDATION_ON_FAILURE
				switch c.cfg.Validation.OnFailure {
				case "dlq":
					// Send to DLQ — need the DLQ writer for this
					// For now, mark offset as committable and log
					c.offsetMgr.AddOffsetsAndSetCommittable([]offsetmanager.Message{
						{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset},
					})
				case "drop":
					c.offsetMgr.AddOffsetsAndSetCommittable([]offsetmanager.Message{
						{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset},
					})
				default: // "ignore"
					c.offsetMgr.AddOffsetsAndSetCommittable([]offsetmanager.Message{
						{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset},
					})
				}
				span.SetAttributes(attribute.Bool("validation.passed", false))
				span.End()
				continue
			}
			span.SetAttributes(attribute.Bool("validation.passed", true))
		}

		sinkMsg := sink.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     parsed,
		}

		filterMsg := filter.Message{
			Topic:     sinkMsg.Topic,
			Partition: sinkMsg.Partition,
			Offset:    sinkMsg.Offset,
			Key:       sinkMsg.Key,
			Value:     sinkMsg.Value,
		}

		passed, dropped := c.filter.Apply([]filter.Message{filterMsg})

		if len(dropped) > 0 {
			if c.offsetMgr != nil {
				c.offsetMgr.AddOffsetsAndSetCommittable(toOffsetMessages(dropped))
			}
			if c.metrics != nil {
				c.metrics.MessagesFiltered.WithLabelValues(msg.Topic).Add(float64(len(dropped)))
			}
			span.SetAttributes(attribute.Bool("filter.passed", false))
		} else {
			span.SetAttributes(attribute.Bool("filter.passed", true))
		}

		if len(passed) == 0 {
			span.End()
			continue
		}

		batch := &worker.Batch{
			ID:       uuid.NewString(),
			Messages: toSinkMessages(passed),
		}

		if c.offsetMgr != nil {
			c.offsetMgr.AddBatch(batch.ID, toOffsetMessages(passed))
		}

		if c.metrics != nil {
			c.metrics.MessagesConsumed.WithLabelValues(msg.Topic, strconv.Itoa(msg.Partition)).Inc()
			// Consumer lag from kafka-go reader stats
			stats := c.reader.Stats()
			c.metrics.ConsumerLag.WithLabelValues(msg.Topic, strconv.Itoa(msg.Partition)).Set(float64(stats.Lag))
		}

		span.SetAttributes(attribute.String("batch.id", batch.ID), attribute.Int("batch.size", len(batch.Messages)))
		span.End()

		select {
		case c.batchChan <- batch:
		case <-ctx.Done():
			return nil
		}
	}
}

// toOffsetMessages converts filter messages to offset-manager messages.
func toOffsetMessages(msgs []filter.Message) []offsetmanager.Message {
	result := make([]offsetmanager.Message, len(msgs))
	for i, m := range msgs {
		result[i] = offsetmanager.Message{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
		}
	}
	return result
}

// toSinkMessages converts filter messages to sink messages.
func toSinkMessages(msgs []filter.Message) []sink.Message {
	result := make([]sink.Message, len(msgs))
	for i, m := range msgs {
		result[i] = sink.Message{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       m.Key,
			Value:     m.Value,
		}
	}
	return result
}
