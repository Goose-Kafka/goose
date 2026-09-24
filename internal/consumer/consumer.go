package consumer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/arelligoutham/goose/internal/config"
	"github.com/arelligoutham/goose/internal/filter"
	"github.com/arelligoutham/goose/internal/metrics"
	"github.com/arelligoutham/goose/internal/offset/offsetmanager"
	"github.com/arelligoutham/goose/internal/schema"
	"github.com/arelligoutham/goose/internal/sink"
	"github.com/arelligoutham/goose/internal/worker"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
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
	metrics   *metrics.Metrics
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
		var ferr error
		f, ferr = filter.NewJSONPathFilter(filter.FilterConfig{Expression: cfg.HTTP.FilterJSONPath})
		if ferr != nil {
			log.Printf("consumer: invalid filter expression %q: %v, falling back to NoOp filter", cfg.HTTP.FilterJSONPath, ferr)
			f = filter.NewNoOpFilter()
		}
	} else {
		f = filter.NewNoOpFilter()
	}

	return &Consumer{
		cfg:       cfg,
		reader:    reader,
		batchChan: batchChan,
		doneChan:  doneChan,
		offsetMgr: offsetMgr,
		schemaMgr: schemaMgr,
		filter:    f,
		metrics:   m,
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

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("consumer: read error: %v", err)
			continue
		}

		parsed, err := c.schemaMgr.Parse(msg.Value)
		if err != nil {
			log.Printf("consumer: schema parse error for topic=%s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
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
		}

		if len(passed) == 0 {
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
			// TODO: set firehose_consumer_lag_messages gauge.
			// Requires reader.Stats().Lag or reader.Lag(ctx, partition) call.
			// See kafka-go ReaderStats for partition lag data.
		}

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
