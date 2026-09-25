package consumer

import (
	"testing"

	"github.com/Goose-Kafka/goose/internal/config"
	"github.com/Goose-Kafka/goose/internal/filter"
	"github.com/Goose-Kafka/goose/internal/worker"
)

func TestConsumerCreation(t *testing.T) {
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:         "localhost:9092",
			Topic:           "test-topic",
			ConsumerGroupID: "test-group",
			MaxPollRecords:  100,
			PollTimeoutMs:   1000,
		},
		HTTP: config.HTTPConfig{
			FilterEnabled:  false,
			FilterJSONPath: "",
		},
		Schema: config.SchemaConfig{
			InputSchemaDataType: "json",
		},
	}

	batchChan := make(chan<- *worker.Batch, 10)
	doneChan := make(<-chan string, 10)

	c, err := New(cfg, batchChan, doneChan, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil consumer, got nil")
	}
}

func TestNoOpFilterIntegration(t *testing.T) {
	f := filter.NewNoOpFilter()
	msgs := []filter.Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte(`{"status":"created"}`)},
	}
	passed, dropped := f.Apply(msgs)
	if len(passed) != 1 {
		t.Fatalf("expected passed=1, got %d", len(passed))
	}
	if len(dropped) != 0 {
		t.Fatalf("expected dropped=0, got %d", len(dropped))
	}
}
