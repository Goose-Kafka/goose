package errorpkg

import (
	"context"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// KafkaDLQWriter writes failed messages to a Kafka dead-letter queue topic.
type KafkaDLQWriter struct {
	writer *kafka.Writer
}

// NewKafkaDLQWriter creates a KafkaDLQWriter that writes to the given topic
// on the specified broker(s).
func NewKafkaDLQWriter(brokers, topic string) *KafkaDLQWriter {
	return &KafkaDLQWriter{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchTimeout:           10 * 1000 * 1000, // 10ms
			AllowAutoTopicCreation: true,
		},
	}
}

// Write converts each FailedMessage into a kafka.Message with metadata headers
// and writes them to the DLQ topic.
func (w *KafkaDLQWriter) Write(msgs []FailedMessage) error {
	kMessages := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		kMessages = append(kMessages, kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Headers: []kafka.Header{
				{Key: "original_topic", Value: []byte(msg.Topic)},
				{Key: "original_partition", Value: []byte(strconv.Itoa(msg.Partition))},
				{Key: "original_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
				{Key: "error_type", Value: []byte(string(msg.ErrorInfo.ErrorType))},
				{Key: "error_status_code", Value: []byte(strconv.Itoa(msg.ErrorInfo.StatusCode))},
				{Key: "error_message", Value: []byte(msg.ErrorInfo.Message)},
			},
		})
	}
	return w.writer.WriteMessages(context.Background(), kMessages...)
}

// Close closes the underlying Kafka writer.
func (w *KafkaDLQWriter) Close() error {
	return w.writer.Close()
}
