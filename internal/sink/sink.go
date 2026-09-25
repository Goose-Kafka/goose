package sink

import errorpkg "github.com/Goose-Kafka/goose/internal/error"

// Message represents a single message to be delivered to a sink.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte // JSON bytes for HTTP sink
}

// Sink is the pluggable interface for delivering messages to an external
// destination. Implementations include HTTP, Redis, Elasticsearch, etc.
type Sink interface {
	Push(msgs []Message) ([]errorpkg.FailedMessage, error)
	Close() error
}

// HTTPSinkConfig holds configuration for an HTTP sink.
type HTTPSinkConfig struct {
	ServiceURL           string
	RequestMethod        string
	Timeout              int // milliseconds
	MaxConnections       int
	ConnectionTTL        int // milliseconds
	ConnectionIdleEvict  int // milliseconds
	ValidateInactivityMs int
	Headers              string
	JSONBodyTemplate     string
	DataFormat           string
}
