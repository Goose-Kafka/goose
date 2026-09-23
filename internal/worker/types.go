package worker

import "github.com/arelligoutham/goose/internal/sink"

// Batch is a group of messages sent to a worker for processing.
type Batch struct {
	ID       string
	Messages []sink.Message
}
