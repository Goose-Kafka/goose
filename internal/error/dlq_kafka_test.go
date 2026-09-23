package errorpkg

import "testing"

func TestDLQWriterCreation(t *testing.T) {
	w := NewKafkaDLQWriter("localhost:9092", "firehose-dlq")
	if w == nil {
		t.Fatal("expected non-nil DLQ writer")
	}
	defer w.Close()
}
