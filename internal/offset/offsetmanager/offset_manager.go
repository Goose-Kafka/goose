package offsetmanager

import "sync"

// TopicPartition identifies a Kafka topic partition.
type TopicPartition struct {
	Topic     string
	Partition int
}

// Message represents a single Kafka message with its location and offset.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
}

// offsetNode tracks a single offset within a partition and whether it has been
// marked as committable (i.e. its batch has finished processing).
type offsetNode struct {
	tp          TopicPartition
	offset      int64
	committable bool
}

// OffsetManager tracks per-partition offsets and computes the highest
// contiguous committable offset per partition. It is the heart of
// at-least-once delivery: an offset can only be committed to Kafka when every
// offset below it in the same partition has also been marked committable.
//
// The model mirrors raystack firehose's OffsetManager: a consumer registers
// offsets under a batch ID (not yet committable), workers call
// SetCommittable(batchID) when a batch finishes, and a commit goroutine
// periodically calls GetCommittable() to obtain the contiguous prefix to
// commit per partition.
type OffsetManager struct {
	mu            sync.Mutex
	batches       map[string][]offsetNode         // batchID → list of registered nodes
	sortedOffsets map[TopicPartition][]offsetNode // partition → offsets sorted ascending
}

// New creates an empty OffsetManager.
func New() *OffsetManager {
	return &OffsetManager{
		batches:       make(map[string][]offsetNode),
		sortedOffsets: make(map[TopicPartition][]offsetNode),
	}
}

// AddBatch registers the given messages under batchID. The offsets are not yet
// committable until SetCommittable(batchID) is called.
func (om *OffsetManager) AddBatch(batchID string, msgs []Message) {
	om.mu.Lock()
	defer om.mu.Unlock()

	for _, m := range msgs {
		node := offsetNode{
			tp:          TopicPartition{Topic: m.Topic, Partition: m.Partition},
			offset:      m.Offset,
			committable: false,
		}
		om.batches[batchID] = append(om.batches[batchID], node)
		om.sortedOffsets[node.tp] = insertSorted(om.sortedOffsets[node.tp], node)
	}
}

// SetCommittable marks all offsets registered under batchID as committable and
// removes the batch from the pending set.
func (om *OffsetManager) SetCommittable(batchID string) {
	om.mu.Lock()
	defer om.mu.Unlock()

	nodes, ok := om.batches[batchID]
	if !ok {
		return
	}

	for _, node := range nodes {
		sorted := om.sortedOffsets[node.tp]
		for i := range sorted {
			if sorted[i].offset == node.offset {
				sorted[i].committable = true
				break
			}
		}
	}

	delete(om.batches, batchID)
}

// AddOffsetsAndSetCommittable registers offsets that are immediately
// committable, without requiring a subsequent SetCommittable call. This is
// used for offsets that skip the batch-processing pipeline (e.g. control
// records or no-op messages).
func (om *OffsetManager) AddOffsetsAndSetCommittable(msgs []Message) {
	om.mu.Lock()
	defer om.mu.Unlock()

	for _, m := range msgs {
		node := offsetNode{
			tp:          TopicPartition{Topic: m.Topic, Partition: m.Partition},
			offset:      m.Offset,
			committable: true,
		}
		om.sortedOffsets[node.tp] = insertSorted(om.sortedOffsets[node.tp], node)
	}
}

// GetCommittable returns the highest contiguous committable offset + 1 per
// partition — i.e. the next offset to consume after all completed work. For
// each partition it walks the sorted offsets from the lowest and collects the
// unbroken committable prefix, stopping at the first non-committable gap.
// An empty map means nothing is committable yet.
func (om *OffsetManager) GetCommittable() map[TopicPartition]int64 {
	om.mu.Lock()
	defer om.mu.Unlock()

	result := make(map[TopicPartition]int64)
	for tp, sorted := range om.sortedOffsets {
		var highest int64 = -1
		for _, node := range sorted {
			if !node.committable {
				break
			}
			highest = node.offset
		}
		if highest >= 0 {
			result[tp] = highest + 1
		}
	}
	return result
}

// insertSorted inserts node into nodes maintaining ascending order by offset.
// It uses binary search to find the insertion index.
func insertSorted(nodes []offsetNode, node offsetNode) []offsetNode {
	lo, hi := 0, len(nodes)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if nodes[mid].offset < node.offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(nodes) {
		return append(nodes, node)
	}
	nodes = append(nodes, offsetNode{})
	copy(nodes[lo+1:], nodes[lo:])
	nodes[lo] = node
	return nodes
}
