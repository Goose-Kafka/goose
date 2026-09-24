package offsetmanager

import (
	"reflect"
	"testing"
)

func TestAddBatchAndSetCommittable(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "test", Partition: 0, Offset: 1},
		{Topic: "test", Partition: 0, Offset: 2},
		{Topic: "test", Partition: 0, Offset: 3},
	})

	// Nothing is committable until the batch is marked done.
	if got := om.GetCommittable(); len(got) != 0 {
		t.Errorf("GetCommittable() before SetCommittable = %v, want empty map", got)
	}

	om.SetCommittable("batch-1")

	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 4, // highest offset 3 + 1 = next to consume
	}
	if got := om.GetCommittable(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCommittable() after SetCommittable = %v, want %v", got, want)
	}
}

func TestContiguityGating(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "test", Partition: 0, Offset: 1},
		{Topic: "test", Partition: 0, Offset: 2},
		{Topic: "test", Partition: 0, Offset: 3},
	})
	om.AddBatch("batch-2", []Message{
		{Topic: "test", Partition: 0, Offset: 4},
		{Topic: "test", Partition: 0, Offset: 5},
		{Topic: "test", Partition: 0, Offset: 6},
	})

	// Mark batch-2 done first — batch-1 still pending blocks contiguity.
	om.SetCommittable("batch-2")
	if got := om.GetCommittable(); len(got) != 0 {
		t.Errorf("GetCommittable() with batch-1 pending = %v, want empty map", got)
	}

	// Now complete batch-1 — full contiguous prefix [1..6] becomes committable.
	om.SetCommittable("batch-1")
	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 7, // highest offset 6 + 1
	}
	if got := om.GetCommittable(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCommittable() after both batches = %v, want %v", got, want)
	}
}

func TestMultiplePartitions(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "topic-a", Partition: 0, Offset: 10},
		{Topic: "topic-a", Partition: 0, Offset: 11},
		{Topic: "topic-b", Partition: 1, Offset: 20},
		{Topic: "topic-b", Partition: 1, Offset: 21},
	})

	om.SetCommittable("batch-1")

	want := map[TopicPartition]int64{
		{Topic: "topic-a", Partition: 0}: 12,
		{Topic: "topic-b", Partition: 1}: 22,
	}
	if got := om.GetCommittable(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCommittable() = %v, want %v", got, want)
	}
}

func TestAddOffsetsAndSetCommittableImmediate(t *testing.T) {
	om := New()

	// Offsets are immediately committable — no SetCommittable needed.
	om.AddOffsetsAndSetCommittable([]Message{
		{Topic: "test", Partition: 0, Offset: 5},
		{Topic: "test", Partition: 0, Offset: 6},
	})

	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 7,
	}
	if got := om.GetCommittable(); !reflect.DeepEqual(got, want) {
		t.Errorf("GetCommittable() = %v, want %v", got, want)
	}
}

func TestGetCommittableIdempotent(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "test", Partition: 0, Offset: 1},
		{Topic: "test", Partition: 0, Offset: 2},
		{Topic: "test", Partition: 0, Offset: 3},
	})
	om.SetCommittable("batch-1")

	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 4,
	}

	first := om.GetCommittable()
	if !reflect.DeepEqual(first, want) {
		t.Errorf("GetCommittable() first call = %v, want %v", first, want)
	}

	second := om.GetCommittable()
	if !reflect.DeepEqual(second, want) {
		t.Errorf("GetCommittable() second call = %v, want %v", second, want)
	}

	if !reflect.DeepEqual(first, second) {
		t.Errorf("GetCommittable() not idempotent: first=%v second=%v", first, second)
	}
}

func TestPruneCommitted(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "test", Partition: 0, Offset: 1},
		{Topic: "test", Partition: 0, Offset: 2},
	})
	om.AddBatch("batch-2", []Message{
		{Topic: "test", Partition: 0, Offset: 3},
		{Topic: "test", Partition: 0, Offset: 4},
	})
	om.SetCommittable("batch-1")
	om.SetCommittable("batch-2")

	committable := om.GetCommittable()
	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 5,
	}
	if !reflect.DeepEqual(committable, want) {
		t.Fatalf("GetCommittable() = %v, want %v", committable, want)
	}

	om.PruneCommitted(committable)

	// After pruning, all committed offsets (1-4) are removed. GetCommittable
	// returns empty because nothing remains.
	pruned := om.GetCommittable()
	if len(pruned) != 0 {
		t.Errorf("GetCommittable() after PruneCommitted = %v, want empty map", pruned)
	}

	// Add new offset and verify contiguity gating works correctly after pruning.
	om.AddBatch("batch-3", []Message{
		{Topic: "test", Partition: 0, Offset: 5},
	})
	// batch-3 is not yet committable, so nothing should be committable
	got := om.GetCommittable()
	if len(got) != 0 {
		t.Errorf("GetCommittable() with pending batch-3 = %v, want empty map", got)
	}

	om.SetCommittable("batch-3")
	got = om.GetCommittable()
	wantAfter := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 6,
	}
	if !reflect.DeepEqual(got, wantAfter) {
		t.Errorf("GetCommittable() after batch-3 = %v, want %v", got, wantAfter)
	}
}

func TestPruneCommittedMultiplePartitions(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "topic-a", Partition: 0, Offset: 10},
		{Topic: "topic-b", Partition: 1, Offset: 20},
	})
	om.SetCommittable("batch-1")

	committable := om.GetCommittable()
	want := map[TopicPartition]int64{
		{Topic: "topic-a", Partition: 0}: 11,
		{Topic: "topic-b", Partition: 1}: 21,
	}
	if !reflect.DeepEqual(committable, want) {
		t.Fatalf("GetCommittable() = %v, want %v", committable, want)
	}

	om.PruneCommitted(committable)

	// After pruning, both partitions should be empty.
	got := om.GetCommittable()
	if len(got) != 0 {
		t.Errorf("GetCommittable() after prune = %v, want empty map", got)
	}
}

func TestPruneCommittedPreservesUncommitted(t *testing.T) {
	om := New()

	om.AddBatch("batch-1", []Message{
		{Topic: "test", Partition: 0, Offset: 1},
	})
	om.AddBatch("batch-2", []Message{
		{Topic: "test", Partition: 0, Offset: 2},
	})
	// Only batch-1 is committable — batch-2 is pending.
	om.SetCommittable("batch-1")

	committable := om.GetCommittable()
	want := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 2,
	}
	if !reflect.DeepEqual(committable, want) {
		t.Fatalf("GetCommittable() = %v, want %v", committable, want)
	}

	om.PruneCommitted(committable)

	// Prune removes offset 1 (< 2) but keeps offset 2 (== 2, not < 2).
	// Offset 2 is not committable, so GetCommittable returns empty.
	got := om.GetCommittable()
	if len(got) != 0 {
		t.Errorf("GetCommittable() after prune = %v, want empty (offset 2 not committable)", got)
	}

	// Now mark batch-2 done — offset 2 should become committable.
	om.SetCommittable("batch-2")
	got = om.GetCommittable()
	wantAfter := map[TopicPartition]int64{
		{Topic: "test", Partition: 0}: 3,
	}
	if !reflect.DeepEqual(got, wantAfter) {
		t.Errorf("GetCommittable() after batch-2 = %v, want %v", got, wantAfter)
	}
}
