package consumer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
)

func TestOffsetStore_StoreAndSnapshot(t *testing.T) {
	store := consumer.NewOffsetStore()

	store.Track("topic-a", 0, 10)
	store.Track("topic-a", 0, 11)
	store.Track("topic-a", 0, 12)
	store.Track("topic-b", 1, 20)
	store.Store("topic-a", 0, 10)
	store.Store("topic-a", 0, 11)
	store.Store("topic-a", 0, 12)
	store.Store("topic-b", 1, 20)

	snapshot := store.Snapshot()
	assert.Equal(t, int64(12), snapshot[consumer.TopicPartition{Topic: "topic-a", Partition: 0}])
	assert.Equal(t, int64(20), snapshot[consumer.TopicPartition{Topic: "topic-b", Partition: 1}])
}

func TestOffsetStore_SnapshotIsCopy(t *testing.T) {
	store := consumer.NewOffsetStore()
	store.Track("topic-a", 0, 10)
	store.Store("topic-a", 0, 10)

	snapshot := store.Snapshot()
	snapshot[consumer.TopicPartition{Topic: "topic-a", Partition: 0}] = 999

	snapshot2 := store.Snapshot()
	assert.Equal(t, int64(10), snapshot2[consumer.TopicPartition{Topic: "topic-a", Partition: 0}])
}

func TestOffsetStore_DoesNotAdvancePastGap(t *testing.T) {
	store := consumer.NewOffsetStore()
	store.Track("topic-a", 0, 10)
	store.Track("topic-a", 0, 11)
	store.Track("topic-a", 0, 12)

	store.Store("topic-a", 0, 10)
	store.Store("topic-a", 0, 12)

	snapshot := store.Snapshot()
	assert.Equal(t, int64(10), snapshot[consumer.TopicPartition{Topic: "topic-a", Partition: 0}])
}

func TestOffsetStore_DoesNotCommitWhenFirstTrackedOffsetIsUnprocessed(t *testing.T) {
	store := consumer.NewOffsetStore()
	store.Track("topic-a", 0, 10)
	store.Track("topic-a", 0, 11)
	store.Track("topic-a", 0, 12)

	store.Store("topic-a", 0, 12)

	snapshot := store.Snapshot()
	_, ok := snapshot[consumer.TopicPartition{Topic: "topic-a", Partition: 0}]
	assert.False(t, ok)
}
