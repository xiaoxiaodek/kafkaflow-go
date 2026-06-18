package consumer

import (
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// OffsetManager collects offsets from all workers and commits the minimum
// processed offset per partition to Kafka. This ensures no offset is committed
// until all prior messages are processed.
type offsetCommitter interface {
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
}

type OffsetManager struct {
	consumer offsetCommitter
	stores   []*OffsetStore
	interval time.Duration
	mu       sync.Mutex
}

// NewOffsetManager creates a new OffsetManager.
func NewOffsetManager(consumer offsetCommitter, stores []*OffsetStore, interval time.Duration) *OffsetManager {
	return &OffsetManager{
		consumer: consumer,
		stores:   stores,
		interval: interval,
	}
}

// Commit collects the minimum offset per partition across all workers and commits.
func (m *OffsetManager) Commit() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	allOffsets := make(map[TopicPartition][]int64)
	for _, store := range m.stores {
		for tp, offset := range store.Snapshot() {
			allOffsets[tp] = append(allOffsets[tp], offset)
		}
	}

	partitions := make([]kafka.TopicPartition, 0, len(allOffsets))
	for tp, offsets := range allOffsets {
		minOffset := offsets[0]
		for _, o := range offsets[1:] {
			if o < minOffset {
				minOffset = o
			}
		}
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &tp.Topic,
			Partition: tp.Partition,
			Offset:    kafka.Offset(minOffset + 1),
		})
	}

	if len(partitions) == 0 {
		return nil
	}

	_, err := m.consumer.CommitOffsets(partitions)
	return err
}

// CommitLoop runs periodic offset commits until the done channel is closed.
func (m *OffsetManager) CommitLoop(done <-chan struct{}) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			m.Commit()
			return
		case <-ticker.C:
			m.Commit()
		}
	}
}
