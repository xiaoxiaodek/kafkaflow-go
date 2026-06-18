package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

type fakeKafkaConsumer struct {
	closed atomic.Bool
}

func (f *fakeKafkaConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error { return nil }
func (f *fakeKafkaConsumer) Assign(partitions []kafka.TopicPartition) error            { return nil }
func (f *fakeKafkaConsumer) Assignment() ([]kafka.TopicPartition, error)                { return nil, nil }
func (f *fakeKafkaConsumer) Pause(partitions []kafka.TopicPartition) error              { return nil }
func (f *fakeKafkaConsumer) Resume(partitions []kafka.TopicPartition) error             { return nil }
func (f *fakeKafkaConsumer) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error) {
	return times, nil
}
func (f *fakeKafkaConsumer) Seek(partition kafka.TopicPartition, timeoutMs int) error { return nil }
func (f *fakeKafkaConsumer) Poll(timeoutMs int) kafka.Event {
	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
	return nil
}
func (f *fakeKafkaConsumer) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return offsets, nil
}
func (f *fakeKafkaConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (int64, int64, error) {
	return 0, 100, nil
}
func (f *fakeKafkaConsumer) Close() error {
	f.closed.Store(true)
	return nil
}

func TestConsumerManager_StartAfterStopCreatesNewKafkaConsumer(t *testing.T) {
	var created atomic.Int32
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error { return nil }
	m := &ConsumerManager{
		config: ConsumerConfig{
			Name:        "orders",
			GroupID:     "orders-group",
			Topics:      []string{"orders"},
			WorkerCount: 1,
			BufferSize:  1,
			Strategy:    distribution.NewFreeWorker(),
			Pipeline:    pipeline,
		},
		consumerFactory: func(cfg ConsumerConfig) (kafkaConsumer, error) {
			created.Add(1)
			return &fakeKafkaConsumer{}, nil
		},
		state: stateStopped,
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- m.Start(ctx1) }()
	time.Sleep(20 * time.Millisecond)
	m.Stop()
	cancel1()
	select {
	case err := <-done1:
		if err != nil {
			t.Fatalf("first start returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first stop")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- m.Start(ctx2) }()
	time.Sleep(20 * time.Millisecond)
	m.Stop()
	cancel2()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second start returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for second stop")
	}

	if got := created.Load(); got != 2 {
		t.Fatalf("expected 2 Kafka consumers to be created, got %d", got)
	}
}

func TestConsumerManager_RestartProperlyStopsThenStarts(t *testing.T) {
	var created atomic.Int32
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error { return nil }
	m := &ConsumerManager{
		config: ConsumerConfig{
			Name:        "orders",
			GroupID:     "orders-group",
			Topics:      []string{"orders"},
			WorkerCount: 1,
			BufferSize:  1,
			Strategy:    distribution.NewFreeWorker(),
			Pipeline:    pipeline,
		},
		consumerFactory: func(cfg ConsumerConfig) (kafkaConsumer, error) {
			created.Add(1)
			return &fakeKafkaConsumer{}, nil
		},
		state: stateStopped,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	if got := m.Status(); got != "Running" {
		t.Fatalf("expected Running after Start, got %s", got)
	}

	if err := m.Restart(); err != nil {
		t.Fatalf("restart returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if got := m.Status(); got != "Stopped" {
		t.Fatalf("expected Stopped immediately after Restart (5s cooldown), got %s", got)
	}

	cancel()
	m.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for final stop")
	}
}
