package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
)

func TestWorker_ProcessesMessage(t *testing.T) {
	processed := make(chan *kafkaflow.MessageContext, 1)
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		processed <- mc
		return nil
	}

	w := consumer.NewWorker(0, 10, pipeline)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	mc := &kafkaflow.MessageContext{
		ConsumerContext: &kafkaflow.ConsumerContext{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    42,
		},
		Message: &kafkaflow.Message{Value: []byte("hello")},
	}

	w.Channel() <- mc

	select {
	case result := <-processed:
		assert.Equal(t, "test-topic", result.ConsumerContext.Topic)
		assert.Equal(t, int32(0), result.ConsumerContext.Partition)
		assert.Equal(t, int64(42), result.ConsumerContext.Offset)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message processing")
	}
}

func TestWorker_StoresOffset(t *testing.T) {
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		return nil
	}

	w := consumer.NewWorker(0, 10, pipeline)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	mc := &kafkaflow.MessageContext{
		ConsumerContext: &kafkaflow.ConsumerContext{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    42,
		},
		Message: &kafkaflow.Message{},
	}

	w.Channel() <- mc
	time.Sleep(50 * time.Millisecond)

	snapshot := w.OffsetStore().Snapshot()
	tp := consumer.TopicPartition{Topic: "test-topic", Partition: 0}
	assert.Equal(t, int64(42), snapshot[tp])
}

func TestWorker_DrainsOnCancel(t *testing.T) {
	processed := make(chan struct{}, 2)
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		processed <- struct{}{}
		return nil
	}

	w := consumer.NewWorker(0, 10, pipeline)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	w.Channel() <- &kafkaflow.MessageContext{
		ConsumerContext: &kafkaflow.ConsumerContext{Topic: "t", Partition: 0, Offset: 1},
		Message:         &kafkaflow.Message{},
	}
	w.Channel() <- &kafkaflow.MessageContext{
		ConsumerContext: &kafkaflow.ConsumerContext{Topic: "t", Partition: 0, Offset: 2},
		Message:         &kafkaflow.Message{},
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for worker to stop")
	}

	assert.Equal(t, 2, len(processed))
}
