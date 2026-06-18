package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

func TestWorkerPool_DispatchToWorker(t *testing.T) {
	processed := make(chan int, 10)
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		processed <- mc.ConsumerContext.WorkerID
		return nil
	}

	pool := consumer.NewWorkerPool(3, 10, pipeline, distribution.NewFreeWorker())
	eg := pool.Start(context.Background())
	defer func() {
		pool.Stop()
		eg.Wait()
	}()

	for i := 0; i < 5; i++ {
		mc := &kafkaflow.MessageContext{
			ConsumerContext: &kafkaflow.ConsumerContext{
				Topic:     "t",
				Partition: 0,
				Offset:    int64(i),
			},
			Message: &kafkaflow.Message{Key: []byte("key")},
		}
		assert.True(t, pool.Dispatch(mc))
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 5, len(processed))
}

func TestWorkerPool_DispatchReturnsFalseWhenFull(t *testing.T) {
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		time.Sleep(time.Second)
		return nil
	}

	pool := consumer.NewWorkerPool(1, 1, pipeline, distribution.NewFreeWorker())
	eg := pool.Start(context.Background())
	defer func() {
		pool.Stop()
		eg.Wait()
	}()

	mc := &kafkaflow.MessageContext{
		ConsumerContext: &kafkaflow.ConsumerContext{Topic: "t", Partition: 0, Offset: 0},
		Message:         &kafkaflow.Message{},
	}

	assert.True(t, pool.Dispatch(mc))
	assert.False(t, pool.Dispatch(mc))
}
