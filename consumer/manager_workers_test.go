package consumer

import (
	"context"
	"testing"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

func TestConsumerManager_ChangeWorkersCountRebuildsWorkerPool(t *testing.T) {
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error { return nil }
	m := &ConsumerManager{
		config: ConsumerConfig{
			WorkerCount: 2,
			BufferSize:  10,
			Pipeline:    pipeline,
			Strategy:    distribution.NewFreeWorker(),
		},
		pool:  NewWorkerPool(2, 10, pipeline, distribution.NewFreeWorker()),
		state: stateRunning,
	}

	if err := m.ChangeWorkersCount(4); err != nil {
		t.Fatalf("change workers: %v", err)
	}

	if got := len(m.pool.Workers()); got != 4 {
		t.Fatalf("expected rebuilt pool with 4 workers, got %d", got)
	}
}
