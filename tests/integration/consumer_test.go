//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kf "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

const (
	testBroker = "localhost:9092"
	testTopic  = "kafkaflow-integration-test"
	testGroup  = "kafkaflow-test-group"
)

func TestConsumerManager_ConsumeMessages(t *testing.T) {
	produceTestMessages(t, testTopic, 10)

	received := make(chan *kf.MessageContext, 10)
	pipeline := func(ctx context.Context, mc *kf.MessageContext) error {
		received <- mc
		return nil
	}

	cm, err := consumer.NewConsumerManager(consumer.ConsumerConfig{
		Name:        "test-consumer",
		GroupID:     testGroup,
		Topics:      []string{testTopic},
		Brokers:     testBroker,
		WorkerCount: 2,
		BufferSize:  100,
		Strategy:    distribution.NewFreeWorker(),
		Pipeline:    pipeline,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		time.Sleep(10 * time.Second)
		cm.Stop()
	}()

	err = cm.Start(ctx)
	assert.NoError(t, err)

	close(received)
	count := 0
	for range received {
		count++
	}
	assert.GreaterOrEqual(t, count, 10)
}

func produceTestMessages(t *testing.T, topic string, count int) {
	t.Helper()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": testBroker,
	})
	require.NoError(t, err)
	defer p.Close()

	for i := 0; i < count; i++ {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("test-message"),
		}, nil)
		require.NoError(t, err)
	}
	p.Flush(5000)
}
