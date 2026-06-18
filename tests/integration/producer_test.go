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
	"github.com/xiaoxiaodek/kafkaflow-go/producer"
)

func TestMessageProducer_ProduceAndConsume(t *testing.T) {
	topic := testTopic + "-producer"

	pipeline := func(ctx context.Context, mc *kf.MessageContext) error {
		return nil
	}

	mp, err := producer.NewMessageProducer(producer.ProducerConfig{
		Name:    "test-producer",
		Brokers: testBroker,
		Topic:   topic,
	}, pipeline)
	require.NoError(t, err)
	defer mp.Close()

	err = mp.Produce(context.Background(), []byte("key-1"), "hello world")
	require.NoError(t, err)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": testBroker,
		"group.id":          testGroup + "-producer-test",
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer c.Close()

	err = c.Subscribe(topic, nil)
	require.NoError(t, err)

	msg, err := c.ReadMessage(10 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(msg.Value))
}
