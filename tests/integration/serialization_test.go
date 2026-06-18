//go:build integration
// +build integration

package integration

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
)

type testPayload struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestSerializationRoundTrip(t *testing.T) {
	topic := testTopic + "-serialization"

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": testBroker})
	require.NoError(t, err)
	defer p.Close()

	payload := testPayload{ID: "123", Name: "Test"}
	data, _ := jsonserializer.NewSerializer().Serialize(payload)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)
	require.NoError(t, err)
	p.Flush(5000)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": testBroker,
		"group.id":          testGroup + "-serialization-test",
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer c.Close()

	err = c.Subscribe(topic, nil)
	require.NoError(t, err)

	msg, err := c.ReadMessage(10 * time.Second)
	require.NoError(t, err)

	var result testPayload
	err = jsonserializer.NewDeserializer().Deserialize(msg.Value, &result)
	require.NoError(t, err)
	assert.Equal(t, "123", result.ID)
	assert.Equal(t, "Test", result.Name)
}
