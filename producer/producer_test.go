package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

func TestMessageProducer_PipelineReceivesProduceValue(t *testing.T) {
	expectedErr := errors.New("stop before kafka produce")
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		assert.Equal(t, []byte("key-1"), mc.Message.Key)
		assert.Equal(t, []byte("payload"), mc.Message.Value)
		return expectedErr
	}

	mp := &MessageProducer{topic: "orders", pipeline: pipeline}
	err := mp.Produce(context.Background(), []byte("key-1"), []byte("payload"))

	require.ErrorIs(t, err, expectedErr)
}

func TestMessageProducer_PipelineReceivesTypedProduceValue(t *testing.T) {
	type order struct {
		ID string
	}

	expectedErr := errors.New("stop before kafka produce")
	value := order{ID: "order-1"}
	pipeline := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		assert.Equal(t, value, mc.Message.Value)
		return expectedErr
	}

	mp := &MessageProducer{topic: "orders", pipeline: pipeline}
	err := mp.Produce(context.Background(), []byte("key-1"), value)

	require.ErrorIs(t, err, expectedErr)
}
