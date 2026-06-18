package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/telemetry"
)

func TestConsumerTracingMiddleware_NoConsumerContext(t *testing.T) {
	mw := telemetry.ConsumerTracingMiddleware()

	called := false
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		called = true
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: []byte("test")},
	}

	err := pipeline(context.Background(), mc)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestProducerTracingMiddleware_NoProducerContext(t *testing.T) {
	mw := telemetry.ProducerTracingMiddleware()

	called := false
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		called = true
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: []byte("test")},
	}

	err := pipeline(context.Background(), mc)
	assert.NoError(t, err)
	assert.True(t, called)
}
