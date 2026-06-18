package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
)

type testBatchHandler struct {
	batches [][]middleware.BatchMessage[string]
}

func (h *testBatchHandler) HandleBatch(ctx context.Context, msgs []middleware.BatchMessage[string]) error {
	h.batches = append(h.batches, msgs)
	return nil
}

func TestBatchConsume_FlushesOnSize(t *testing.T) {
	handler := &testBatchHandler{}
	mw := middleware.BatchConsume[string](handler, middleware.BatchConsumeConfig{
		BatchSize:   3,
		MaxWaitTime: time.Hour,
	})

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)

	for i := 0; i < 3; i++ {
		mc := &kafkaflow.MessageContext{
			Message: &kafkaflow.Message{},
			Items:   map[string]any{},
		}
		mc.SetItem(middleware.BatchPayloadKey, "msg")
		err := pipeline(context.Background(), mc)
		assert.NoError(t, err)
	}

	assert.Len(t, handler.batches, 1)
	assert.Len(t, handler.batches[0], 3)
}

func TestBatchConsume_FlushesOnTimeout(t *testing.T) {
	handler := &testBatchHandler{}
	mw := middleware.BatchConsume[string](handler, middleware.BatchConsumeConfig{
		BatchSize:   100,
		MaxWaitTime: 50 * time.Millisecond,
	})

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)

	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{},
		Items:   map[string]any{},
	}
	mc.SetItem(middleware.BatchPayloadKey, "msg")
	err := pipeline(context.Background(), mc)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mc2 := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{},
		Items:   map[string]any{},
	}
	mc2.SetItem(middleware.BatchPayloadKey, "msg2")
	err = pipeline(context.Background(), mc2)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, len(handler.batches), 1)
}
