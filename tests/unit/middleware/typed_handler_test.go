package middleware_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
)

type Order struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type orderHandler struct {
	received *Order
}

func (h *orderHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, order Order) error {
	h.received = &order
	return nil
}

func TestTypedHandlerMiddleware(t *testing.T) {
	handler := &orderHandler{}
	mw := middleware.TypedHandler[Order](handler)

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		return nil
	}

	orderJSON, _ := json.Marshal(Order{ID: "123", Name: "Test Order"})
	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: orderJSON},
	}

	err := pipeline(context.Background(), mc)
	require.NoError(t, err)
	assert.NotNil(t, handler.received)
	assert.Equal(t, "123", handler.received.ID)
	assert.Equal(t, "Test Order", handler.received.Name)
}

func TestTypedHandlerMiddleware_InvalidJSON(t *testing.T) {
	handler := &orderHandler{}
	mw := middleware.TypedHandler[Order](handler)

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: []byte("not json")},
	}

	err := pipeline(context.Background(), mc)
	assert.Error(t, err)
}
