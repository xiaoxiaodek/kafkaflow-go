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

type testSerializer struct{}

func (s *testSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

type testDeserializer struct{}

func (d *testDeserializer) Deserialize(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

func TestSerializerMiddleware(t *testing.T) {
	s := &testSerializer{}
	mw := middleware.Serializer(s)

	captured := make([]byte, 0)
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		captured = mc.Message.Value.([]byte)
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	original := []byte(`{"key":"value"}`)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: original},
	}

	err := pipeline(context.Background(), mc)
	require.NoError(t, err)
	assert.NotEmpty(t, captured)
	assert.NotEqual(t, original, captured, "serialized data should differ from raw input")
}

func TestDeserializerMiddleware(t *testing.T) {
	d := &testDeserializer{}
	mw := middleware.Deserializer(d)

	var captured interface{}
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		captured = mc.Message.Value
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: []byte(`{"key":"value"}`)},
	}

	err := pipeline(context.Background(), mc)
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"key": "value"}, captured)
}
