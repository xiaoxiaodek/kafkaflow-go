package admin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

type mockRegistry struct {
	consumers map[string]*consumer.ConsumerManager
}

func (r *mockRegistry) GetByName(name string) (*consumer.ConsumerManager, bool) {
	cm, ok := r.consumers[name]
	return cm, ok
}

func (r *mockRegistry) GetByGroup(groupID string) []*consumer.ConsumerManager {
	return nil
}

func (r *mockRegistry) AllConsumers() []*consumer.ConsumerManager {
	return nil
}

func TestAdminHandler_UnknownMessageType(t *testing.T) {
	handler := admin.NewAdminHandler(&mockRegistry{}, log.DefaultLogger())

	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{
			Value:   []byte(`garbage`),
			Headers: []kafkaflow.Header{{Key: "message-type", Value: []byte("UnknownType")}},
		},
		Items: make(map[string]any),
	}

	err := handler.HandleMessage(context.Background(), mc)
	assert.NoError(t, err)
}

func TestAdminHandler_PauseConsumerByName_ConsumerNotFound(t *testing.T) {
	handler := admin.NewAdminHandler(&mockRegistry{consumers: map[string]*consumer.ConsumerManager{}}, log.DefaultLogger())

	msg := admin.PauseConsumerByName{ConsumerName: "nonexistent", Topics: []string{"t1"}}
	data, _ := admin.EncodeAdminMessage(&msg)

	mc := &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{
			Value:   data,
			Headers: []kafkaflow.Header{{Key: "message-type", Value: []byte("PauseConsumerByName")}},
		},
		Items: make(map[string]any),
	}

	err := handler.HandleMessage(context.Background(), mc)
	assert.NoError(t, err)
}
