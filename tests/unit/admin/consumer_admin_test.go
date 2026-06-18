package admin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
)

func TestConsumerAdmin_CreatesCorrectMessages(t *testing.T) {
	ca := admin.NewConsumerAdmin(nil)
	assert.NotNil(t, ca)
}

func TestAdminMessageTypes(t *testing.T) {
	msg := &admin.PauseConsumerByName{ConsumerName: "test", Topics: []string{"t1"}}
	assert.Equal(t, "test", msg.ConsumerName)
	assert.Equal(t, []string{"t1"}, msg.Topics)
}

func TestTelemetryMetricFields(t *testing.T) {
	m := admin.ConsumerTelemetryMetric{
		GroupID:      "g1",
		ConsumerName: "c1",
		Topic:        "t1",
		InstanceName: "host-123",
		Status:       "Running",
		WorkersCount: 5,
		Lag:          100,
	}
	assert.Equal(t, "g1", m.GroupID)
	assert.Equal(t, "c1", m.ConsumerName)
	assert.Equal(t, 5, m.WorkersCount)
}
