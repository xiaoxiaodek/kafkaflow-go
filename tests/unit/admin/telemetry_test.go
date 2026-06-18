package admin_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
)

func TestMemoryTelemetryStorage_PutAndGet(t *testing.T) {
	s := admin.NewMemoryTelemetryStorage(5 * time.Minute)
	defer s.Stop()

	m1 := admin.ConsumerTelemetryMetric{
		GroupID:      "g1",
		ConsumerName: "c1",
		Topic:        "t1",
		InstanceName: "host-1",
		SentAt:       time.Now().UnixMilli(),
	}

	s.Put(m1)

	metrics := s.GetAll()
	assert.Len(t, metrics, 1)
	assert.Equal(t, "c1", metrics[0].ConsumerName)
}

func TestMemoryTelemetryStorage_Overwrite(t *testing.T) {
	s := admin.NewMemoryTelemetryStorage(5 * time.Minute)
	defer s.Stop()

	now := time.Now().UnixMilli()
	m1 := admin.ConsumerTelemetryMetric{
		GroupID:      "g1",
		ConsumerName: "c1",
		Topic:        "t1",
		InstanceName: "host-1",
		SentAt:       now,
		WorkersCount: 5,
	}
	m2 := admin.ConsumerTelemetryMetric{
		GroupID:      "g1",
		ConsumerName: "c1",
		Topic:        "t1",
		InstanceName: "host-1",
		SentAt:       now + 1000,
		WorkersCount: 10,
	}

	s.Put(m1)
	s.Put(m2)

	metrics := s.GetAll()
	assert.Len(t, metrics, 1)
	assert.Equal(t, 10, metrics[0].WorkersCount)
}
