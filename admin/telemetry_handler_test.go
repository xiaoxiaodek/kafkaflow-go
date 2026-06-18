package admin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

func TestTelemetryHandlerStoresMetric(t *testing.T) {
	storage := NewMemoryTelemetryStorage(5 * time.Minute)
	defer storage.Stop()
	handler := NewTelemetryHandler(storage)
	metric := ConsumerTelemetryMetric{
		GroupID:      "group-1",
		ConsumerName: "consumer-1",
		Topic:        "orders",
		InstanceName: "host-1",
		SentAt:       time.Now().UnixMilli(),
		WorkersCount: 3,
	}
	data, err := json.Marshal(metric)
	if err != nil {
		t.Fatalf("marshal metric: %v", err)
	}

	err = handler.HandleMessage(context.Background(), &kafkaflow.MessageContext{
		Message: &kafkaflow.Message{Value: data},
	})

	if err != nil {
		t.Fatalf("handle telemetry: %v", err)
	}
	metrics := storage.GetAll()
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].ConsumerName != "consumer-1" {
		t.Fatalf("expected consumer-1, got %s", metrics[0].ConsumerName)
	}
}
