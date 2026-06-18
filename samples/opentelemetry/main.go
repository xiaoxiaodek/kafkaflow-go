package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
	"github.com/xiaoxiaodek/kafkaflow-go/telemetry"
)

type Event struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type EventHandler struct{}

func (h *EventHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, event Event) error {
	fmt.Printf("Received event: id=%s data=%s\n", event.ID, event.Data)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	bus, err := builder.NewConfig("otel-sample").
		WithBrokers(brokers).
		WithConsumer("events-consumer", "events-group").
		WithTopics("events").
		WithWorkers(5).
		WithMiddleware(
			telemetry.ConsumerTracingMiddleware(),
			middleware.Deserializer(jsonserializer.NewDeserializer()),
			middleware.TypedHandler[Event](&EventHandler{}),
		).
		Done().
		WithProducer("events-producer").
		WithDefaultTopic("events").
		WithMiddleware(
			telemetry.ProducerTracingMiddleware(),
			middleware.Serializer(jsonserializer.NewSerializer()),
		).
		Done().
		Build()

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("Shutting down...")
		bus.Stop()
		cancel()
	}()

	producer, err := bus.ProducerAccessor().Get("events-producer")
	if err != nil {
		panic(err)
	}

	event := Event{ID: "evt-1", Data: "hello from otel"}
	if err := producer.Produce(ctx, []byte(event.ID), event); err != nil {
		fmt.Printf("Failed to produce: %v\n", err)
	}

	fmt.Println("OpenTelemetry sample started. Press Ctrl+C to stop.")
	if err := bus.Start(ctx); err != nil {
		fmt.Printf("Bus stopped: %v\n", err)
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
