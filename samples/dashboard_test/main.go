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
)

type TestMsg struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

type TestHandler struct{}

func (h *TestHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, msg TestMsg) error {
	fmt.Printf("Received: id=%s text=%s\n", msg.ID, msg.Text)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	bus, err := builder.NewConfig("dashboard-test").
		WithBrokers(brokers).
		WithConsumer("test-consumer", "test-group").
			WithTopics("test-topic").
			WithWorkers(2).
			WithMiddleware(
				middleware.Deserializer(jsonserializer.NewDeserializer()),
				middleware.TypedHandler[TestMsg](&TestHandler{}),
			).
			Done().
		WithProducer("test-producer").
			WithDefaultTopic("test-topic").
			WithMiddleware(
				middleware.Serializer(jsonserializer.NewSerializer()),
			).
			Done().
		EnableAdminMessages("kafkaflow-admin").
		EnableTelemetry("kafkaflow-telemetry").
		WithAdminAPI(":8080").
		Build()

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := bus.Start(ctx); err != nil {
			fmt.Printf("Bus stopped: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Dashboard test server started on :8080")
	fmt.Println("Dashboard: http://localhost:8080/kafkaflow/")
	fmt.Println("Admin API: http://localhost:8080/kafkaflow/groups")
	fmt.Println("Telemetry: http://localhost:8080/kafkaflow/consumers/telemetry")
	<-sigCh

	fmt.Println("Shutting down...")
	bus.Stop()
	cancel()
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
