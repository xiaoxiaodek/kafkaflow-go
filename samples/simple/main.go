package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
)

type Order struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type OrderHandler struct{}

func (h *OrderHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, order Order) error {
	fmt.Printf("Received order: id=%s name=%s offset=%d partition=%d\n",
		order.ID, order.Name, mc.ConsumerContext.Offset, mc.ConsumerContext.Partition)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	bus, err := builder.NewConfig("simple-sample").
		WithBrokers(brokers).
		WithConsumer("orders-consumer", "order-group").
			WithTopics("orders").
			WithWorkers(10).
			WithBufferSize(100).
			WithDistribution(distribution.NewPartitionKey()).
			WithMiddleware(
				middleware.Deserializer(jsonserializer.NewDeserializer()),
				middleware.TypedHandler[Order](&OrderHandler{}),
			).
			Done().
		WithProducer("orders-producer").
			WithDefaultTopic("orders").
			WithMiddleware(
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

	producer, err := bus.ProducerAccessor().Get("orders-producer")
	if err != nil {
		panic(err)
	}

	order := Order{ID: "order-1", Name: "First Order"}
	orderJSON, _ := json.Marshal(order)
	producer.Produce(ctx, []byte(order.ID), orderJSON)

	fmt.Println("KafkaFlow bus started. Press Ctrl+C to stop.")
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
