package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
)

type SlowMessage struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

type SlowHandler struct {
	count int
}

func (h *SlowHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, msg SlowMessage) error {
	h.count++
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("[%d] Processed: %s\n", h.count, msg.ID)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	bus, err := builder.NewConfig("throttling-sample").
		WithBrokers(brokers).
		WithConsumer("slow-consumer", "slow-group").
		WithTopics("slow-topic").
		WithWorkers(2).
		WithBufferSize(5).
		WithMiddleware(
			middleware.Throttling(middleware.ThrottlingConfig{
				MaxPendingMessages: 10,
				CheckInterval:      100 * time.Millisecond,
			}),
			middleware.Deserializer(jsonserializer.NewDeserializer()),
			middleware.TypedHandler[SlowMessage](&SlowHandler{}),
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

	fmt.Println("Throttling sample started. Press Ctrl+C to stop.")
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
