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

type BatchMessage struct {
	BatchID string `json:"batch_id"`
	Data    string `json:"data"`
}

type BatchHandler struct {
	count int
}

func (h *BatchHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, msg BatchMessage) error {
	h.count++
	fmt.Printf("[%d] Batch message: %s\n", h.count, msg.BatchID)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	handler := &BatchHandler{}
	bus, err := builder.NewConfig("batch-sample").
		WithBrokers(brokers).
		WithConsumer("batch-consumer", "batch-group").
			WithTopics("batch-topic").
			WithWorkers(5).
			WithBufferSize(200).
			WithMiddleware(
				middleware.Deserializer(jsonserializer.NewDeserializer()),
				middleware.TypedHandler[BatchMessage](handler),
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

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("Processed %d messages so far\n", handler.count)
			}
		}
	}()

	fmt.Println("Batch consumer started. Press Ctrl+C to stop.")
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
