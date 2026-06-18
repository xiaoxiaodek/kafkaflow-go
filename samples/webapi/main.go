package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
	jsonserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/json"
)

type APIEvent struct {
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
}

type APIEventHandler struct{}

func (h *APIEventHandler) Handle(ctx context.Context, mc *kafkaflow.MessageContext, event APIEvent) error {
	fmt.Printf("API Event: type=%s payload=%s\n", event.EventType, event.Payload)
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")

	bus, err := builder.NewConfig("webapi-sample").
		WithBrokers(brokers).
		WithConsumer("api-consumer", "api-group").
			WithTopics("api-events").
			WithWorkers(5).
			WithMiddleware(
				middleware.Deserializer(jsonserializer.NewDeserializer()),
				middleware.TypedHandler[APIEvent](&APIEventHandler{}),
			).
			Done().
		WithProducer("api-producer").
			WithDefaultTopic("api-events").
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

	go func() {
		if err := bus.Start(ctx); err != nil {
			fmt.Printf("Bus stopped: %v\n", err)
		}
	}()

	r := gin.Default()

	r.POST("/events", func(c *gin.Context) {
		var event APIEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		producer, err := bus.ProducerAccessor().Get("api-producer")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if err := producer.Produce(ctx, []byte(event.EventType), event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "published"})
	})

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("Shutting down...")
		bus.Stop()
		cancel()
	}()

	fmt.Println("WebAPI server started on :8080")
	r.Run(":8080")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
