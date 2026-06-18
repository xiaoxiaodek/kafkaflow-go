package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/xiaoxiaodek/kafkaflow-go/builder"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
	"github.com/xiaoxiaodek/kafkaflow-go/schemaregistry"
	srserializer "github.com/xiaoxiaodek/kafkaflow-go/serializer/schemaregistry"
)

const avroSchema = `{
	"type": "record",
	"name": "User",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "email", "type": "string"}
	]
}`

type User struct {
	ID    string `avro:"id"`
	Name  string `avro:"name"`
	Email string `avro:"email"`
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	schemaRegistryURL := getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	client, err := schemaregistry.NewClient(schemaRegistryURL)
	if err != nil {
		panic(fmt.Sprintf("failed to create schema registry client: %v", err))
	}

	serializer, err := srserializer.NewAvroSerializer(client, "users-value", avroSchema)
	if err != nil {
		panic(fmt.Sprintf("failed to create avro serializer: %v", err))
	}

	deserializer := srserializer.NewAvroDeserializer(client)

	bus, err := builder.NewConfig("schema-registry-sample").
		WithBrokers(brokers).
		WithConsumer("users-consumer", "users-group").
			WithTopics("users").
			WithWorkers(5).
			WithMiddleware(
				middleware.Deserializer(deserializer),
			).
			Done().
		WithProducer("users-producer").
			WithDefaultTopic("users").
			WithMiddleware(
				middleware.Serializer(serializer),
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

	producer, err := bus.ProducerAccessor().Get("users-producer")
	if err != nil {
		panic(err)
	}

	user := User{ID: "user-1", Name: "Alice", Email: "alice@example.com"}
	if err := producer.Produce(ctx, []byte(user.ID), user); err != nil {
		fmt.Printf("Failed to produce: %v\n", err)
	}

	fmt.Println("Schema Registry sample started. Press Ctrl+C to stop.")
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
