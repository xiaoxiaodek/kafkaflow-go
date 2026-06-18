# KafkaFlow Go

A high-level Go framework for building applications on top of Apache Kafka, inspired by [KafkaFlow](https://github.com/Farfetch/kafkaflow).

## Features

- **Middleware pipeline** for producing and consuming messages
- **Multi-worker consumer** with message order guarantee via partition-key affinity
- **Fluent configuration builder** API
- **JSON, Protobuf, Avro, and Schema Registry serialization**
- **Gzip compression**
- **Typed message handlers** with Go generics
- **Dependency injection** via google/wire
- **Admin API** for pause/resume/restart, offset reset/rewind, worker count changes
- **Telemetry storage and scheduler**
- **OpenTelemetry tracing middleware**
- **Batch consume middleware**
- **Consumer throttling middleware**
- **Dynamic worker balancer**
- **Graceful shutdown** with offset storage guarantee
- **Distribution strategies**: BytesSum, FreeWorker, PartitionKey

## Installation

```bash
go get github.com/xiaoxiaodek/kafkaflow-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"

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
    fmt.Printf("Received order: %s\n", order.ID)
    return nil
}

func main() {
    bus, _ := builder.NewConfig("my-app").
        WithBrokers("localhost:9092").
        WithConsumer("orders", "order-group").
            WithTopics("order-topic").
            WithWorkers(10).
            WithBufferSize(100).
            WithDistribution(distribution.NewPartitionKey()).
            WithMiddleware(
                middleware.Deserializer(jsonserializer.NewDeserializer()),
                middleware.TypedHandler[Order](&OrderHandler{}),
            ).
            Done().
        WithProducer("orders-producer").
            WithDefaultTopic("order-topic").
            WithMiddleware(
                middleware.Serializer(jsonserializer.NewSerializer()),
            ).
            Done().
        EnableAdminMessages("kafkaflow-admin").
        EnableTelemetry("kafkaflow-telemetry").
        WithAdminAPI(":8080").
        Build()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        sigCh := make(chan os.Signal, 1)
        signal.Notify(sigCh, os.Interrupt)
        <-sigCh
        bus.Stop()
        cancel()
    }()

    bus.Start(ctx)
}
```

## Serializers

- `serializer/json` — JSON via `encoding/json`
- `serializer/protobuf` — Protobuf via `google.golang.org/protobuf`
- `serializer/avro` — Avro via `github.com/hamba/avro/v2`
- `serializer/schemaregistry` — Confluent Schema Registry Avro/Protobuf wire format

## Admin API

When `WithAdminAPI(":8080")` is enabled:

- `GET /kafkaflow/groups`
- `POST /kafkaflow/groups/:groupId/pause`
- `POST /kafkaflow/groups/:groupId/resume`
- `GET /kafkaflow/groups/:groupId/consumers`
- `GET /kafkaflow/groups/:groupId/consumers/:consumerName`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/pause`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/resume`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/start`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/stop`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/restart`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/reset-offsets`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/rewind-offsets-to-date`
- `POST /kafkaflow/groups/:groupId/consumers/:consumerName/change-worker-count`
- `GET /kafkaflow/telemetry`

## Samples

- `samples/simple`
- `samples/batch`
- `samples/webapi`
- `samples/schema_registry`
- `samples/opentelemetry`
- `samples/throttling`

## Running Tests

```bash
make test-unit
make init-broker
make test-integration
make shutdown-broker
```

## License

MIT
