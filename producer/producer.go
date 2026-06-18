package producer

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

// ProducerConfig holds configuration for a producer.
type ProducerConfig struct {
	Name            string
	Brokers         string
	Topic           string
	Security        *kafkaflow.SecurityConfig
	CompressionType string
	Acks            string
}

// MessageProducer wraps a Kafka producer with a middleware pipeline.
type MessageProducer struct {
	producer *kafka.Producer
	pipeline kafkaflow.Handler
	topic    string
}

// NewMessageProducer creates a new MessageProducer.
func NewMessageProducer(cfg ProducerConfig, pipeline kafkaflow.Handler) (*MessageProducer, error) {
	cm := &kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
	}
	if cfg.CompressionType != "" {
		cm.SetKey("compression.type", cfg.CompressionType)
	}
	if cfg.Acks != "" {
		cm.SetKey("acks", cfg.Acks)
	}
	kafkaflow.ApplySecurityConfig(cm, cfg.Security)

	p, err := kafka.NewProducer(cm)
	if err != nil {
		return nil, err
	}

	return &MessageProducer{
		producer: p,
		pipeline: pipeline,
		topic:    cfg.Topic,
	}, nil
}

// ProduceResult holds the result of an async produce operation.
type ProduceResult struct {
	Partition int32
	Offset    int64
	Err       error
}

// Produce sends a message through the middleware pipeline and then to Kafka.
func (p *MessageProducer) Produce(ctx context.Context, key []byte, value interface{}, headers ...kafkaflow.Header) error {
	mc := &kafkaflow.MessageContext{
		ProducerContext: &kafkaflow.ProducerContext{
			Topic:        p.topic,
			PartitionKey: key,
		},
		Message: &kafkaflow.Message{
			Key:     key,
			Value:   normalizeValue(value),
			Headers: headers,
		},
		Items: make(map[string]any),
	}

	if err := p.pipeline(ctx, mc); err != nil {
		return err
	}

	kafkaHeaders := make([]kafka.Header, len(mc.Message.Headers))
	for i, h := range mc.Message.Headers {
		kafkaHeaders[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}

	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            mc.Message.Key,
		Value:          valueBytes(mc.Message.Value),
		Headers:        kafkaHeaders,
	}, nil)
}

// ProduceAsync sends a message asynchronously and returns a channel for the result.
func (p *MessageProducer) ProduceAsync(ctx context.Context, key []byte, value interface{}, headers ...kafkaflow.Header) <-chan ProduceResult {
	resultCh := make(chan ProduceResult, 1)

	mc := &kafkaflow.MessageContext{
		ProducerContext: &kafkaflow.ProducerContext{
			Topic:        p.topic,
			PartitionKey: key,
		},
		Message: &kafkaflow.Message{
			Key:     key,
			Value:   normalizeValue(value),
			Headers: headers,
		},
		Items: make(map[string]any),
	}

	if err := p.pipeline(ctx, mc); err != nil {
		resultCh <- ProduceResult{Err: err}
		close(resultCh)
		return resultCh
	}

	kafkaHeaders := make([]kafka.Header, len(mc.Message.Headers))
	for i, h := range mc.Message.Headers {
		kafkaHeaders[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}

	go func() {
		defer close(resultCh)

		deliveryCh := make(chan kafka.Event, 1)
		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
			Key:            mc.Message.Key,
			Value:          valueBytes(mc.Message.Value),
			Headers:        kafkaHeaders,
		}, deliveryCh)

		if err != nil {
			resultCh <- ProduceResult{Err: err}
			return
		}

		e := <-deliveryCh
		switch ev := e.(type) {
		case *kafka.Message:
			resultCh <- ProduceResult{
				Partition: ev.TopicPartition.Partition,
				Offset:    int64(ev.TopicPartition.Offset),
			}
		case kafka.Error:
			resultCh <- ProduceResult{Err: ev}
		}
	}()

	return resultCh
}

// Close flushes and closes the underlying Kafka producer.
func (p *MessageProducer) Close() {
	p.producer.Flush(10 * 1000)
	p.producer.Close()
}

func normalizeValue(value interface{}) interface{} {
	return value
}

func valueBytes(value interface{}) []byte {
	if value == nil {
		return nil
	}
	if data, ok := value.([]byte); ok {
		return data
	}
	return []byte(fmt.Sprint(value))
}
