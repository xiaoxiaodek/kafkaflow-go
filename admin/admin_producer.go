package admin

import (
	"context"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// AdminProducer sends admin messages to the admin Kafka topic.
type AdminProducer struct {
	producer  *kafka.Producer
	topic     string
	partition int32
}

// NewAdminProducer creates a new AdminProducer.
func NewAdminProducer(brokers string, topic string) (*AdminProducer, error) {
	return NewAdminProducerWithPartition(brokers, topic, 0)
}

// NewAdminProducerWithPartition creates a new AdminProducer that writes commands to a fixed partition.
func NewAdminProducerWithPartition(brokers string, topic string, partition int32) (*AdminProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return nil, err
	}

	return &AdminProducer{
		producer:  p,
		topic:     topic,
		partition: partition,
	}, nil
}

// Produce sends an admin message to the admin topic.
func (ap *AdminProducer) Produce(ctx context.Context, msg AdminMessage) error {
	kafkaMsg, err := buildAdminKafkaMessage(ap.topic, ap.partition, msg)
	if err != nil {
		return err
	}
	return ap.producer.Produce(kafkaMsg, nil)
}

// ProduceTelemetry sends a telemetry metric to the telemetry topic.
func (ap *AdminProducer) ProduceTelemetry(ctx context.Context, topic string, metric ConsumerTelemetryMetric) error {
	data, err := json.Marshal(metric)
	if err != nil {
		return err
	}

	return ap.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil)
}

// Close flushes and closes the producer.
func (ap *AdminProducer) Close() {
	ap.producer.Flush(5000)
	ap.producer.Close()
}

func buildAdminKafkaMessage(topic string, partition int32, msg AdminMessage) (*kafka.Message, error) {
	data, err := encodeAdminMessage(msg)
	if err != nil {
		return nil, err
	}
	msgType := getMessageType(msg)
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Value:          data,
		Headers:        []kafka.Header{{Key: "message-type", Value: []byte(msgType)}},
	}, nil
}

func getMessageType(msg AdminMessage) string {
	switch msg.(type) {
	case *PauseConsumerByName:
		return "PauseConsumerByName"
	case *ResumeConsumerByName:
		return "ResumeConsumerByName"
	case *StartConsumerByName:
		return "StartConsumerByName"
	case *StopConsumerByName:
		return "StopConsumerByName"
	case *RestartConsumerByName:
		return "RestartConsumerByName"
	case *ResetConsumerOffset:
		return "ResetConsumerOffset"
	case *RewindConsumerOffsetToDateTime:
		return "RewindConsumerOffsetToDateTime"
	case *ChangeConsumerWorkersCount:
		return "ChangeConsumerWorkersCount"
	case *PauseConsumersByGroup:
		return "PauseConsumersByGroup"
	case *ResumeConsumersByGroup:
		return "ResumeConsumersByGroup"
	default:
		return "Unknown"
	}
}
