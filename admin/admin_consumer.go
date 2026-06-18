package admin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// AdminConsumer listens on the admin topic and dispatches commands to the handler.
type AdminConsumer struct {
	consumer *kafka.Consumer
	handler  *AdminHandler
	logger   log.Logger
}

// NewAdminConsumer creates a new AdminConsumer assigned to partition 0.
func NewAdminConsumer(brokers string, topic string, groupID string, handler *AdminHandler, logger log.Logger) (*AdminConsumer, error) {
	return NewAdminConsumerWithPartition(brokers, topic, groupID, 0, handler, logger)
}

// NewAdminConsumerWithPartition creates a new AdminConsumer assigned to a fixed partition.
func NewAdminConsumerWithPartition(brokers string, topic string, groupID string, partition int32, handler *AdminHandler, logger log.Logger) (*AdminConsumer, error) {
	if logger == nil {
		logger = log.DefaultLogger()
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": true,
	})
	if err != nil {
		return nil, err
	}

	if err := c.Assign(buildAdminConsumerAssignment(topic, partition)); err != nil {
		c.Close()
		return nil, err
	}

	return &AdminConsumer{
		consumer: c,
		handler:  handler,
		logger:   logger,
	}, nil
}

// Start begins consuming admin messages. Blocks until ctx is cancelled.
func (ac *AdminConsumer) Start(ctx context.Context) error {
	ac.logger.Info(ctx, "admin consumer started")
	defer ac.consumer.Close()

	for {
		select {
		case <-ctx.Done():
			ac.logger.Info(ctx, "admin consumer stopping")
			return nil
		default:
		}

		ev := ac.consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			mc := &kafkaflow.MessageContext{
				Message: &kafkaflow.Message{
					Key:     e.Key,
					Value:   e.Value,
					Headers: convertAdminHeaders(e.Headers),
				},
				Items: make(map[string]any),
			}

			if err := ac.handler.HandleMessage(ctx, mc); err != nil {
				ac.logger.Error(ctx, "failed to handle admin message", "error", err)
			}

		case kafka.Error:
			ac.logger.Error(ctx, "admin consumer error", "error", e.Error())
			if e.IsFatal() {
				return e
			}
		}
	}
}

func buildAdminConsumerAssignment(topic string, partition int32) []kafka.TopicPartition {
	return []kafka.TopicPartition{{Topic: &topic, Partition: partition}}
}

func convertAdminHeaders(headers []kafka.Header) []kafkaflow.Header {
	result := make([]kafkaflow.Header, len(headers))
	for i, h := range headers {
		result[i] = kafkaflow.Header{Key: h.Key, Value: h.Value}
	}
	return result
}
