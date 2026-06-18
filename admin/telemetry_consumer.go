package admin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// TelemetryConsumerService listens on the telemetry topic and stores metrics.
type TelemetryConsumerService struct {
	consumer *kafka.Consumer
	handler  *TelemetryHandler
	logger   log.Logger
}

// NewTelemetryConsumerService creates a new telemetry consumer subscribed to the telemetry topic.
func NewTelemetryConsumerService(brokers string, topic string, groupID string, handler *TelemetryHandler, logger log.Logger) (*TelemetryConsumerService, error) {
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

	if err := c.Subscribe(topic, nil); err != nil {
		c.Close()
		return nil, err
	}

	return &TelemetryConsumerService{
		consumer: c,
		handler:  handler,
		logger:   logger,
	}, nil
}

// Start begins consuming telemetry messages. Blocks until ctx is cancelled.
func (tcs *TelemetryConsumerService) Start(ctx context.Context) error {
	tcs.logger.Info(ctx, "telemetry consumer started")
	defer tcs.consumer.Close()

	for {
		select {
		case <-ctx.Done():
			tcs.logger.Info(ctx, "telemetry consumer stopping")
			return nil
		default:
		}

		ev := tcs.consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			mc := &kafkaflow.MessageContext{
				Message: &kafkaflow.Message{Value: e.Value},
			}
			if err := tcs.handler.HandleMessage(ctx, mc); err != nil {
				tcs.logger.Warn(ctx, "failed to handle telemetry metric", "error", err)
			}

		case kafka.Error:
			tcs.logger.Error(ctx, "telemetry consumer error", "error", e.Error())
			if e.IsFatal() {
				return e
			}
		}
	}
}
