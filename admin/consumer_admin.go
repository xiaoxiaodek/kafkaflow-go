package admin

import (
	"context"
)

// ConsumerAdmin provides the public API for consumer administration.
type ConsumerAdmin struct {
	producer *AdminProducer
}

// NewConsumerAdmin creates a new ConsumerAdmin.
func NewConsumerAdmin(producer *AdminProducer) *ConsumerAdmin {
	return &ConsumerAdmin{producer: producer}
}

func (ca *ConsumerAdmin) PauseConsumer(ctx context.Context, name string, topics []string) error {
	return ca.producer.Produce(ctx, &PauseConsumerByName{ConsumerName: name, Topics: topics})
}

func (ca *ConsumerAdmin) ResumeConsumer(ctx context.Context, name string, topics []string) error {
	return ca.producer.Produce(ctx, &ResumeConsumerByName{ConsumerName: name, Topics: topics})
}

func (ca *ConsumerAdmin) StartConsumer(ctx context.Context, name string) error {
	return ca.producer.Produce(ctx, &StartConsumerByName{ConsumerName: name})
}

func (ca *ConsumerAdmin) StopConsumer(ctx context.Context, name string) error {
	return ca.producer.Produce(ctx, &StopConsumerByName{ConsumerName: name})
}

func (ca *ConsumerAdmin) RestartConsumer(ctx context.Context, name string) error {
	return ca.producer.Produce(ctx, &RestartConsumerByName{ConsumerName: name})
}

func (ca *ConsumerAdmin) ResetConsumerOffset(ctx context.Context, name string, topics []string) error {
	return ca.producer.Produce(ctx, &ResetConsumerOffset{ConsumerName: name, Topics: topics})
}

func (ca *ConsumerAdmin) RewindConsumerOffset(ctx context.Context, name string, timestamp int64, topics []string) error {
	return ca.producer.Produce(ctx, &RewindConsumerOffsetToDateTime{
		ConsumerName: name,
		Timestamp:    timestamp,
		Topics:       topics,
	})
}

func (ca *ConsumerAdmin) ChangeWorkersCount(ctx context.Context, name string, count int) error {
	return ca.producer.Produce(ctx, &ChangeConsumerWorkersCount{ConsumerName: name, WorkersCount: count})
}

func (ca *ConsumerAdmin) PauseGroup(ctx context.Context, groupID string, topics []string) error {
	return ca.producer.Produce(ctx, &PauseConsumersByGroup{GroupID: groupID, Topics: topics})
}

func (ca *ConsumerAdmin) ResumeGroup(ctx context.Context, groupID string, topics []string) error {
	return ca.producer.Produce(ctx, &ResumeConsumersByGroup{GroupID: groupID, Topics: topics})
}
