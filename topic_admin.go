package kafkaflow

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TopicConfig struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

func CreateTopicsIfNotExist(brokers string, security *SecurityConfig, topics []TopicConfig) error {
	cm := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}
	ApplySecurityConfig(cm, security)

	admin, err := kafka.NewAdminClient(cm)
	if err != nil {
		return fmt.Errorf("kafkaflow: failed to create admin client: %w", err)
	}
	defer admin.Close()

	ctx := context.Background()
	metadata, err := admin.GetMetadata(nil, true, 5000)
	if err != nil {
		return fmt.Errorf("kafkaflow: failed to get metadata: %w", err)
	}

	existingTopics := make(map[string]bool)
	for topic := range metadata.Topics {
		existingTopics[topic] = true
	}

	var specs []kafka.TopicSpecification
	for _, tc := range topics {
		if existingTopics[tc.Name] {
			continue
		}
		partitions := tc.NumPartitions
		if partitions <= 0 {
			partitions = 1
		}
		rf := tc.ReplicationFactor
		if rf <= 0 {
			rf = 1
		}
		specs = append(specs, kafka.TopicSpecification{
			Topic:             tc.Name,
			NumPartitions:     partitions,
			ReplicationFactor: rf,
		})
	}

	if len(specs) == 0 {
		return nil
	}

	results, err := admin.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("kafkaflow: failed to create topics: %w", err)
	}

	for _, res := range results {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("kafkaflow: failed to create topic %s: %s", res.Topic, res.Error)
		}
	}

	return nil
}
