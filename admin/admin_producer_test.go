package admin

import "testing"

func TestBuildAdminKafkaMessageUsesFixedPartition(t *testing.T) {
	msg, err := buildAdminKafkaMessage("admin-topic", 0, &PauseConsumerByName{ConsumerName: "orders"})
	if err != nil {
		t.Fatalf("build admin message: %v", err)
	}
	if msg.TopicPartition.Partition != 0 {
		t.Fatalf("expected fixed partition 0, got %d", msg.TopicPartition.Partition)
	}
}

func TestBuildAdminKafkaMessageSetsTopic(t *testing.T) {
	msg, err := buildAdminKafkaMessage("admin-topic", 2, &PauseConsumerByName{ConsumerName: "orders"})
	if err != nil {
		t.Fatalf("build admin message: %v", err)
	}
	if msg.TopicPartition.Topic == nil || *msg.TopicPartition.Topic != "admin-topic" {
		t.Fatalf("expected admin-topic, got %v", msg.TopicPartition.Topic)
	}
	if msg.TopicPartition.Partition != 2 {
		t.Fatalf("expected fixed partition 2, got %d", msg.TopicPartition.Partition)
	}
}
