package distribution

import kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"

// PartitionKey routes messages by Kafka partition, ensuring partition order affinity.
type PartitionKey struct{}

// NewPartitionKey creates a new PartitionKey strategy.
func NewPartitionKey() *PartitionKey {
	return &PartitionKey{}
}

// Route selects a worker by Kafka partition modulo worker count.
func (s *PartitionKey) Route(mc *kafkaflow.MessageContext, workers []WorkerInfo) int {
	if len(workers) == 0 {
		return 0
	}
	return int(mc.Message.Partition) % len(workers)
}
