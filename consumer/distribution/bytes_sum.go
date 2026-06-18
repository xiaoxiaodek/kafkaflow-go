package distribution

import kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"

// BytesSum routes messages by hashing the message key and distributing
// based on the sum of bytes, preferring workers with fewer pending messages.
type BytesSum struct{}

// NewBytesSum creates a new BytesSum strategy.
func NewBytesSum() *BytesSum {
	return &BytesSum{}
}

// Route selects a worker by summing key bytes and applying modulo worker count.
func (s *BytesSum) Route(mc *kafkaflow.MessageContext, workers []WorkerInfo) int {
	if len(workers) == 0 {
		return 0
	}

	sum := 0
	for _, b := range mc.Message.Key {
		sum += int(b)
	}
	return sum % len(workers)
}
