package distribution_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

func TestPartitionKey_SingleWorker(t *testing.T) {
	s := distribution.NewPartitionKey()
	workers := []distribution.WorkerInfo{&mockWorker{id: 0, pendingCount: 5}}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Key: []byte("key")}}, workers)
	assert.Equal(t, 0, idx)
}

func TestPartitionKey_UsesKafkaPartitionModuloWorkerCount(t *testing.T) {
	s := distribution.NewPartitionKey()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 0},
		&mockWorker{id: 1, pendingCount: 0},
		&mockWorker{id: 2, pendingCount: 0},
	}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Partition: 5, Key: []byte("ignored")}}, workers)
	assert.Equal(t, 2, idx)
}

func TestPartitionKey_IgnoresMessageKey(t *testing.T) {
	s := distribution.NewPartitionKey()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 0},
		&mockWorker{id: 1, pendingCount: 0},
	}
	idx1 := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Partition: 1, Key: []byte("key-a")}}, workers)
	idx2 := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Partition: 1, Key: []byte("key-b")}}, workers)
	assert.Equal(t, idx1, idx2)
}
