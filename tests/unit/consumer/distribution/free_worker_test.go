package distribution_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

func TestFreeWorker_SingleWorker(t *testing.T) {
	s := distribution.NewFreeWorker()
	workers := []distribution.WorkerInfo{&mockWorker{id: 0, pendingCount: 5}}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{}}, workers)
	assert.Equal(t, 0, idx)
}

func TestFreeWorker_ChoosesLeastPending(t *testing.T) {
	s := distribution.NewFreeWorker()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 10},
		&mockWorker{id: 1, pendingCount: 3},
		&mockWorker{id: 2, pendingCount: 7},
	}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{}}, workers)
	assert.Equal(t, 1, idx)
}

func TestFreeWorker_FirstWhenAllEqual(t *testing.T) {
	s := distribution.NewFreeWorker()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 5},
		&mockWorker{id: 1, pendingCount: 5},
	}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{}}, workers)
	assert.Equal(t, 0, idx)
}
