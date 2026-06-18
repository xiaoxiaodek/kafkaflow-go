package distribution_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
)

type mockWorker struct {
	id           int
	pendingCount int
}

func (w *mockWorker) ID() int           { return w.id }
func (w *mockWorker) PendingCount() int { return w.pendingCount }

func TestBytesSum_SingleWorker(t *testing.T) {
	s := distribution.NewBytesSum()
	workers := []distribution.WorkerInfo{&mockWorker{id: 0, pendingCount: 5}}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Key: []byte("key")}}, workers)
	assert.Equal(t, 0, idx)
}

func TestBytesSum_UsesSumOfKeyBytesModuloWorkerCount(t *testing.T) {
	s := distribution.NewBytesSum()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 10},
		&mockWorker{id: 1, pendingCount: 3},
		&mockWorker{id: 2, pendingCount: 7},
	}
	idx := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Key: []byte{1, 2, 3}}}, workers)
	assert.Equal(t, 0, idx)
}

func TestBytesSum_ConsistentWithSameKey(t *testing.T) {
	s := distribution.NewBytesSum()
	workers := []distribution.WorkerInfo{
		&mockWorker{id: 0, pendingCount: 0},
		&mockWorker{id: 1, pendingCount: 0},
	}
	idx1 := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Key: []byte("same-key")}}, workers)
	idx2 := s.Route(&kafkaflow.MessageContext{Message: &kafkaflow.Message{Key: []byte("same-key")}}, workers)
	assert.Equal(t, idx1, idx2)
}
