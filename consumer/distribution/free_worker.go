package distribution

import kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"

// FreeWorker routes messages to the worker with the fewest pending messages.
type FreeWorker struct{}

// NewFreeWorker creates a new FreeWorker strategy.
func NewFreeWorker() *FreeWorker {
	return &FreeWorker{}
}

// Route selects the worker with the shortest channel queue.
func (s *FreeWorker) Route(mc *kafkaflow.MessageContext, workers []WorkerInfo) int {
	if len(workers) == 0 {
		return 0
	}

	minIdx := 0
	minPending := workers[0].PendingCount()
	for i := 1; i < len(workers); i++ {
		pending := workers[i].PendingCount()
		if pending < minPending {
			minPending = pending
			minIdx = i
		}
	}
	return minIdx
}
