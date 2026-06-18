package distribution

import kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"

// WorkerInfo provides information about a worker for distribution decisions.
type WorkerInfo interface {
	ID() int
	PendingCount() int
}

// Strategy determines which worker should process a given message.
type Strategy interface {
	// Route returns the worker index for the given message context.
	Route(mc *kafkaflow.MessageContext, workers []WorkerInfo) int
}
