package consumer

import (
	"context"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
	"github.com/xiaoxiaodek/kafkaflow-go/events"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
	"golang.org/x/sync/errgroup"
)

// WorkerPool manages a set of workers and dispatches messages to them
// using a distribution strategy.
type WorkerPool struct {
	workers    []*Worker
	strategy   distribution.Strategy
	bufferSize int
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool(workerCount int, bufferSize int, pipeline kafkaflow.Handler, strategy distribution.Strategy) *WorkerPool {
	workers := make([]*Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = NewWorker(i, bufferSize, pipeline)
	}
	return &WorkerPool{
		workers:    workers,
		strategy:   strategy,
		bufferSize: bufferSize,
	}
}

// Workers returns the list of workers.
func (p *WorkerPool) Workers() []*Worker {
	return p.workers
}

// SetEventBus sets the event bus on all workers.
func (p *WorkerPool) SetEventBus(eb *events.Bus) {
	for _, w := range p.workers {
		w.SetEventBus(eb)
	}
}

// SetLogger sets the logger on all workers.
func (p *WorkerPool) SetLogger(l log.Logger) {
	for _, w := range p.workers {
		w.SetLogger(l)
	}
}

// Dispatch routes a message to a worker based on the distribution strategy.
// Returns false if all worker channels are full.
func (p *WorkerPool) Dispatch(mc *kafkaflow.MessageContext) bool {
	infos := make([]distribution.WorkerInfo, len(p.workers))
	for i, w := range p.workers {
		infos[i] = w
	}
	idx := p.strategy.Route(mc, infos)

	select {
	case p.workers[idx].Channel() <- mc:
		return true
	default:
		return false
	}
}

// Start launches all workers in separate goroutines managed by errgroup.
func (p *WorkerPool) Start(ctx context.Context) *errgroup.Group {
	eg, ctx := errgroup.WithContext(ctx)
	for _, w := range p.workers {
		w := w
		eg.Go(func() error {
			return w.Run(ctx)
		})
	}
	return eg
}

// Stop closes all worker channels.
func (p *WorkerPool) Stop() {
	for _, w := range p.workers {
		close(w.Channel())
	}
}

// OffsetStores returns the offset stores for all workers.
func (p *WorkerPool) OffsetStores() []*OffsetStore {
	stores := make([]*OffsetStore, len(p.workers))
	for i, w := range p.workers {
		stores[i] = w.OffsetStore()
	}
	return stores
}
