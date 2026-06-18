package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// WorkerBalancer dynamically adjusts worker count based on consumer lag.
type WorkerBalancer struct {
	manager       *ConsumerManager
	minWorkers    int
	maxWorkers    int
	checkInterval time.Duration
	logger        log.Logger
	stopCh        chan struct{}
	mu            sync.Mutex
}

// WorkerBalancerConfig configures the dynamic worker balancer.
type WorkerBalancerConfig struct {
	MinWorkers    int
	MaxWorkers    int
	CheckInterval time.Duration
	Logger        log.Logger
}

// NewWorkerBalancer creates a new WorkerBalancer.
func NewWorkerBalancer(manager *ConsumerManager, cfg WorkerBalancerConfig) *WorkerBalancer {
	if cfg.MinWorkers <= 0 {
		cfg.MinWorkers = 1
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = 10
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.DefaultLogger()
	}

	return &WorkerBalancer{
		manager:       manager,
		minWorkers:    cfg.MinWorkers,
		maxWorkers:    cfg.MaxWorkers,
		checkInterval: cfg.CheckInterval,
		logger:        cfg.Logger,
		stopCh:        make(chan struct{}),
	}
}

// Start begins periodic worker count evaluation.
func (b *WorkerBalancer) Start() {
	go b.loop()
}

// Stop stops the balancer.
func (b *WorkerBalancer) Stop() {
	close(b.stopCh)
}

func (b *WorkerBalancer) loop() {
	ticker := time.NewTicker(b.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.evaluate()
		}
	}
}

func (b *WorkerBalancer) evaluate() {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentWorkers := b.manager.WorkerCount()

	// Calculate pending load across all workers
	totalPending := 0
	for _, w := range b.manager.pool.Workers() {
		totalPending += w.PendingCount()
	}

	avgPending := 0
	if currentWorkers > 0 {
		avgPending = totalPending / currentWorkers
	}

	var newWorkers int
	switch {
	case avgPending > 50 && currentWorkers < b.maxWorkers:
		newWorkers = currentWorkers + 1
	case avgPending < 5 && currentWorkers > b.minWorkers:
		newWorkers = currentWorkers - 1
	default:
		return
	}

	if newWorkers != currentWorkers {
		b.logger.Info(context.Background(), "adjusting worker count",
			"consumer", b.manager.Name(),
			"from", currentWorkers,
			"to", newWorkers,
			"avg_pending", avgPending,
		)
		b.manager.ChangeWorkersCount(newWorkers)
	}
}
