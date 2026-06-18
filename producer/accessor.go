package producer

import (
	"fmt"
	"sync"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

// ProducerAccessor manages named producers, creating them on demand.
type ProducerAccessor struct {
	producers map[string]*MessageProducer
	configs   map[string]ProducerConfig
	pipelines map[string]kafkaflow.Handler
	mu        sync.RWMutex
}

// NewProducerAccessor creates a new ProducerAccessor.
func NewProducerAccessor() *ProducerAccessor {
	return &ProducerAccessor{
		producers: make(map[string]*MessageProducer),
		configs:   make(map[string]ProducerConfig),
		pipelines: make(map[string]kafkaflow.Handler),
	}
}

// Register adds a producer configuration without creating it yet.
func (pa *ProducerAccessor) Register(cfg ProducerConfig, pipeline kafkaflow.Handler) {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	pa.configs[cfg.Name] = cfg
	pa.pipelines[cfg.Name] = pipeline
}

// Get returns a producer by name, creating it if necessary.
func (pa *ProducerAccessor) Get(name string) (*MessageProducer, error) {
	pa.mu.RLock()
	mp, ok := pa.producers[name]
	pa.mu.RUnlock()
	if ok {
		return mp, nil
	}

	pa.mu.Lock()
	defer pa.mu.Unlock()

	if mp, ok := pa.producers[name]; ok {
		return mp, nil
	}

	cfg, ok := pa.configs[name]
	if !ok {
		return nil, fmt.Errorf("kafkaflow: producer %q not registered", name)
	}

	pipeline, ok := pa.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("kafkaflow: pipeline for producer %q not registered", name)
	}

	mp, err := NewMessageProducer(cfg, pipeline)
	if err != nil {
		return nil, err
	}

	pa.producers[name] = mp
	return mp, nil
}

// Close closes all managed producers.
func (pa *ProducerAccessor) Close() {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	for _, mp := range pa.producers {
		mp.Close()
	}
	pa.producers = make(map[string]*MessageProducer)
}
