package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// TelemetryStorage stores telemetry metrics in memory.
type TelemetryStorage interface {
	Put(metric ConsumerTelemetryMetric)
	GetAll() []ConsumerTelemetryMetric
}

// MemoryTelemetryStorage is an in-memory implementation of TelemetryStorage.
type MemoryTelemetryStorage struct {
	mu      sync.RWMutex
	metrics map[string]ConsumerTelemetryMetric
	expiry  time.Duration
	stopCh  chan struct{}
}

// NewMemoryTelemetryStorage creates a new MemoryTelemetryStorage.
func NewMemoryTelemetryStorage(expiry time.Duration) *MemoryTelemetryStorage {
	s := &MemoryTelemetryStorage{
		metrics: make(map[string]ConsumerTelemetryMetric),
		expiry:  expiry,
		stopCh:  make(chan struct{}),
	}
	go s.cleanLoop()
	return s
}

func (s *MemoryTelemetryStorage) key(m ConsumerTelemetryMetric) string {
	return fmt.Sprintf("%s:%s:%s:%s", m.InstanceName, m.GroupID, m.ConsumerName, m.Topic)
}

// Put stores a telemetry metric.
func (s *MemoryTelemetryStorage) Put(metric ConsumerTelemetryMetric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics[s.key(metric)] = metric
}

// GetAll returns all stored metrics.
func (s *MemoryTelemetryStorage) GetAll() []ConsumerTelemetryMetric {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]ConsumerTelemetryMetric, 0, len(s.metrics))
	for _, m := range s.metrics {
		result = append(result, m)
	}
	return result
}

func (s *MemoryTelemetryStorage) cleanLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.clean()
		}
	}
}

func (s *MemoryTelemetryStorage) clean() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-s.expiry).UnixMilli()
	for key, m := range s.metrics {
		if m.SentAt < cutoff {
			delete(s.metrics, key)
		}
	}
}

// Stop stops the clean loop.
func (s *MemoryTelemetryStorage) Stop() {
	close(s.stopCh)
}

// TelemetryHandler stores telemetry metrics consumed from Kafka.
type TelemetryHandler struct {
	storage TelemetryStorage
}

// NewTelemetryHandler creates a new TelemetryHandler.
func NewTelemetryHandler(storage TelemetryStorage) *TelemetryHandler {
	return &TelemetryHandler{storage: storage}
}

// HandleMessage deserializes and stores a telemetry metric.
func (h *TelemetryHandler) HandleMessage(ctx context.Context, mc *kafkaflow.MessageContext) error {
	data, ok := mc.Message.Value.([]byte)
	if !ok {
		return fmt.Errorf("telemetry: expected []byte, got %T", mc.Message.Value)
	}
	var metric ConsumerTelemetryMetric
	if err := json.Unmarshal(data, &metric); err != nil {
		return err
	}
	h.storage.Put(metric)
	return nil
}

// TelemetryScheduler periodically collects consumer metrics and publishes them.
type TelemetryScheduler struct {
	consumers []*consumer.ConsumerManager
	producer  *AdminProducer
	topic     string
	storage   TelemetryStorage
	logger    log.Logger
	interval  time.Duration
	stopCh    chan struct{}
	instance  string
}

// NewTelemetryScheduler creates a new TelemetryScheduler.
func NewTelemetryScheduler(
	consumers []*consumer.ConsumerManager,
	producer *AdminProducer,
	topic string,
	storage TelemetryStorage,
	logger log.Logger,
) *TelemetryScheduler {
	hostname, _ := os.Hostname()
	instance := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	return &TelemetryScheduler{
		consumers: consumers,
		producer:  producer,
		topic:     topic,
		storage:   storage,
		logger:    logger,
		interval:  5 * time.Second,
		stopCh:    make(chan struct{}),
		instance:  instance,
	}
}

// Start begins periodic telemetry collection.
func (ts *TelemetryScheduler) Start() {
	ts.logger.Info(context.Background(), "telemetry scheduler started")
	go ts.loop()
}

// Stop stops the telemetry scheduler.
func (ts *TelemetryScheduler) Stop() {
	close(ts.stopCh)
}

func (ts *TelemetryScheduler) loop() {
	ticker := time.NewTicker(ts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ts.stopCh:
			return
		case <-ticker.C:
			ts.collect()
		}
	}
}

func (ts *TelemetryScheduler) collect() {
	ctx := context.Background()

	for _, cm := range ts.consumers {
		for _, topic := range cm.Topics() {
			metric := ConsumerTelemetryMetric{
				GroupID:      cm.GroupID(),
				ConsumerName: cm.Name(),
				Topic:        topic,
				InstanceName: ts.instance,
				SentAt:       time.Now().UnixMilli(),
				WorkersCount: cm.WorkerCount(),
				Status:       cm.Status(),
			}

			if err := ts.producer.ProduceTelemetry(ctx, ts.topic, metric); err != nil {
				ts.logger.Error(ctx, "failed to produce telemetry",
					"consumer", cm.Name(),
					"error", err,
				)
			}
		}
	}
}
