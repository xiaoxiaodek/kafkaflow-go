package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
	"github.com/xiaoxiaodek/kafkaflow-go/events"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
	"golang.org/x/sync/errgroup"
)

type kafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Assign(partitions []kafka.TopicPartition) error
	Assignment() ([]kafka.TopicPartition, error)
	Pause(partitions []kafka.TopicPartition) error
	Resume(partitions []kafka.TopicPartition) error
	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error)
	Seek(partition kafka.TopicPartition, timeoutMs int) error
	Poll(timeoutMs int) kafka.Event
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low int64, high int64, err error)
	Close() error
}

type kafkaConsumerFactory func(cfg ConsumerConfig) (kafkaConsumer, error)

type consumerState int

const (
	stateStopped consumerState = iota
	stateStarting
	stateRunning
	stateStopping
)

// ConsumerConfig holds configuration for a consumer.
type ConsumerConfig struct {
	Name                 string
	GroupID              string
	Topics               []string
	Brokers              string
	WorkerCount          int
	BufferSize           int
	Strategy             distribution.Strategy
	Pipeline             kafkaflow.Handler
	Logger               log.Logger
	Security             *kafkaflow.SecurityConfig
	AutoOffsetReset      string
	ManualAssign         bool
	StoreOffsets         bool
	ManualCompletion     bool
	WorkerStopTimeout    time.Duration
	StartImmediately     bool
	MaxPollIntervalMs    int
	CompressionType      string
	StatisticsHandler    func(string)
	PartitionsAssigned   func([]kafka.TopicPartition)
	PartitionsRevoked    func([]kafka.TopicPartition)
	WorkersCountCalculator func(WorkersCountContext) int
	WorkersEvaluationInterval time.Duration
}

// WorkersCountContext provides context for dynamic worker count calculation.
type WorkersCountContext struct {
	ConsumerName  string
	GroupID       string
	AssignedPartitions int
	CurrentWorkers int
}

// ConsumerManager manages the lifecycle of a Kafka consumer and its worker pool.
type ConsumerManager struct {
	config          ConsumerConfig
	consumer        kafkaConsumer
	consumerFactory kafkaConsumerFactory
	pool            *WorkerPool
	committer       *OffsetCommitter
	logger          log.Logger
	eventBus        *events.Bus
	eg              *errgroup.Group
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	state           consumerState
	stopped         chan struct{}
	storeOffsets    bool
	evalTimer       *time.Ticker
	evalStopCh      chan struct{}
}

// NewConsumerManager creates a new ConsumerManager.
func NewConsumerManager(cfg ConsumerConfig) (*ConsumerManager, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.DefaultLogger()
	}
	if cfg.Strategy == nil {
		cfg.Strategy = distribution.NewFreeWorker()
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 100
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "earliest"
	}
	if cfg.WorkerStopTimeout == 0 {
		cfg.WorkerStopTimeout = 30 * time.Second
	}
	if cfg.StoreOffsets {
		cfg.StoreOffsets = true
	}

	m := &ConsumerManager{
		config:          cfg,
		consumerFactory: defaultKafkaConsumerFactory,
		logger:          cfg.Logger,
		state:           stateStopped,
		storeOffsets:    true,
	}
	if err := m.initializeConsumer(); err != nil {
		return nil, err
	}

	return m, nil
}

func defaultKafkaConsumerFactory(cfg ConsumerConfig) (kafkaConsumer, error) {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.Brokers,
		"group.id":           cfg.GroupID,
		"enable.auto.commit": false,
	}
	if cfg.AutoOffsetReset != "" {
		cm.SetKey("auto.offset.reset", cfg.AutoOffsetReset)
	} else {
		cm.SetKey("auto.offset.reset", "earliest")
	}
	if cfg.MaxPollIntervalMs > 0 {
		cm.SetKey("max.poll.interval.ms", cfg.MaxPollIntervalMs)
	}
	if cfg.CompressionType != "" {
		cm.SetKey("compression.type", cfg.CompressionType)
	}
	kafkaflow.ApplySecurityConfig(cm, cfg.Security)

	c, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (m *ConsumerManager) initializeConsumer() error {
	if m.consumerFactory == nil {
		m.consumerFactory = defaultKafkaConsumerFactory
	}
	c, err := m.consumerFactory(m.config)
	if err != nil {
		return err
	}

	if m.config.ManualAssign {
		tps := make([]kafka.TopicPartition, len(m.config.Topics))
		for i, topic := range m.config.Topics {
			topicCopy := topic
			tps[i] = kafka.TopicPartition{Topic: &topicCopy, Partition: kafka.PartitionAny}
		}
		if err := c.Assign(tps); err != nil {
			c.Close()
			return err
		}
	} else {
		if err := c.SubscribeTopics(m.config.Topics, m.rebalanceCallback); err != nil {
			c.Close()
			return err
		}
	}

	m.consumer = c
	m.rebuildWorkerPool()
	return nil
}

// SetEventBus sets the event bus for lifecycle events.
func (m *ConsumerManager) SetEventBus(eb *events.Bus) {
	m.eventBus = eb
}

func (m *ConsumerManager) rebalanceCallback(c *kafka.Consumer, ev kafka.Event) error {
	switch e := ev.(type) {
	case kafka.AssignedPartitions:
		m.logger.Info(context.Background(), "partitions assigned",
			"consumer", m.config.Name,
			"partitions", len(e.Partitions),
		)
	case kafka.RevokedPartitions:
		m.logger.Info(context.Background(), "partitions revoked",
			"consumer", m.config.Name,
			"partitions", len(e.Partitions),
		)
	}
	return nil
}

// Start begins consuming messages. Blocks until ctx is cancelled or Stop is called.
func (m *ConsumerManager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.state == stateRunning || m.state == stateStarting {
		m.mu.Unlock()
		return nil
	}
	m.state = stateStarting
	m.stopped = make(chan struct{})
	m.mu.Unlock()

	if m.consumer == nil {
		if err := m.initializeConsumer(); err != nil {
			m.mu.Lock()
			m.state = stateStopped
			close(m.stopped)
			m.mu.Unlock()
			return err
		}
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.eg, _ = errgroup.WithContext(m.ctx)

	m.mu.Lock()
	m.state = stateRunning
	m.mu.Unlock()

	if m.config.WorkersCountCalculator != nil && m.config.WorkersEvaluationInterval > 0 {
		m.startWorkerEvaluation()
	}

	workerEg := m.pool.Start(m.ctx)
	if m.committer != nil {
		m.committer.Start()
	}

	m.eg.Go(func() error {
		return m.pollLoop(m.ctx)
	})

	err := workerEg.Wait()
	m.pool.Stop()
	if m.committer != nil {
		m.committer.Stop()
	}
	m.stopWorkerEvaluation()
	m.mu.Lock()
	if m.consumer != nil {
		m.consumer.Close()
		m.consumer = nil
	}
	m.state = stateStopped
	close(m.stopped)
	m.mu.Unlock()

	return err
}

// Stop signals the consumer to stop gracefully and waits for full shutdown.
func (m *ConsumerManager) Stop() {
	m.mu.Lock()
	if m.state != stateRunning && m.state != stateStarting {
		m.mu.Unlock()
		return
	}
	m.state = stateStopping
	if m.cancel != nil {
		m.cancel()
	}
	stopped := m.stopped
	m.mu.Unlock()

	if stopped != nil {
		<-stopped
	}

	m.mu.Lock()
	if m.state == stateStopping {
		m.state = stateStopped
	}
	m.mu.Unlock()
}

// Restart stops and restarts the consumer with a cooldown delay matching C# behavior.
func (m *ConsumerManager) Restart() error {
	m.Stop()

	go func() {
		time.Sleep(5 * time.Second)
		if err := m.Start(context.Background()); err != nil {
			m.logger.Error(context.Background(), "consumer restart failed",
				"name", m.config.Name,
				"error", err,
			)
		}
	}()
	return nil
}

// Status returns the current consumer status.
func (m *ConsumerManager) Status() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case stateStopped:
		return "Stopped"
	case stateStarting:
		return "Starting"
	case stateRunning:
		return "Running"
	case stateStopping:
		return "Stopping"
	default:
		return "Unknown"
	}
}

// Name returns the consumer name.
func (m *ConsumerManager) Name() string {
	return m.config.Name
}

// GroupID returns the consumer group ID.
func (m *ConsumerManager) GroupID() string {
	return m.config.GroupID
}

// Topics returns the subscribed topics.
func (m *ConsumerManager) Topics() []string {
	return m.config.Topics
}

// WorkerCount returns the current worker count.
func (m *ConsumerManager) WorkerCount() int {
	return m.config.WorkerCount
}

// Pause pauses consumption for the given topic partitions.
func (m *ConsumerManager) Pause(topics []string) error {
	partitions, err := m.filterAssignment(topics)
	if err != nil {
		return err
	}
	return m.consumer.Pause(partitions)
}

// Resume resumes consumption for the given topic partitions.
func (m *ConsumerManager) Resume(topics []string) error {
	partitions, err := m.filterAssignment(topics)
	if err != nil {
		return err
	}
	return m.consumer.Resume(partitions)
}

// ResetOffsets resets offsets to the beginning for the given topics and restarts.
func (m *ConsumerManager) ResetOffsets(topics []string) error {
	partitions, err := m.filterAssignment(topics)
	if err != nil {
		return err
	}

	beginningOffsets, err := m.getOffsetsForTimestamp(partitions, -2)
	if err != nil {
		return err
	}

	return m.overrideOffsetsAndRestart(beginningOffsets)
}

// RewindOffsets rewinds offsets to a specific timestamp for the given topics and restarts.
func (m *ConsumerManager) RewindOffsets(topics []string, timestamp int64) error {
	partitions, err := m.filterAssignment(topics)
	if err != nil {
		return err
	}

	tsOffsets, err := m.getOffsetsForTimestamp(partitions, timestamp)
	if err != nil {
		return err
	}

	return m.overrideOffsetsAndRestart(tsOffsets)
}

// ChangeWorkersCount changes the number of workers and restarts.
func (m *ConsumerManager) ChangeWorkersCount(count int) error {
	if count <= 0 {
		return fmt.Errorf("worker count must be positive, got %d", count)
	}
	m.config.WorkerCount = count
	m.rebuildWorkerPool()
	return m.Restart()
}

func (m *ConsumerManager) rebuildWorkerPool() {
	if m.config.BufferSize == 0 {
		m.config.BufferSize = 100
	}
	if m.config.Strategy == nil {
		m.config.Strategy = distribution.NewFreeWorker()
	}
	if m.config.Pipeline == nil {
		m.config.Pipeline = kafkaflow.NoopHandler
	}
	m.pool = NewWorkerPool(m.config.WorkerCount, m.config.BufferSize, m.config.Pipeline, m.config.Strategy)
	m.pool.SetLogger(m.logger)
	if m.eventBus != nil {
		m.pool.SetEventBus(m.eventBus)
	}
	if m.consumer != nil && m.storeOffsets {
		offsetMgr := NewOffsetManager(m.consumer, m.pool.OffsetStores(), 5*time.Second)
		m.committer = NewOffsetCommitter(offsetMgr)
	}
}

func (m *ConsumerManager) filterAssignment(topics []string) ([]kafka.TopicPartition, error) {
	assignment, err := m.consumer.Assignment()
	if err != nil {
		return nil, err
	}

	if len(topics) == 0 {
		return assignment, nil
	}

	topicSet := make(map[string]bool, len(topics))
	for _, t := range topics {
		topicSet[t] = true
	}

	var filtered []kafka.TopicPartition
	for _, tp := range assignment {
		if topicSet[*tp.Topic] {
			filtered = append(filtered, tp)
		}
	}
	return filtered, nil
}

func (m *ConsumerManager) getOffsetsForTimestamp(partitions []kafka.TopicPartition, timestamp int64) ([]kafka.TopicPartition, error) {
	tps := make([]kafka.TopicPartition, len(partitions))
	for i, tp := range partitions {
		tps[i] = kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    kafka.Offset(timestamp),
		}
	}

	return m.consumer.OffsetsForTimes(tps, 5000)
}

func (m *ConsumerManager) overrideOffsetsAndRestart(offsets []kafka.TopicPartition) error {
	for _, tp := range offsets {
		if err := m.consumer.Seek(tp, 5000); err != nil {
			return err
		}
	}
	return m.Restart()
}

// PartitionLag holds lag information for a topic partition.
type PartitionLag struct {
	Topic     string
	Partition int32
	Lag       int64
}

// Lag returns the consumer lag for each assigned partition.
func (m *ConsumerManager) Lag() ([]PartitionLag, error) {
	assignment, err := m.consumer.Assignment()
	if err != nil {
		return nil, err
	}

	var result []PartitionLag
	for _, tp := range assignment {
		_, high, err := m.consumer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 5000)
		if err != nil {
			continue
		}
		lag := high - int64(tp.Offset)
		if lag < 0 {
			lag = 0
		}
		result = append(result, PartitionLag{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
			Lag:       lag,
		})
	}
	return result, nil
}

func (m *ConsumerManager) startWorkerEvaluation() {
	m.evalStopCh = make(chan struct{})
	m.evalTimer = time.NewTicker(m.config.WorkersEvaluationInterval)
	go func() {
		for {
			select {
			case <-m.evalStopCh:
				m.evalTimer.Stop()
				return
			case <-m.evalTimer.C:
				m.evaluateWorkersCount()
			}
		}
	}()
}

func (m *ConsumerManager) stopWorkerEvaluation() {
	if m.evalStopCh != nil {
		close(m.evalStopCh)
		m.evalStopCh = nil
	}
}

func (m *ConsumerManager) evaluateWorkersCount() {
	if m.config.WorkersCountCalculator == nil {
		return
	}

	assignment, err := m.consumer.Assignment()
	partitionCount := 0
	if err == nil {
		partitionCount = len(assignment)
	}

	ctx := WorkersCountContext{
		ConsumerName:       m.config.Name,
		GroupID:            m.config.GroupID,
		AssignedPartitions: partitionCount,
		CurrentWorkers:     m.config.WorkerCount,
	}

	newCount := m.config.WorkersCountCalculator(ctx)
	if newCount <= 0 {
		return
	}

	if newCount != m.config.WorkerCount {
		m.logger.Info(context.Background(), "dynamic worker count changed",
			"consumer", m.config.Name,
			"from", m.config.WorkerCount,
			"to", newCount,
		)
		m.ChangeWorkersCount(newCount)
	}
}

func (m *ConsumerManager) pollLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		ev := m.consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			mc := &kafkaflow.MessageContext{
				ConsumerContext: &kafkaflow.ConsumerContext{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Offset:    int64(e.TopicPartition.Offset),
					GroupID:   m.config.GroupID,
				},
				Message: &kafkaflow.Message{
					Key:       e.Key,
					Value:     e.Value,
					Headers:   convertHeaders(e.Headers),
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Offset:    int64(e.TopicPartition.Offset),
					Timestamp: e.Timestamp.UnixMilli(),
				},
				Items: make(map[string]any),
			}

			if !m.pool.Dispatch(mc) {
				m.logger.Warn(ctx, "worker pool full, dropping message",
					"topic", *e.TopicPartition.Topic,
					"partition", e.TopicPartition.Partition,
					"offset", e.TopicPartition.Offset,
				)
			}

		case kafka.Error:
			m.logger.Error(ctx, "kafka consumer error", "error", e.Error())
			if e.IsFatal() {
				return e
			}
		}
	}
}

func convertHeaders(headers []kafka.Header) []kafkaflow.Header {
	result := make([]kafkaflow.Header, len(headers))
	for i, h := range headers {
		result[i] = kafkaflow.Header{Key: h.Key, Value: h.Value}
	}
	return result
}
