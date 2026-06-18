package builder

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/admin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin/api"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer/distribution"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
	"github.com/xiaoxiaodek/kafkaflow-go/producer"
)

// Config is the top-level configuration builder for KafkaFlow.
type Config struct {
	appName          string
	brokers          string
	consumers        []ConsumerBuilder
	producers        []ProducerBuilder
	logger           log.Logger
	security         *kafkaflow.SecurityConfig
	adminEnabled     bool
	adminTopic       string
	adminGroupID     string
	telemetryEnabled bool
	telemetryTopic   string
	telemetryGroupID string
	adminAPIAddr     string
	topicsToCreate   []kafkaflow.TopicConfig
}

// NewConfig creates a new configuration builder.
func NewConfig(appName string) *Config {
	return &Config{
		appName: appName,
		logger:  log.DefaultLogger(),
	}
}

// WithBrokers sets the Kafka broker addresses.
func (c *Config) WithBrokers(brokers string) *Config {
	c.brokers = brokers
	return c
}

// WithLogger sets a custom logger.
func (c *Config) WithLogger(l log.Logger) *Config {
	c.logger = l
	return c
}

// WithSecurity sets SASL/SSL security configuration.
func (c *Config) WithSecurity(sc *kafkaflow.SecurityConfig) *Config {
	c.security = sc
	return c
}

// WithConsumer starts a consumer configuration block.
func (c *Config) WithConsumer(name, groupID string) *ConsumerBuilder {
	cb := ConsumerBuilder{
		config: c,
		cfg: consumer.ConsumerConfig{
			Name:       name,
			GroupID:    groupID,
			Brokers:    c.brokers,
			BufferSize: 100,
			Logger:     c.logger,
			Security:   c.security,
		},
	}
	c.consumers = append(c.consumers, cb)
	return &c.consumers[len(c.consumers)-1]
}

// WithProducer starts a producer configuration block.
func (c *Config) WithProducer(name string) *ProducerBuilder {
	pb := ProducerBuilder{
		config: c,
		cfg: producer.ProducerConfig{
			Name:    name,
			Brokers: c.brokers,
		},
	}
	c.producers = append(c.producers, pb)
	return &c.producers[len(c.producers)-1]
}

// EnableAdminMessages enables the admin messaging system.
func (c *Config) EnableAdminMessages(topic string) *Config {
	c.adminEnabled = true
	c.adminTopic = topic
	if c.adminGroupID == "" {
		c.adminGroupID = "Admin-" + c.appName
	}
	return c
}

// EnableTelemetry enables telemetry collection and publishing.
func (c *Config) EnableTelemetry(topic string) *Config {
	c.telemetryEnabled = true
	c.telemetryTopic = topic
	if c.telemetryGroupID == "" {
		c.telemetryGroupID = "Telemetry-" + c.appName
	}
	return c
}

// WithAdminAPI enables the admin REST API on the given address.
func (c *Config) WithAdminAPI(addr string) *Config {
	c.adminAPIAddr = addr
	return c
}

// CreateTopicIfNotExists registers a topic for auto-creation on startup.
func (c *Config) CreateTopicIfNotExists(name string, numPartitions int, replicationFactor int) *Config {
	c.topicsToCreate = append(c.topicsToCreate, kafkaflow.TopicConfig{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	return c
}

// Build creates the Bus from the configuration.
func (c *Config) Build() (*Bus, error) {
	if c.brokers == "" {
		return nil, fmt.Errorf("%w: brokers must be configured", kafkaflow.ErrInvalidConfiguration)
	}

	if len(c.topicsToCreate) > 0 {
		if err := kafkaflow.CreateTopicsIfNotExist(c.brokers, c.security, c.topicsToCreate); err != nil {
			return nil, fmt.Errorf("failed to create topics: %w", err)
		}
	}

	consumers := make([]*consumer.ConsumerManager, len(c.consumers))
	for i, cb := range c.consumers {
		cm, err := consumer.NewConsumerManager(cb.cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer %q: %w", cb.cfg.Name, err)
		}
		consumers[i] = cm
	}

	pa := producer.NewProducerAccessor()
	for _, pb := range c.producers {
		pa.Register(pb.cfg, pb.pipeline)
	}

	bus := NewBus(consumers, pa, c.logger)

	for _, cm := range consumers {
		cm.SetEventBus(bus.eventBus)
	}

	if c.adminEnabled {
		adminProducer, err := admin.NewAdminProducer(c.brokers, c.adminTopic)
		if err != nil {
			return nil, fmt.Errorf("failed to create admin producer: %w", err)
		}

		adminHandler := admin.NewAdminHandler(bus, c.logger)
		adminConsumer, err := admin.NewAdminConsumer(
			c.brokers, c.adminTopic, c.adminGroupID, adminHandler, c.logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create admin consumer: %w", err)
		}

		bus.adminConsumer = adminConsumer
		bus.adminProducer = adminProducer
		bus.consumerAdmin = admin.NewConsumerAdmin(adminProducer)
	}

	if c.telemetryEnabled {
		if bus.adminProducer == nil {
			var err error
			bus.adminProducer, err = admin.NewAdminProducer(c.brokers, c.telemetryTopic)
			if err != nil {
				return nil, fmt.Errorf("failed to create telemetry producer: %w", err)
			}
		}

		storage := admin.NewMemoryTelemetryStorage(5 * time.Minute)
		bus.telemetryStorage = storage

		scheduler := admin.NewTelemetryScheduler(
			consumers, bus.adminProducer, c.telemetryTopic, storage, c.logger,
		)
		bus.telemetryScheduler = scheduler

		telemetryHandler := admin.NewTelemetryHandler(storage)
		telemetryConsumer, err := admin.NewTelemetryConsumerService(
			c.brokers, c.telemetryTopic, c.telemetryGroupID, telemetryHandler, c.logger,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create telemetry consumer: %w", err)
		}
		bus.telemetryConsumer = telemetryConsumer
	}

	if c.adminAPIAddr != "" {
		if bus.consumerAdmin == nil {
			return nil, fmt.Errorf("admin API requires EnableAdminMessages to be called")
		}
		bus.adminAPIServer = api.NewServer(bus.consumerAdmin, consumers, bus.telemetryStorage)
		bus.adminAPIAddr = c.adminAPIAddr
	}

	return bus, nil
}

// ConsumerBuilder builds a consumer configuration.
type ConsumerBuilder struct {
	config   *Config
	cfg      consumer.ConsumerConfig
	pipeline kafkaflow.Handler
}

// WithTopics sets the topics to subscribe to.
func (cb *ConsumerBuilder) WithTopics(topics ...string) *ConsumerBuilder {
	cb.cfg.Topics = topics
	return cb
}

// WithWorkers sets the number of worker goroutines.
func (cb *ConsumerBuilder) WithWorkers(count int) *ConsumerBuilder {
	cb.cfg.WorkerCount = count
	return cb
}

// WithBufferSize sets the per-worker channel buffer size.
func (cb *ConsumerBuilder) WithBufferSize(size int) *ConsumerBuilder {
	cb.cfg.BufferSize = size
	return cb
}

// WithDistribution sets the distribution strategy.
func (cb *ConsumerBuilder) WithDistribution(strategy distribution.Strategy) *ConsumerBuilder {
	cb.cfg.Strategy = strategy
	return cb
}

// WithMiddleware sets the middleware pipeline for this consumer.
func (cb *ConsumerBuilder) WithMiddleware(middlewares ...kafkaflow.Middleware) *ConsumerBuilder {
	cb.cfg.Pipeline = kafkaflow.ComposePipeline(middlewares, kafkaflow.NoopHandler)
	return cb
}

// WithAutoOffsetReset sets the auto.offset.reset config.
func (cb *ConsumerBuilder) WithAutoOffsetReset(reset string) *ConsumerBuilder {
	cb.cfg.AutoOffsetReset = reset
	return cb
}

// WithManualAssignPartitions assigns partitions manually instead of using group balancing.
func (cb *ConsumerBuilder) WithManualAssignPartitions() *ConsumerBuilder {
	cb.cfg.ManualAssign = true
	return cb
}

// WithoutStoringOffsets disables offset storage and committing.
func (cb *ConsumerBuilder) WithoutStoringOffsets() *ConsumerBuilder {
	cb.cfg.StoreOffsets = false
	return cb
}

// WithManualMessageCompletion requires explicit offset completion from handlers.
func (cb *ConsumerBuilder) WithManualMessageCompletion() *ConsumerBuilder {
	cb.cfg.ManualCompletion = true
	return cb
}

// WithWorkerStopTimeout sets the maximum time to wait for workers to drain on shutdown.
func (cb *ConsumerBuilder) WithWorkerStopTimeout(timeout time.Duration) *ConsumerBuilder {
	cb.cfg.WorkerStopTimeout = timeout
	return cb
}

// WithPartitionsAssignedHandler sets a callback for when partitions are assigned.
func (cb *ConsumerBuilder) WithPartitionsAssignedHandler(handler func([]kafka.TopicPartition)) *ConsumerBuilder {
	cb.cfg.PartitionsAssigned = handler
	return cb
}

// WithPartitionsRevokedHandler sets a callback for when partitions are revoked.
func (cb *ConsumerBuilder) WithPartitionsRevokedHandler(handler func([]kafka.TopicPartition)) *ConsumerBuilder {
	cb.cfg.PartitionsRevoked = handler
	return cb
}

// WithInitialState sets whether the consumer starts in Running or Stopped state.
func (cb *ConsumerBuilder) WithInitialState(start bool) *ConsumerBuilder {
	cb.cfg.StartImmediately = start
	return cb
}

// WithMaxPollIntervalMs sets the max.poll.interval.ms config.
func (cb *ConsumerBuilder) WithMaxPollIntervalMs(ms int) *ConsumerBuilder {
	cb.cfg.MaxPollIntervalMs = ms
	return cb
}

// WithCompressionType sets the native Kafka compression type (lz4, snappy, zstd, gzip).
func (cb *ConsumerBuilder) WithCompressionType(compressionType string) *ConsumerBuilder {
	cb.cfg.CompressionType = compressionType
	return cb
}

// WithStatisticsHandler sets a callback for librdkafka statistics.
func (cb *ConsumerBuilder) WithStatisticsHandler(handler func(string)) *ConsumerBuilder {
	cb.cfg.StatisticsHandler = handler
	return cb
}

// WithDynamicWorkersCount sets a dynamic worker count calculator with evaluation interval.
func (cb *ConsumerBuilder) WithDynamicWorkersCount(calculator func(consumer.WorkersCountContext) int, interval time.Duration) *ConsumerBuilder {
	cb.cfg.WorkersCountCalculator = calculator
	cb.cfg.WorkersEvaluationInterval = interval
	return cb
}

// Done finalizes the consumer configuration and returns to the parent Config.
func (cb *ConsumerBuilder) Done() *Config {
	return cb.config
}

// ProducerBuilder builds a producer configuration.
type ProducerBuilder struct {
	config   *Config
	cfg      producer.ProducerConfig
	pipeline kafkaflow.Handler
}

// WithDefaultTopic sets the default topic for this producer.
func (pb *ProducerBuilder) WithDefaultTopic(topic string) *ProducerBuilder {
	pb.cfg.Topic = topic
	return pb
}

// WithCompressionType sets the native Kafka compression type (lz4, snappy, zstd, gzip).
func (pb *ProducerBuilder) WithCompressionType(compressionType string) *ProducerBuilder {
	pb.cfg.CompressionType = compressionType
	return pb
}

// WithAcks sets the producer acknowledgment level (0, 1, all).
func (pb *ProducerBuilder) WithAcks(acks string) *ProducerBuilder {
	pb.cfg.Acks = acks
	return pb
}

// WithMiddleware sets the middleware pipeline for this producer.
func (pb *ProducerBuilder) WithMiddleware(middlewares ...kafkaflow.Middleware) *ProducerBuilder {
	pb.pipeline = kafkaflow.ComposePipeline(middlewares, kafkaflow.NoopHandler)
	return pb
}

// Done finalizes the producer configuration and returns to the parent Config.
func (pb *ProducerBuilder) Done() *Config {
	return pb.config
}
