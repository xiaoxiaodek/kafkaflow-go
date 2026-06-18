package builder

import (
	"context"
	"sync"

	"github.com/xiaoxiaodek/kafkaflow-go/admin"
	"github.com/xiaoxiaodek/kafkaflow-go/admin/api"
	"github.com/xiaoxiaodek/kafkaflow-go/consumer"
	"github.com/xiaoxiaodek/kafkaflow-go/events"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
	"github.com/xiaoxiaodek/kafkaflow-go/producer"
	"golang.org/x/sync/errgroup"
)

// Bus is the central orchestrator that manages all consumers and producers.
type Bus struct {
	consumers          []*consumer.ConsumerManager
	producerAccessor   *producer.ProducerAccessor
	logger             log.Logger
	eg                 *errgroup.Group
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.Mutex
	running            bool
	adminConsumer      *admin.AdminConsumer
	adminProducer      *admin.AdminProducer
	consumerAdmin      *admin.ConsumerAdmin
	telemetryStorage   admin.TelemetryStorage
	telemetryScheduler *admin.TelemetryScheduler
	telemetryConsumer  *admin.TelemetryConsumerService
	adminAPIServer     *api.Server
	adminAPIAddr       string
	eventBus           *events.Bus
}

// NewBus creates a new Bus.
func NewBus(consumers []*consumer.ConsumerManager, producerAccessor *producer.ProducerAccessor, logger log.Logger) *Bus {
	if logger == nil {
		logger = log.DefaultLogger()
	}
	return &Bus{
		consumers:        consumers,
		producerAccessor: producerAccessor,
		logger:           logger,
		eventBus:         events.NewBus(),
	}
}

// Start launches all consumers. Blocks until ctx is cancelled or a consumer fails.
func (b *Bus) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return nil
	}
	b.running = true
	b.mu.Unlock()

	b.ctx, b.cancel = context.WithCancel(ctx)
	b.eg, b.ctx = errgroup.WithContext(b.ctx)

	for _, cm := range b.consumers {
		cm := cm
		b.eg.Go(func() error {
			b.eventBus.Emit(b.ctx, events.Event{Type: events.EventConsumerStarted})
			err := cm.Start(b.ctx)
			b.eventBus.Emit(b.ctx, events.Event{Type: events.EventConsumerStopped})
			return err
		})
	}

	if b.adminConsumer != nil {
		b.eg.Go(func() error {
			return b.adminConsumer.Start(b.ctx)
		})
	}

	if b.telemetryScheduler != nil {
		b.telemetryScheduler.Start()
	}

	if b.telemetryConsumer != nil {
		b.eg.Go(func() error {
			return b.telemetryConsumer.Start(b.ctx)
		})
	}

	if b.adminAPIServer != nil {
		b.eg.Go(func() error {
			return b.adminAPIServer.Run(b.adminAPIAddr)
		})
	}

	b.logger.Info(ctx, "kafkaflow bus started", "consumers", len(b.consumers))
	return b.eg.Wait()
}

// Stop gracefully stops all consumers and producers.
func (b *Bus) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cancel != nil {
		b.cancel()
	}

	for _, cm := range b.consumers {
		cm.Stop()
	}

	if b.producerAccessor != nil {
		b.producerAccessor.Close()
	}

	if b.telemetryScheduler != nil {
		b.telemetryScheduler.Stop()
	}

	b.running = false
}

// ProducerAccessor returns the producer accessor for creating/getting producers.
func (b *Bus) ProducerAccessor() *producer.ProducerAccessor {
	return b.producerAccessor
}

// EventBus returns the event bus for subscribing to lifecycle events.
func (b *Bus) EventBus() *events.Bus {
	return b.eventBus
}

// GetByName returns a consumer by name.
func (b *Bus) GetByName(name string) (*consumer.ConsumerManager, bool) {
	for _, cm := range b.consumers {
		if cm.Name() == name {
			return cm, true
		}
	}
	return nil, false
}

// GetByGroup returns all consumers in a group.
func (b *Bus) GetByGroup(groupID string) []*consumer.ConsumerManager {
	var result []*consumer.ConsumerManager
	for _, cm := range b.consumers {
		if cm.GroupID() == groupID {
			result = append(result, cm)
		}
	}
	return result
}

// AllConsumers returns all consumer managers.
func (b *Bus) AllConsumers() []*consumer.ConsumerManager {
	return b.consumers
}
