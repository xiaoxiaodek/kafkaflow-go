package consumer

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/events"
	"github.com/xiaoxiaodek/kafkaflow-go/log"
)

// WorkerStopTimeout is the maximum time a worker will wait to drain messages during shutdown.
const WorkerStopTimeout = 30 * time.Second

// TopicPartition identifies a topic and partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// OffsetStore tracks received and processed offsets per partition for a single worker.
type OffsetStore struct {
	mu          sync.Mutex
	tracked     map[TopicPartition]map[int64]bool
	committable map[TopicPartition]int64
}

// NewOffsetStore creates a new OffsetStore.
func NewOffsetStore() *OffsetStore {
	return &OffsetStore{
		tracked:     make(map[TopicPartition]map[int64]bool),
		committable: make(map[TopicPartition]int64),
	}
}

// Track records a received offset before processing starts.
func (s *OffsetStore) Track(topic string, partition int32, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tp := TopicPartition{Topic: topic, Partition: partition}
	if _, ok := s.tracked[tp]; !ok {
		s.tracked[tp] = make(map[int64]bool)
	}
	s.tracked[tp][offset] = false
}

// Store records a processed offset for a topic/partition.
func (s *OffsetStore) Store(topic string, partition int32, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tp := TopicPartition{Topic: topic, Partition: partition}
	if _, ok := s.tracked[tp]; !ok {
		s.tracked[tp] = make(map[int64]bool)
	}
	s.tracked[tp][offset] = true
	s.advance(tp)
}

// Snapshot returns a copy of all committable contiguous offsets.
func (s *OffsetStore) Snapshot() map[TopicPartition]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := make(map[TopicPartition]int64, len(s.committable))
	for k, v := range s.committable {
		snapshot[k] = v
	}
	return snapshot
}

func (s *OffsetStore) advance(tp TopicPartition) {
	offsets := s.tracked[tp]
	current, ok := s.committable[tp]
	if !ok {
		minTrackedSet := false
		minTracked := int64(0)
		for offset := range offsets {
			if !minTrackedSet || offset < minTracked {
				minTracked = offset
				minTrackedSet = true
			}
		}
		if !minTrackedSet || !offsets[minTracked] {
			return
		}
		current = minTracked
		s.committable[tp] = current
	}

	for {
		next := current + 1
		processed, exists := offsets[next]
		if !exists || !processed {
			return
		}
		current = next
		s.committable[tp] = current
	}
}

// Worker processes messages sequentially from a channel through the middleware pipeline.
type Worker struct {
	id          int
	channel     chan *kafkaflow.MessageContext
	pipeline    kafkaflow.Handler
	offsetStore *OffsetStore
	eventBus    *events.Bus
	logger      log.Logger
}

// NewWorker creates a new Worker.
func NewWorker(id int, bufferSize int, pipeline kafkaflow.Handler) *Worker {
	return &Worker{
		id:          id,
		channel:     make(chan *kafkaflow.MessageContext, bufferSize),
		pipeline:    pipeline,
		offsetStore: NewOffsetStore(),
	}
}

// ID returns the worker's identifier.
func (w *Worker) ID() int {
	return w.id
}

// Channel returns the worker's message channel for the dispatcher to send to.
func (w *Worker) Channel() chan<- *kafkaflow.MessageContext {
	return w.channel
}

// PendingCount returns the number of messages waiting in the channel.
func (w *Worker) PendingCount() int {
	return len(w.channel)
}

// OffsetStore returns the worker's offset store.
func (w *Worker) OffsetStore() *OffsetStore {
	return w.offsetStore
}

// SetEventBus sets the event bus for lifecycle events.
func (w *Worker) SetEventBus(eb *events.Bus) {
	w.eventBus = eb
}

// SetLogger sets the logger for the worker.
func (w *Worker) SetLogger(l log.Logger) {
	w.logger = l
}

// Run starts the worker's processing loop. Blocks until ctx is cancelled and channel is drained.
func (w *Worker) Run(ctx context.Context) error {
	defer w.recoverPanic(ctx)

	for {
		select {
		case <-ctx.Done():
			return w.drainWithTimeout()
		case mc, ok := <-w.channel:
			if !ok {
				return nil
			}
			if mc == nil {
				continue
			}
			w.processMessage(ctx, mc)
		}
	}
}

func (w *Worker) processMessage(ctx context.Context, mc *kafkaflow.MessageContext) {
	if mc.ConsumerContext != nil {
		w.offsetStore.Track(
			mc.ConsumerContext.Topic,
			mc.ConsumerContext.Partition,
			mc.ConsumerContext.Offset,
		)
	}

	w.emitEvent(ctx, events.EventMessageConsumed, mc, nil)

	if err := w.pipeline(ctx, mc); err != nil {
		w.emitEvent(ctx, events.EventMessageError, mc, err)
		if w.logger != nil {
			w.logger.Error(ctx, "worker pipeline error",
				"worker_id", w.id,
				"topic", mc.ConsumerContext.Topic,
				"partition", mc.ConsumerContext.Partition,
				"offset", mc.ConsumerContext.Offset,
				"error", err,
			)
		}
		return
	}

	if mc.ConsumerContext != nil {
		w.offsetStore.Store(
			mc.ConsumerContext.Topic,
			mc.ConsumerContext.Partition,
			mc.ConsumerContext.Offset,
		)
	}
}

func (w *Worker) emitEvent(ctx context.Context, eventType events.EventType, mc *kafkaflow.MessageContext, err error) {
	if w.eventBus == nil {
		return
	}
	w.eventBus.Emit(ctx, events.Event{
		Type:    eventType,
		Context: mc,
		Error:   err,
	})
}

func (w *Worker) recoverPanic(ctx context.Context) {
	if r := recover(); r != nil {
		stack := string(debug.Stack())
		if w.logger != nil {
			w.logger.Error(ctx, "worker panic recovered",
				"worker_id", w.id,
				"panic", fmt.Sprintf("%v", r),
				"stack", stack,
			)
		}
	}
}

func (w *Worker) drainWithTimeout() error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.drain()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(WorkerStopTimeout):
		if w.logger != nil {
			w.logger.Warn(context.Background(), "worker drain timed out",
				"worker_id", w.id,
				"timeout", WorkerStopTimeout,
			)
		}
		return nil
	}
}

func (w *Worker) drain() {
	for {
		select {
		case mc, ok := <-w.channel:
			if !ok {
				return
			}
			if mc == nil {
				continue
			}
			w.processMessage(context.Background(), mc)
		default:
			return
		}
	}
}
