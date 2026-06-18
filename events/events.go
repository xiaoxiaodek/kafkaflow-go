package events

import (
	"context"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

// EventType represents the type of lifecycle event.
type EventType string

const (
	EventMessageProduced EventType = "message_produced"
	EventMessageConsumed EventType = "message_consumed"
	EventMessageError    EventType = "message_error"
	EventConsumerStarted EventType = "consumer_started"
	EventConsumerStopped EventType = "consumer_stopped"
	EventWorkerStarted   EventType = "worker_started"
	EventWorkerStopped   EventType = "worker_stopped"
)

// Event represents a lifecycle event.
type Event struct {
	Type    EventType
	Context *kafkaflow.MessageContext
	Error   error
}

// Handler is a function that handles lifecycle events.
type Handler func(ctx context.Context, event Event)

// Bus manages event subscriptions and dispatching.
type Bus struct {
	handlers map[EventType][]Handler
}

// NewBus creates a new event Bus.
func NewBus() *Bus {
	return &Bus{
		handlers: make(map[EventType][]Handler),
	}
}

// On registers a handler for a specific event type.
func (b *Bus) On(eventType EventType, handler Handler) {
	b.handlers[eventType] = append(b.handlers[eventType], handler)
}

// Emit dispatches an event to all registered handlers for its type.
func (b *Bus) Emit(ctx context.Context, event Event) {
	for _, handler := range b.handlers[event.Type] {
		handler(ctx, event)
	}
}
