package kafkaflow

// ConsumerContext holds metadata about the consumer side of a message.
type ConsumerContext struct {
	Topic     string
	Partition int32
	Offset    int64
	GroupID   string
	WorkerID  int
}

// ProducerContext holds metadata about the producer side of a message.
type ProducerContext struct {
	Topic        string
	PartitionKey []byte
}

// MessageContext carries a message through the middleware pipeline.
// It holds consumer or producer metadata, the message itself, a scoped
// key-value store for middleware communication, and a DI resolver.
type MessageContext struct {
	ConsumerContext *ConsumerContext
	ProducerContext *ProducerContext
	Message         *Message
	Items           map[string]any
	Resolver        Resolver
}

// NewConsumerContext creates a MessageContext for a consumed message.
func NewConsumerContext(cc *ConsumerContext, msg *Message, resolver Resolver) *MessageContext {
	return &MessageContext{
		ConsumerContext: cc,
		Message:         msg,
		Items:           make(map[string]any),
		Resolver:        resolver,
	}
}

// NewProducerContext creates a MessageContext for a produced message.
func NewProducerContext(pc *ProducerContext, msg *Message, resolver Resolver) *MessageContext {
	return &MessageContext{
		ProducerContext: pc,
		Message:         msg,
		Items:           make(map[string]any),
		Resolver:        resolver,
	}
}

// GetItem retrieves a scoped value by key.
func (mc *MessageContext) GetItem(key string) (any, bool) {
	v, ok := mc.Items[key]
	return v, ok
}

// SetItem stores a scoped value by key.
func (mc *MessageContext) SetItem(key string, value any) {
	mc.Items[key] = value
}
