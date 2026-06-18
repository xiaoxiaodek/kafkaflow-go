package kafkaflow

import "errors"

var (
	ErrMiddlewareChainBroken = errors.New("kafkaflow: middleware chain broken")
	ErrSerializationFailed   = errors.New("kafkaflow: serialization failed")
	ErrCompressionFailed     = errors.New("kafkaflow: compression failed")
	ErrConsumerStopped       = errors.New("kafkaflow: consumer stopped")
	ErrProducerClosed        = errors.New("kafkaflow: producer closed")
	ErrNoHandlerRegistered   = errors.New("kafkaflow: no handler registered for message type")
	ErrInvalidConfiguration  = errors.New("kafkaflow: invalid configuration")
)
