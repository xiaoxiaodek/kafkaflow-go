package middleware

import (
	"context"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/serializer"
)

const MessageTypeHeader = "message-type"

// MessageTypeResolver resolves the target type for deserialization.
// C# equivalent: IMessageTypeResolver
type MessageTypeResolver interface {
	// OnConsume resolves the target type from message headers and context.
	OnConsume(ctx context.Context, mc *kafkaflow.MessageContext) (interface{}, error)
	// OnProduce sets the message type header on the message context.
	OnProduce(ctx context.Context, mc *kafkaflow.MessageContext, value interface{}) error
}

// SingleMessageTypeResolver always resolves to a fixed type.
// C# equivalent: SingleMessageTypeResolver
type SingleMessageTypeResolver struct {
	TypeName string
	Factory  func() interface{}
}

// NewSingleMessageTypeResolver creates a resolver for a single message type.
func NewSingleMessageTypeResolver(typeName string, factory func() interface{}) *SingleMessageTypeResolver {
	return &SingleMessageTypeResolver{TypeName: typeName, Factory: factory}
}

func (r *SingleMessageTypeResolver) OnConsume(ctx context.Context, mc *kafkaflow.MessageContext) (interface{}, error) {
	return r.Factory(), nil
}

func (r *SingleMessageTypeResolver) OnProduce(ctx context.Context, mc *kafkaflow.MessageContext, value interface{}) error {
	mc.Message.Headers = append(mc.Message.Headers, kafkaflow.Header{
		Key:   MessageTypeHeader,
		Value: []byte(r.TypeName),
	})
	return nil
}

// Serializer creates a producer middleware that serializes the message value.
func Serializer(s serializer.Serializer) kafkaflow.Middleware {
	return SerializerWithResolver(s, nil)
}

// SerializerWithResolver creates a producer middleware that serializes the message value
// and uses a type resolver to set the message-type header.
func SerializerWithResolver(s serializer.Serializer, resolver MessageTypeResolver) kafkaflow.Middleware {
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.Message.Value != nil {
				if resolver != nil {
					if err := resolver.OnProduce(ctx, mc, mc.Message.Value); err != nil {
						return err
					}
				}
				data, err := s.Serialize(mc.Message.Value)
				if err != nil {
					return kafkaflow.ErrSerializationFailed
				}
				mc.Message.Value = data
			}
			return next(ctx, mc)
		}
	}
}

// Deserializer creates a consumer middleware that deserializes the message value.
func Deserializer(d serializer.Deserializer) kafkaflow.Middleware {
	return DeserializerWithResolver(d, nil)
}

// DeserializerWithResolver creates a consumer middleware that deserializes the message value
// using a type resolver to determine the target type.
func DeserializerWithResolver(d serializer.Deserializer, resolver MessageTypeResolver) kafkaflow.Middleware {
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.Message.Value != nil {
				data, ok := mc.Message.Value.([]byte)
				if !ok {
					return kafkaflow.ErrSerializationFailed
				}

				var target interface{}
				if resolver != nil {
					var err error
					target, err = resolver.OnConsume(ctx, mc)
					if err != nil {
						return err
					}
				} else {
					var v interface{}
					target = &v
				}

				if err := d.Deserialize(data, target); err != nil {
					return kafkaflow.ErrSerializationFailed
				}

				if resolver != nil {
					mc.Message.Value = target
				} else {
					mc.Message.Value = *target.(*interface{})
				}
			}
			return next(ctx, mc)
		}
	}
}
