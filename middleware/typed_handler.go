package middleware

import (
	"context"
	"encoding/json"
	"fmt"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

type MessageHandler[T any] interface {
	Handle(ctx context.Context, mc *kafkaflow.MessageContext, payload T) error
}

type TypedHandlerConfig struct {
	OnNoHandlerFound func(ctx context.Context, mc *kafkaflow.MessageContext)
}

func TypedHandler[T any](handler MessageHandler[T]) kafkaflow.Middleware {
	return TypedHandlersWithConfig([]MessageHandler[T]{handler}, nil)
}

func TypedHandlers[T any](handlers []MessageHandler[T]) kafkaflow.Middleware {
	return TypedHandlersWithConfig(handlers, nil)
}

func TypedHandlersWithConfig[T any](handlers []MessageHandler[T], cfg *TypedHandlerConfig) kafkaflow.Middleware {
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			var payload T
			if typed, ok := mc.Message.Value.(T); ok {
				payload = typed
			} else {
				data, ok := mc.Message.Value.([]byte)
				if !ok {
					return fmt.Errorf("%w: typed handler expected %T or []byte, got %T", kafkaflow.ErrSerializationFailed, payload, mc.Message.Value)
				}
				if err := json.Unmarshal(data, &payload); err != nil {
					return fmt.Errorf("%w: failed to deserialize %T: %v", kafkaflow.ErrSerializationFailed, payload, err)
				}
			}

			if len(handlers) == 0 {
				if cfg != nil && cfg.OnNoHandlerFound != nil {
					cfg.OnNoHandlerFound(ctx, mc)
				}
				return next(ctx, mc)
			}

			for _, handler := range handlers {
				if err := handler.Handle(ctx, mc, payload); err != nil {
					return err
				}
			}
			return next(ctx, mc)
		}
	}
}
