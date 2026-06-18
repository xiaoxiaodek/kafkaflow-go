package middleware

import (
	"context"
	"fmt"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/compressor"
)

// Compressor creates a producer middleware that compresses the message value.
func Compressor(c compressor.Compressor) kafkaflow.Middleware {
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.Message.Value != nil {
				value, ok := mc.Message.Value.([]byte)
				if !ok {
					return fmt.Errorf("%w: compressor expected []byte, got %T", kafkaflow.ErrCompressionFailed, mc.Message.Value)
				}
				data, err := c.Compress(value)
				if err != nil {
					return fmt.Errorf("%w: %v", kafkaflow.ErrCompressionFailed, err)
				}
				mc.Message.Value = data
			}
			return next(ctx, mc)
		}
	}
}

// Decompressor creates a consumer middleware that decompresses the message value.
func Decompressor(d compressor.Decompressor) kafkaflow.Middleware {
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.Message.Value != nil {
				value, ok := mc.Message.Value.([]byte)
				if !ok {
					return fmt.Errorf("%w: decompressor expected []byte, got %T", kafkaflow.ErrCompressionFailed, mc.Message.Value)
				}
				data, err := d.Decompress(value)
				if err != nil {
					return fmt.Errorf("%w: %v", kafkaflow.ErrCompressionFailed, err)
				}
				mc.Message.Value = data
			}
			return next(ctx, mc)
		}
	}
}
