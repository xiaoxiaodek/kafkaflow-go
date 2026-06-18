package middleware

import (
	"context"
	"sync/atomic"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

// ThrottlingConfig configures consumer throttling behavior.
type ThrottlingConfig struct {
	MaxPendingMessages int64
	MaxPendingBytes    int64
	CheckInterval      time.Duration
}

// Throttling creates a middleware that throttles consumption based on thresholds.
// When pending messages or bytes exceed the configured limits, the middleware
// delays processing until the thresholds are met.
func Throttling(cfg ThrottlingConfig) kafkaflow.Middleware {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 100 * time.Millisecond
	}

	var (
		pendingMessages atomic.Int64
		pendingBytes    atomic.Int64
	)

	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			pendingMessages.Add(1)
			messageBytes := valueSize(mc.Message.Value)
			pendingBytes.Add(messageBytes)
			defer func() {
				pendingMessages.Add(-1)
				pendingBytes.Add(-messageBytes)
			}()

			// Wait if thresholds exceeded
			for {
				if cfg.MaxPendingMessages > 0 && pendingMessages.Load() > cfg.MaxPendingMessages {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(cfg.CheckInterval):
						continue
					}
				}
				if cfg.MaxPendingBytes > 0 && pendingBytes.Load() > cfg.MaxPendingBytes {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(cfg.CheckInterval):
						continue
					}
				}
				break
			}

			return next(ctx, mc)
		}
	}
}

func valueSize(value interface{}) int64 {
	switch v := value.(type) {
	case []byte:
		return int64(len(v))
	case string:
		return int64(len(v))
	default:
		return 0
	}
}
