package middleware

import (
	"context"
	"sync"
	"time"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

const BatchPayloadKey = "kafkaflow:batch_payload"

type BatchHandler[T any] interface {
	HandleBatch(ctx context.Context, messages []BatchMessage[T]) error
}

type BatchMessage[T any] struct {
	Context *kafkaflow.MessageContext
	Payload T
}

type BatchConsumeConfig struct {
	BatchSize   int
	MaxWaitTime time.Duration
}

// batchState holds the per-instance state for a BatchConsume middleware.
type batchState[T any] struct {
	mu      sync.Mutex
	batch   []BatchMessage[T]
	timer   *time.Timer
	timerCh <-chan time.Time
}

func BatchConsume[T any](handler BatchHandler[T], cfg BatchConsumeConfig) kafkaflow.Middleware {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxWaitTime <= 0 {
		cfg.MaxWaitTime = 5 * time.Second
	}

	state := &batchState[T]{}

	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			var payload T
			if v, ok := mc.GetItem(BatchPayloadKey); ok {
				if p, ok := v.(T); ok {
					payload = p
				}
			}

			state.mu.Lock()
			state.batch = append(state.batch, BatchMessage[T]{Context: mc, Payload: payload})

			if len(state.batch) >= cfg.BatchSize {
				b := state.batch
				state.batch = nil
				if state.timer != nil {
					state.timer.Stop()
					state.timer = nil
				}
				state.timerCh = nil
				state.mu.Unlock()
				return handler.HandleBatch(ctx, b)
			}

			if state.timer == nil {
				state.timer = time.NewTimer(cfg.MaxWaitTime)
				state.timerCh = state.timer.C
			}
			state.mu.Unlock()

			select {
			case <-state.timerCh:
				state.mu.Lock()
				b := state.batch
				state.batch = nil
				state.timer = nil
				state.timerCh = nil
				state.mu.Unlock()
				if len(b) > 0 {
					return handler.HandleBatch(ctx, b)
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			return next(ctx, mc)
		}
	}
}
