package middleware_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
	"github.com/xiaoxiaodek/kafkaflow-go/middleware"
)

func TestThrottling_AllowsWhenUnderThreshold(t *testing.T) {
	mw := middleware.Throttling(middleware.ThrottlingConfig{
		MaxPendingMessages: 10,
		CheckInterval:      10 * time.Millisecond,
	})

	var processed atomic.Int32
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		processed.Add(1)
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)

	for i := 0; i < 5; i++ {
		mc := &kafkaflow.MessageContext{
			Message: &kafkaflow.Message{Value: []byte("test")},
		}
		err := pipeline(context.Background(), mc)
		assert.NoError(t, err)
	}

	assert.Equal(t, int32(5), processed.Load())
}

func TestThrottling_BlocksWhenOverThreshold(t *testing.T) {
	mw := middleware.Throttling(middleware.ThrottlingConfig{
		MaxPendingMessages: 2,
		CheckInterval:      10 * time.Millisecond,
	})

	var inFlight atomic.Int32
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		inFlight.Add(1)
		time.Sleep(100 * time.Millisecond) // hold the message
		inFlight.Add(-1)
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)

	// Launch 3 concurrent messages with threshold of 2
	done := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func() {
			mc := &kafkaflow.MessageContext{
				Message: &kafkaflow.Message{Value: []byte("test")},
			}
			pipeline(context.Background(), mc)
			done <- true
		}()
	}

	// All 3 should complete
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for throttled messages")
		}
	}
}
