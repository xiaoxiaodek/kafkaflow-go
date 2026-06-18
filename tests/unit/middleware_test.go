package unit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

func TestComposePipeline_EmptyMiddleware(t *testing.T) {
	called := false
	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		called = true
		return nil
	}

	pipeline := kafkaflow.ComposePipeline(nil, final)
	err := pipeline(context.Background(), &kafkaflow.MessageContext{})

	assert.NoError(t, err)
	assert.True(t, called)
}

func TestComposePipeline_SingleMiddleware(t *testing.T) {
	order := []string{}

	mw := func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			order = append(order, "before")
			err := next(ctx, mc)
			order = append(order, "after")
			return err
		}
	}

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		order = append(order, "final")
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	err := pipeline(context.Background(), &kafkaflow.MessageContext{})

	assert.NoError(t, err)
	assert.Equal(t, []string{"before", "final", "after"}, order)
}

func TestComposePipeline_MultipleMiddleware(t *testing.T) {
	order := []string{}

	mw1 := func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			order = append(order, "mw1-before")
			err := next(ctx, mc)
			order = append(order, "mw1-after")
			return err
		}
	}

	mw2 := func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			order = append(order, "mw2-before")
			err := next(ctx, mc)
			order = append(order, "mw2-after")
			return err
		}
	}

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		order = append(order, "final")
		return nil
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw1, mw2}, final)
	err := pipeline(context.Background(), &kafkaflow.MessageContext{})

	assert.NoError(t, err)
	assert.Equal(t, []string{"mw1-before", "mw2-before", "final", "mw2-after", "mw1-after"}, order)
}

func TestComposePipeline_MiddlewareStopsOnError(t *testing.T) {
	order := []string{}
	testErr := errors.New("test error")

	mw := func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			order = append(order, "mw-before")
			err := next(ctx, mc)
			order = append(order, "mw-after")
			return err
		}
	}

	final := func(ctx context.Context, mc *kafkaflow.MessageContext) error {
		order = append(order, "final")
		return testErr
	}

	pipeline := kafkaflow.ComposePipeline([]kafkaflow.Middleware{mw}, final)
	err := pipeline(context.Background(), &kafkaflow.MessageContext{})

	assert.ErrorIs(t, err, testErr)
	assert.Equal(t, []string{"mw-before", "final", "mw-after"}, order)
}

func TestNoopHandler(t *testing.T) {
	err := kafkaflow.NoopHandler(context.Background(), &kafkaflow.MessageContext{})
	assert.NoError(t, err)
}
