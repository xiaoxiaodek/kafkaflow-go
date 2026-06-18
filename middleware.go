package kafkaflow

import "context"

type Handler func(ctx context.Context, mc *MessageContext) error

type Middleware func(next Handler) Handler

func ComposePipeline(middlewares []Middleware, final Handler) Handler {
	h := final
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func NoopHandler(ctx context.Context, mc *MessageContext) error {
	return nil
}
