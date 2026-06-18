package kafkaflow

type MiddlewareLifetime int

const (
	MiddlewareLifetimeMessage          MiddlewareLifetime = iota
	MiddlewareLifetimeWorker
	MiddlewareLifetimeConsumerOrProducer
	MiddlewareLifetimeSingleton
)

type MiddlewareFactory func() Middleware

func ComposePipelineWithLifetimes(factories []MiddlewareFactory, final Handler, lifetime MiddlewareLifetime) Handler {
	middlewares := make([]Middleware, len(factories))
	for i, f := range factories {
		middlewares[i] = f()
	}
	return ComposePipeline(middlewares, final)
}
