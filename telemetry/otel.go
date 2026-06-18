package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	kafkaflow "github.com/xiaoxiaodek/kafkaflow-go"
)

const tracerName = "github.com/xiaoxiaodek/kafkaflow-go"

func ConsumerTracingMiddleware() kafkaflow.Middleware {
	tracer := otel.Tracer(tracerName)
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.ConsumerContext == nil {
				return next(ctx, mc)
			}

			attrs := []attribute.KeyValue{
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", mc.ConsumerContext.Topic),
				attribute.String("messaging.kafka.consumer_group", mc.ConsumerContext.GroupID),
				attribute.Int("messaging.kafka.partition", int(mc.ConsumerContext.Partition)),
				attribute.Int64("messaging.kafka.offset", mc.ConsumerContext.Offset),
			}

			ctx, span := tracer.Start(ctx, mc.ConsumerContext.Topic+" receive",
				trace.WithAttributes(attrs...),
				trace.WithSpanKind(trace.SpanKindConsumer),
			)
			defer span.End()

			err := next(ctx, mc)
			if err != nil {
				span.RecordError(err)
			}
			return err
		}
	}
}

func ProducerTracingMiddleware() kafkaflow.Middleware {
	tracer := otel.Tracer(tracerName)
	return func(next kafkaflow.Handler) kafkaflow.Handler {
		return func(ctx context.Context, mc *kafkaflow.MessageContext) error {
			if mc.ProducerContext == nil {
				return next(ctx, mc)
			}

			attrs := []attribute.KeyValue{
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", mc.ProducerContext.Topic),
			}

			ctx, span := tracer.Start(ctx, mc.ProducerContext.Topic+" send",
				trace.WithAttributes(attrs...),
				trace.WithSpanKind(trace.SpanKindProducer),
			)
			defer span.End()

			err := next(ctx, mc)
			if err != nil {
				span.RecordError(err)
			}
			return err
		}
	}
}
