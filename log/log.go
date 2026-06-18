package log

import (
	"context"
	"log/slog"
)

// Logger is the logging abstraction used throughout KafkaFlow.
// The default implementation wraps slog.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

// SlogLogger wraps the standard library slog.Logger.
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a Logger backed by slog.
func NewSlogLogger(logger *slog.Logger) Logger {
	return &SlogLogger{logger: logger}
}

func (l *SlogLogger) Debug(ctx context.Context, msg string, args ...any) {
	l.logger.DebugContext(ctx, msg, args...)
}

func (l *SlogLogger) Info(ctx context.Context, msg string, args ...any) {
	l.logger.InfoContext(ctx, msg, args...)
}

func (l *SlogLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.logger.WarnContext(ctx, msg, args...)
}

func (l *SlogLogger) Error(ctx context.Context, msg string, args ...any) {
	l.logger.ErrorContext(ctx, msg, args...)
}

// DefaultLogger returns a Logger using the default slog handler.
func DefaultLogger() Logger {
	return NewSlogLogger(slog.Default())
}
