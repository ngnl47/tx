package log

import (
	"context"
	"log/slog"
	"sync"
)

var (
	defaultMu     sync.RWMutex
	defaultLogger *slog.Logger
)

func SetDefault(logger *slog.Logger) {
	defaultMu.Lock()
	defaultLogger = logger
	defaultMu.Unlock()
}

func Default() *slog.Logger {
	defaultMu.RLock()
	logger := defaultLogger
	defaultMu.RUnlock()
	if logger == nil {
		return slog.Default()
	}
	return logger
}

func Debug(msg string, args ...any) {
	Default().Debug(msg, args...)
}

func Info(msg string, args ...any) {
	Default().Info(msg, args...)
}

func Warn(msg string, args ...any) {
	Default().Warn(msg, args...)
}

func Error(msg string, args ...any) {
	Default().Error(msg, args...)
}

func DebugContext(ctx context.Context, msg string, args ...any) {
	Default().DebugContext(ctx, msg, args...)
}

func InfoContext(ctx context.Context, msg string, args ...any) {
	Default().InfoContext(ctx, msg, args...)
}

func WarnContext(ctx context.Context, msg string, args ...any) {
	Default().WarnContext(ctx, msg, args...)
}

func ErrorContext(ctx context.Context, msg string, args ...any) {
	Default().ErrorContext(ctx, msg, args...)
}

type Logger struct {
	name string
}

func NewLogger(name string) Logger {
	return Logger{name: name}
}

func (l Logger) Debug(msg string, args ...any) {
	l.defaultLogger().Debug(msg, args...)
}

func (l Logger) Info(msg string, args ...any) {
	l.defaultLogger().Info(msg, args...)
}

func (l Logger) Warn(msg string, args ...any) {
	l.defaultLogger().Warn(msg, args...)
}

func (l Logger) Error(msg string, args ...any) {
	l.defaultLogger().Error(msg, args...)
}

func (l Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.defaultLogger().DebugContext(ctx, msg, args...)
}

func (l Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.defaultLogger().InfoContext(ctx, msg, args...)
}

func (l Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.defaultLogger().WarnContext(ctx, msg, args...)
}

func (l Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.defaultLogger().ErrorContext(ctx, msg, args...)
}

func (l Logger) defaultLogger() *slog.Logger {
	logger := Default()
	if l.name == "" {
		return logger
	}
	return logger.With(slog.String("logger", l.name))
}
