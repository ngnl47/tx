package log

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"time"
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
	write(context.Background(), Default(), "", slog.LevelDebug, msg, args...)
}

func Info(msg string, args ...any) {
	write(context.Background(), Default(), "", slog.LevelInfo, msg, args...)
}

func Warn(msg string, args ...any) {
	write(context.Background(), Default(), "", slog.LevelWarn, msg, args...)
}

func Error(msg string, args ...any) {
	write(context.Background(), Default(), "", slog.LevelError, msg, args...)
}

func DebugContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), "", slog.LevelDebug, msg, args...)
}

func InfoContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), "", slog.LevelInfo, msg, args...)
}

func WarnContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), "", slog.LevelWarn, msg, args...)
}

func ErrorContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), "", slog.LevelError, msg, args...)
}

type Logger struct {
	name string
}

func NewLogger(name string) Logger {
	return Logger{name: name}
}

func (l Logger) Debug(msg string, args ...any) {
	write(context.Background(), Default(), l.name, slog.LevelDebug, msg, args...)
}

func (l Logger) Info(msg string, args ...any) {
	write(context.Background(), Default(), l.name, slog.LevelInfo, msg, args...)
}

func (l Logger) Warn(msg string, args ...any) {
	write(context.Background(), Default(), l.name, slog.LevelWarn, msg, args...)
}

func (l Logger) Error(msg string, args ...any) {
	write(context.Background(), Default(), l.name, slog.LevelError, msg, args...)
}

func (l Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), l.name, slog.LevelDebug, msg, args...)
}

func (l Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), l.name, slog.LevelInfo, msg, args...)
}

func (l Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), l.name, slog.LevelWarn, msg, args...)
}

func (l Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	write(ctx, Default(), l.name, slog.LevelError, msg, args...)
}

func write(ctx context.Context, logger *slog.Logger, name string, level slog.Level, msg string, args ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	handler := logger.Handler()
	if !handler.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	if name != "" {
		record.AddAttrs(slog.String("logger", name))
	}
	record.Add(args...)
	_ = handler.Handle(ctx, record)
}
