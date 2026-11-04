// Package log 提供基于 slog 的进程日志入口及命名记录器，不创建或关闭日志输出资源。
// 应用通过 SetDefault 选择输出；未设置时使用 slog.Default，字段和过滤策略由其 Handler 决定。
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

// SetDefault 替换进程默认记录器，传 nil 时恢复使用 slog.Default。
// 它不关闭旧输出资源；已有 Logger 也会在下一次记录时读取新的默认记录器。
func SetDefault(logger *slog.Logger) {
	defaultMu.Lock()
	defaultLogger = logger
	defaultMu.Unlock()
}

// Default 返回当前默认记录器；未设置时回退 slog.Default。
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

// Logger 只保存日志分类名，不持有独立输出资源。
type Logger struct {
	name string
}

// NewLogger 创建附带 logger 分类字段的轻量记录器，共用进程默认输出。
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
