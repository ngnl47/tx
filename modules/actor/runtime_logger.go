package actor

import (
	"context"
	"log/slog"

	txlog "github.com/ngnl47/tx/log"
)

// newRuntimeLogger 复用项目日志的格式和输出目标，只独立限制 ProtoActor 内部日志级别。
func newRuntimeLogger(systemID string, level LogLevel) *slog.Logger {
	handler := minimumLevelHandler{
		next:  txlog.Default().Handler(),
		level: level.slogLevel(),
	}
	return slog.New(handler).With(
		"lib", "Proto.Actor",
		"system", systemID,
	)
}

func (level LogLevel) slogLevel() slog.Level {
	switch level {
	case LogLevelDebug:
		return slog.LevelDebug
	case LogLevelWarn:
		return slog.LevelWarn
	case LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// minimumLevelHandler 在已有 Handler 外增加模块级阈值，不接管格式化和写入。
type minimumLevelHandler struct {
	next  slog.Handler
	level slog.Level
}

func (handler minimumLevelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= handler.level && handler.next.Enabled(ctx, level)
}

func (handler minimumLevelHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level < handler.level {
		return nil
	}
	return handler.next.Handle(ctx, record)
}

func (handler minimumLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minimumLevelHandler{next: handler.next.WithAttrs(attrs), level: handler.level}
}

func (handler minimumLevelHandler) WithGroup(name string) slog.Handler {
	return minimumLevelHandler{next: handler.next.WithGroup(name), level: handler.level}
}
