package framework

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	txlog "github.com/ngnl47/tx/log"
)

func configureLogger(config LogConfig) error {
	output := config.Output
	if output == nil {
		output = os.Stdout
	}
	level, err := parseLogLevel(config.Level)
	if err != nil {
		return err
	}
	options := &slog.HandlerOptions{
		AddSource:   true,
		Level:       level,
		ReplaceAttr: replaceJSONLogAttr,
	}
	switch strings.ToLower(strings.TrimSpace(config.Format)) {
	case "", "console":
		txlog.SetDefault(slog.New(newConsoleHandler(output, level)))
	case "json":
		txlog.SetDefault(slog.New(slog.NewJSONHandler(output, options)))
	default:
		return fmt.Errorf("framework: unknown log format %q", config.Format)
	}
	return nil
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("framework: unknown log level %q", value)
	}
}
