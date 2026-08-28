package framework

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	txlog "github.com/ngnl47/tx/log"
)

func TestConfigureLoggerUsesLevelAndFormat(t *testing.T) {
	var output bytes.Buffer
	if err := configureLogger(LogConfig{Level: "warn", Format: "console", Output: &output}); err != nil {
		t.Fatalf("configureLogger returned error: %v", err)
	}
	t.Cleanup(func() { txlog.SetDefault(nil) })
	txlog.Info("hidden info")
	writeConsoleLogForTest()

	text := output.String()
	if strings.Contains(text, "hidden info") {
		t.Fatalf("log contains filtered info: %s", text)
	}
	pattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \[WARN\] \[framework/logging_test\.go:\d+\] (\{.*\})\n$`)
	match := pattern.FindStringSubmatch(text)
	if len(match) != 2 {
		t.Fatalf("console log = %q", text)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(match[1]), &event); err != nil {
		t.Fatalf("decode console event: %v", err)
	}
	if event["message"] != "visible warn" || event["role_id"] != float64(1001) || event["error"] != "database unavailable" {
		t.Fatalf("console event = %#v", event)
	}
}

func TestConfigureLoggerUsesJSONLines(t *testing.T) {
	var output bytes.Buffer
	if err := configureLogger(LogConfig{Level: "debug", Format: "json", Output: &output}); err != nil {
		t.Fatalf("configureLogger returned error: %v", err)
	}
	t.Cleanup(func() { txlog.SetDefault(nil) })
	txlog.Debug("visible debug", "role_id", int64(1001), "error", errors.New("database unavailable"))

	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("JSON log lines = %d, want one: %q", len(lines), output.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("decode JSON log: %v", err)
	}
	if event["level"] != "DEBUG" || event["message"] != "visible debug" || event["role_id"] != float64(1001) || event["error"] != "database unavailable" {
		t.Fatalf("JSON event = %#v", event)
	}
	source, ok := event["source"].(string)
	if !ok || !regexp.MustCompile(`^framework/logging_test\.go:\d+$`).MatchString(source) {
		t.Fatalf("JSON source = %#v", event["source"])
	}
	if _, ok := event["time"].(string); !ok {
		t.Fatalf("JSON time = %#v", event["time"])
	}
}

func writeConsoleLogForTest() {
	txlog.Warn("visible warn", "role_id", int64(1001), "error", errors.New("database unavailable"))
}

func TestConfigureLoggerRejectsInvalidConfig(t *testing.T) {
	if err := configureLogger(LogConfig{Level: "trace"}); err == nil || !strings.Contains(err.Error(), "log level") {
		t.Fatalf("level error = %v, want log level error", err)
	}
	if err := configureLogger(LogConfig{Format: "xml"}); err == nil || !strings.Contains(err.Error(), "log format") {
		t.Fatalf("format error = %v, want log format error", err)
	}
}
