package actor

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	txlog "github.com/ngnl47/tx/log"
)

func TestNewActorSystemUsesConfiguredLogger(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	system := newActorSystem(LogLevelInfo)
	if system == nil {
		t.Fatal("newActorSystem returned nil")
	}
	t.Cleanup(system.Shutdown)

	line := strings.TrimSpace(output.String())
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("decode actor startup log: %v; log=%q", err, line)
	}
	if got := event["msg"]; got != "actor system started" {
		t.Fatalf("message = %v, want actor system started", got)
	}
	if got := event["lib"]; got != "Proto.Actor" {
		t.Fatalf("lib = %v, want Proto.Actor", got)
	}
	if got := event["system"]; got != system.ID {
		t.Fatalf("system = %v, want %s", got, system.ID)
	}
}

func TestActorLoggerUsesIndependentMinimumLevel(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	logger := newRuntimeLogger("test-system", LogLevelInfo)
	logger.Debug("gossip heartbeat")
	logger.Info("member joined")

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("actor log lines = %d, want 1; log=%q", len(lines), output.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("decode actor log: %v; log=%q", err, lines[0])
	}
	if got := event["msg"]; got != "member joined" {
		t.Fatalf("message = %v, want member joined", got)
	}
}

func TestActorLoggerCanEnableDebugExplicitly(t *testing.T) {
	var output bytes.Buffer
	txlog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	newRuntimeLogger("test-system", LogLevelDebug).Debug("gossip heartbeat")

	if !strings.Contains(output.String(), "gossip heartbeat") {
		t.Fatalf("actor debug log was filtered: %q", output.String())
	}
}
