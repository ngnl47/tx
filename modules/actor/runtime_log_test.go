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

	system := newActorSystem()
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
