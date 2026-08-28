package tests

import (
	"context"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	txlog "github.com/ngnl47/tx/log"
)

func TestLogDefaultLoggerCanBeReplaced(t *testing.T) {
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	defer txlog.SetDefault(nil)

	txlog.Default().Error("save failed", slog.Int64("id", 1001), slog.String("err", "temporary"))

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("records = %#v, want one", records)
	}
	record := records[0]
	if record.Level != slog.LevelError || record.Message != "save failed" {
		t.Fatalf("record = %#v, want error save failed", record)
	}
	if !recordHasInt64Attr(record, "id", 1001) || !recordHasStringAttr(record, "err", "temporary") {
		t.Fatalf("record attrs = %#v", recordAttrs(record))
	}

	txlog.SetDefault(nil)
	if txlog.Default() == nil {
		t.Fatalf("default logger is nil after reset")
	}
}

func TestLogPackageLevelShortcutsUseDefaultLogger(t *testing.T) {
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	defer txlog.SetDefault(nil)

	txlog.Error("save failed", "id", int64(1001), "err", "temporary")

	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("records = %#v, want one", records)
	}
	record := records[0]
	if record.Level != slog.LevelError || record.Message != "save failed" {
		t.Fatalf("record = %#v, want error save failed", record)
	}
	if !recordHasInt64Attr(record, "id", 1001) || !recordHasStringAttr(record, "err", "temporary") {
		t.Fatalf("record attrs = %#v", recordAttrs(record))
	}
	frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
	if !strings.HasSuffix(filepath.ToSlash(frame.File), "/tests/log_test.go") {
		t.Fatalf("record source = %s, want tests/log_test.go", frame.File)
	}
}

func TestLogNamedLoggerUsesCurrentDefaultLogger(t *testing.T) {
	roleLog := txlog.NewLogger("role")
	firstHandler := &recordingSlogHandler{}
	secondHandler := &recordingSlogHandler{}
	defer txlog.SetDefault(nil)

	txlog.SetDefault(slog.New(firstHandler))
	roleLog.Info("role loaded", "id", int64(1001))

	txlog.SetDefault(slog.New(secondHandler))
	roleLog.ErrorContext(context.Background(), "role save failed", "id", int64(1002))

	firstRecords := firstHandler.Records()
	if len(firstRecords) != 1 {
		t.Fatalf("first records = %#v, want one", firstRecords)
	}
	first := firstRecords[0]
	if first.Level != slog.LevelInfo || first.Message != "role loaded" ||
		!recordHasStringAttr(first, "logger", "role") ||
		!recordHasInt64Attr(first, "id", 1001) {
		t.Fatalf("first record = %#v attrs=%#v", first, recordAttrs(first))
	}

	secondRecords := secondHandler.Records()
	if len(secondRecords) != 1 {
		t.Fatalf("second records = %#v, want one", secondRecords)
	}
	second := secondRecords[0]
	if second.Level != slog.LevelError || second.Message != "role save failed" ||
		!recordHasStringAttr(second, "logger", "role") ||
		!recordHasInt64Attr(second, "id", 1002) {
		t.Fatalf("second record = %#v attrs=%#v", second, recordAttrs(second))
	}
}

type recordingSlogHandler struct {
	mu      sync.Mutex
	records []slog.Record
	attrs   []slog.Attr
	sink    *recordingSlogHandler
}

func (h *recordingSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingSlogHandler) Handle(_ context.Context, record slog.Record) error {
	sink := h.recordSink()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	clone := record.Clone()
	if len(h.attrs) > 0 {
		clone.AddAttrs(h.attrs...)
	}
	sink.records = append(sink.records, clone)
	return nil
}

func (h *recordingSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	combined = append(combined, h.attrs...)
	combined = append(combined, attrs...)
	return &recordingSlogHandler{attrs: combined, sink: h.recordSink()}
}

func (h *recordingSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingSlogHandler) Records() []slog.Record {
	sink := h.recordSink()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]slog.Record(nil), sink.records...)
}

func (h *recordingSlogHandler) recordSink() *recordingSlogHandler {
	if h.sink != nil {
		return h.sink
	}
	return h
}

func recordHasStringAttr(record slog.Record, key string, value string) bool {
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key && attr.Value.String() == value {
			found = true
			return false
		}
		return true
	})
	return found
}

func recordHasInt64Attr(record slog.Record, key string, value int64) bool {
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key && attr.Value.Int64() == value {
			found = true
			return false
		}
		return true
	})
	return found
}

func recordAttrs(record slog.Record) []slog.Attr {
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	return attrs
}
