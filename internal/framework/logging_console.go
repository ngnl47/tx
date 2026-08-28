package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path"
	"path/filepath"
	stdruntime "runtime"
	"strconv"
	"strings"
	"sync"
)

const (
	consoleTimeLayout = "2006-01-02 15:04:05.000"
	jsonTimeLayout    = "2006-01-02T15:04:05.000Z07:00"
)

type consoleHandlerState struct {
	mu     sync.Mutex
	output io.Writer
}

type consoleBoundAttr struct {
	groups []string
	attr   slog.Attr
}

type consoleHandler struct {
	state  *consoleHandlerState
	level  slog.Leveler
	attrs  []consoleBoundAttr
	groups []string
}

func newConsoleHandler(output io.Writer, level slog.Leveler) slog.Handler {
	return &consoleHandler{
		state: &consoleHandlerState{output: output},
		level: level,
	}
}

func (handler *consoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	minimum := slog.LevelInfo
	if handler.level != nil {
		minimum = handler.level.Level()
	}
	return level >= minimum
}

func (handler *consoleHandler) Handle(_ context.Context, record slog.Record) error {
	event := make(map[string]any, len(handler.attrs)+record.NumAttrs()+1)
	for _, bound := range handler.attrs {
		addConsoleAttr(event, bound.groups, bound.attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		addConsoleAttr(event, handler.groups, attr)
		return true
	})
	event["message"] = record.Message
	payload, encodeErr := json.Marshal(event)
	if encodeErr != nil {
		payload, _ = json.Marshal(map[string]any{
			"log_error": encodeErr.Error(),
			"message":   record.Message,
		})
	}
	line := fmt.Sprintf("%s [%s] [%s] %s\n",
		record.Time.Local().Format(consoleTimeLayout),
		strings.ToUpper(record.Level.String()),
		shortLogSource(record.PC),
		payload,
	)
	handler.state.mu.Lock()
	_, writeErr := io.WriteString(handler.state.output, line)
	handler.state.mu.Unlock()
	if writeErr != nil {
		return writeErr
	}
	return encodeErr
}

func (handler *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *handler
	next.attrs = append([]consoleBoundAttr(nil), handler.attrs...)
	for _, attr := range attrs {
		next.attrs = append(next.attrs, consoleBoundAttr{
			groups: append([]string(nil), handler.groups...),
			attr:   attr,
		})
	}
	return &next
}

func (handler *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return handler
	}
	next := *handler
	next.groups = append(append([]string(nil), handler.groups...), name)
	return &next
}

func addConsoleAttr(event map[string]any, groups []string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		nextGroups := groups
		if attr.Key != "" {
			nextGroups = append(append([]string(nil), groups...), attr.Key)
		}
		for _, nested := range attr.Value.Group() {
			addConsoleAttr(event, nextGroups, nested)
		}
		return
	}
	if attr.Key == "" {
		return
	}
	target := consoleGroup(event, groups)
	if err, ok := attr.Value.Any().(error); ok {
		target[attr.Key] = err.Error()
		return
	}
	target[attr.Key] = attr.Value.Any()
}

func consoleGroup(event map[string]any, groups []string) map[string]any {
	target := event
	for _, group := range groups {
		nested, ok := target[group].(map[string]any)
		if !ok {
			nested = make(map[string]any)
			target[group] = nested
		}
		target = nested
	}
	return target
}

func replaceJSONLogAttr(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch attr.Key {
		case slog.TimeKey:
			if attr.Value.Kind() == slog.KindTime {
				return slog.String(slog.TimeKey, attr.Value.Time().Local().Format(jsonTimeLayout))
			}
		case slog.MessageKey:
			attr.Key = "message"
		case slog.SourceKey:
			if source, ok := attr.Value.Any().(*slog.Source); ok {
				return slog.String(slog.SourceKey, formatLogSource(source.File, source.Line))
			}
		}
	}
	if err, ok := attr.Value.Any().(error); ok {
		attr.Value = slog.StringValue(err.Error())
	}
	return attr
}

func shortLogSource(pc uintptr) string {
	if pc == 0 {
		return "unknown:0"
	}
	frame, _ := stdruntime.CallersFrames([]uintptr{pc}).Next()
	return formatLogSource(frame.File, frame.Line)
}

func formatLogSource(file string, line int) string {
	file = filepath.ToSlash(file)
	return path.Base(path.Dir(file)) + "/" + path.Base(file) + ":" + strconv.Itoa(line)
}
