package server

import (
	"bytes"
	"strings"
	"testing"

	txlog "github.com/ngnl47/tx/log"
)

func TestConfigureLoggerUsesLevelAndFormat(t *testing.T) {
	for _, test := range []struct {
		name   string
		config LogConfig
		write  func()
		want   []string
	}{
		{
			name:   "text",
			config: LogConfig{Level: "warn", Format: "text"},
			write: func() {
				txlog.Info("hidden info")
				txlog.Warn("visible warn", "roleId", int64(1001))
			},
			want: []string{"visible warn", "roleId=1001"},
		},
		{
			name:   "json",
			config: LogConfig{Level: "debug", Format: "json"},
			write:  func() { txlog.Debug("visible debug", "roleId", int64(1001)) },
			want:   []string{`"level":"DEBUG"`, `"msg":"visible debug"`, `"roleId":1001`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			test.config.Output = &output
			if err := configureLogger(test.config); err != nil {
				t.Fatalf("configureLogger returned error: %v", err)
			}
			t.Cleanup(func() { txlog.SetDefault(nil) })
			test.write()
			text := output.String()
			if strings.Contains(text, "hidden info") {
				t.Fatalf("log contains filtered info: %s", text)
			}
			for _, want := range test.want {
				if !strings.Contains(text, want) {
					t.Fatalf("log = %q, want %s", text, want)
				}
			}
		})
	}
}

func TestConfigureLoggerRejectsInvalidConfig(t *testing.T) {
	if err := configureLogger(LogConfig{Level: "trace"}); err == nil || !strings.Contains(err.Error(), "log level") {
		t.Fatalf("level error = %v, want log level error", err)
	}
	if err := configureLogger(LogConfig{Format: "xml"}); err == nil || !strings.Contains(err.Error(), "log format") {
		t.Fatalf("format error = %v, want log format error", err)
	}
}
