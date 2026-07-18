package redis

import (
	"strings"
	"testing"
)

func TestTaskLuaScriptsNameProtocolValues(t *testing.T) {
	for name, body := range map[string]string{
		"enqueue":  enqueueScript,
		"complete": completeScript,
		"retry":    retryScript,
	} {
		for _, want := range []string{
			`local RESULT_OK = "ok"`,
			`local RESULT_STALE = "stale"`,
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s script missing protocol constant %q", name, want)
			}
		}
	}
	if !strings.Contains(enqueueScript, `local RESULT_LEASED = "leased"`) {
		t.Fatal("enqueue script missing leased protocol constant")
	}
}
