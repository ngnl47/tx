package redis

import (
	"strings"
	"testing"
)

func TestLuaScriptsAreEmbeddedAssets(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	if !strings.Contains(string(updateBody), "redis.replicate_commands()") {
		t.Fatalf("embedded update script does not look like Lua: %s", string(updateBody))
	}

	findBody, err := luaScripts.ReadFile("scripts/find_document.lua")
	if err != nil {
		t.Fatalf("ReadFile find script returned error: %v", err)
	}
	if !strings.Contains(string(findBody), "return result") {
		t.Fatalf("embedded find script does not look like Lua: %s", string(findBody))
	}
}

func TestUpdateLuaScriptNamesProtocolValues(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	text := string(updateBody)
	for _, want := range []string{
		"local FLAG_TRUE = \"1\"",
		"local RESULT_NOT_EXISTS = \"ne\"",
		"local PATCH_MODE_REPLACE = \"1\"",
		"local PATCH_MODE_PARTIAL_REPLACE = \"2\"",
		"local PATCH_MODE_TTL_ONLY = \"-1\"",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script missing protocol constant %q", want)
		}
	}
}
