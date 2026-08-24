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
		"local RESULT_PATH_TYPE_MISMATCH = \"pt\"",
		"local PATCH_MODE_REPLACE = \"1\"",
		"local PATCH_MODE_PARTIAL_REPLACE = \"2\"",
		"local PATCH_MODE_OPERATIONS = \"3\"",
		"local PATCH_MODE_KEEP_EMPTY = \"4\"",
		"local PATCH_MODE_TTL_ONLY = \"-1\"",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script missing protocol constant %q", want)
		}
	}
}

func TestUpdateLuaScriptKeepsExplicitEmptyDocuments(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	text := string(updateBody)
	for _, want := range []string{
		"local keep_empty = {}",
		"keep_empty[full_key] = true",
		"elseif v == \"{}\" and not keep_empty[k] then",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script does not preserve explicit empty document: missing %q", want)
		}
	}
}

func TestUpdateLuaScriptRejectsExistingNonObjectPathNodes(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	text := string(updateBody)
	for _, want := range []string{
		"cjson.decode_array_with_array_mt(true)",
		"local function is_object(v)",
		"return RESULT_PATH_TYPE_MISMATCH .. \":\" .. path",
		"local merged, mismatch_path = merge(old, patch, \"\")",
		"local updated, mismatch_path = set_path(old, operation.path, operation.value)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script does not reject path type mismatch: missing %q", want)
		}
	}
}

func TestUpdateLuaScriptKeepsVersionKeyLifecycleInSync(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	text := string(updateBody)
	for _, want := range []string{
		"redis.call(\"pexpire\", vkey, ttl)",
		"redis.call(\"persist\", vkey)",
		"redis.call(\"pttl\", key)",
		"redis.call(\"psetex\", k, ttl, v)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script does not synchronize version key lifecycle: missing %q", want)
		}
	}
	if strings.Contains(text, "redis.call(\"expire\"") || strings.Contains(text, "redis.call(\"setex\"") || strings.Contains(text, "redis.call(\"ttl\"") {
		t.Fatalf("update script must use millisecond TTL commands")
	}
	if strings.Contains(text, "redis.call(\"pexpire\", vkey, 60000)") {
		t.Fatalf("update script must not use a fixed version key ttl")
	}
}

func TestDeleteLuaOnlyDeletesProvidedDocumentKeys(t *testing.T) {
	body, err := luaScripts.ReadFile("scripts/delete_document.lua")
	if err != nil {
		t.Fatalf("ReadFile delete script returned error: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "redis.call(\"del\", KEYS[i])") {
		t.Fatal("delete script must delete every provided Redis document key")
	}
	if strings.Contains(text, "expected_revision") {
		t.Fatal("explicit Redis delete script must not require a lifecycle revision")
	}
}
