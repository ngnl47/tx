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
		"redis.call(\"expire\", vkey, ttl)",
		"redis.call(\"persist\", vkey)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("update script does not synchronize version key lifecycle: missing %q", want)
		}
	}
	if strings.Contains(text, "redis.call(\"expire\", vkey, 60)") {
		t.Fatalf("update script must not use a fixed version key ttl")
	}
}

func TestFusionHandoffLuaProtocolKeepsCapacityAndRevisionFences(t *testing.T) {
	updateBody, err := luaScripts.ReadFile("scripts/update_document.lua")
	if err != nil {
		t.Fatalf("ReadFile update script returned error: %v", err)
	}
	updateText := string(updateBody)
	for _, want := range []string{
		"local RESULT_HANDOFF_FULL = \"hf\"",
		"redis.call(\"hexists\", KEYS[2], marker)",
		"redis.call(\"hlen\", KEYS[2]) >= max_pending",
		"local revision = redis.call(\"incr\", vkey)",
		"redis.call(\"hset\", KEYS[2], marker, token)",
		"return revision",
	} {
		if !strings.Contains(updateText, want) {
			t.Fatalf("update script does not preserve Fusion handoff protocol: missing %q", want)
		}
	}
	if strings.Index(updateText, "redis.call(\"hlen\", KEYS[2]) >= max_pending") > strings.Index(updateText, "for k, v in pairs(save) do") {
		t.Fatal("handoff capacity must be checked before Redis documents are mutated")
	}

	confirmBody, err := luaScripts.ReadFile("scripts/confirm_handoff.lua")
	if err != nil {
		t.Fatalf("ReadFile confirm handoff script returned error: %v", err)
	}
	confirmText := string(confirmBody)
	for _, want := range []string{
		"revision ~= ARGV[2]",
		"redis.call(\"hget\", KEYS[2], ARGV[1])",
		"token ~= ARGV[3]",
		"redis.call(\"hdel\", KEYS[2], ARGV[1])",
	} {
		if !strings.Contains(confirmText, want) {
			t.Fatalf("confirm handoff script does not fence stale writers: missing %q", want)
		}
	}
}

func TestReadHandoffsLuaUsesBoundedKnownHashRead(t *testing.T) {
	body, err := luaScripts.ReadFile("scripts/read_handoffs.lua")
	if err != nil {
		t.Fatalf("ReadFile handoff reader returned error: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "redis.call(\"hrandfield\", KEYS[1], count, \"withvalues\")") {
		t.Fatal("handoff reader must use bounded HRANDFIELD WITHVALUES on the known slot Hash")
	}
	for _, forbidden := range []string{"redis.call(\"scan\"", "redis.call(\"keys\""} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("handoff reader must not inspect the Redis keyspace with %q", forbidden)
		}
	}
}

func TestDeleteLuaClearsHandoffOnlyAfterRevisionMatch(t *testing.T) {
	body, err := luaScripts.ReadFile("scripts/delete_document.lua")
	if err != nil {
		t.Fatalf("ReadFile delete script returned error: %v", err)
	}
	text := string(body)
	revisionCheck := strings.Index(text, "current_revision ~= expected_revision")
	handoffDelete := strings.Index(text, "redis.call(\"hdel\", KEYS[#KEYS], handoff_marker)")
	if revisionCheck < 0 || handoffDelete < 0 || revisionCheck > handoffDelete {
		t.Fatal("delete script must clear the handoff marker only after the revision fence passes")
	}
}
