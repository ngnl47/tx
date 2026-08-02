package redis

import "embed"

//go:embed scripts/*.lua
var luaScripts embed.FS

var (
	deleteDocumentScript = mustReadLuaScript("scripts/delete_document.lua")
	findDocumentScript   = mustReadLuaScript("scripts/find_document.lua")
	lockRecordScript     = mustReadLuaScript("scripts/lock_record.lua")
	unlockRecordScript   = mustReadLuaScript("scripts/unlock_record.lua")
	updateDocumentScript = mustReadLuaScript("scripts/update_document.lua")
)

func mustReadLuaScript(name string) string {
	body, err := luaScripts.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return string(body)
}
