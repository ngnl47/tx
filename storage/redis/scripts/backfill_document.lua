-- 仅当主热文档仍不存在时，原子回填主文档、subKey 文档和版本键。
redis.replicate_commands()

local RESULT_OK = "ok"
local RESULT_EXISTS = "ex"
local RESULT_INVALID = "invalid"
local PATCH_MODE_REPLACE = "1"
local PATCH_MODE_KEEP_EMPTY = "4"

local key = KEYS[1]
local arg_len = #ARGV
if arg_len < 4 or (arg_len - 1) % 3 ~= 0 then
  return RESULT_INVALID
end
if redis.call("exists", key) ~= 0 then
  return RESULT_EXISTS
end

local ttl = tonumber(ARGV[arg_len])
if ttl == nil then
  return RESULT_INVALID
end

-- 写入前先完整校验参数，避免未知模式造成部分回填。
for i = 1, arg_len - 1, 3 do
  local mode = ARGV[i + 2]
  if mode ~= PATCH_MODE_REPLACE and mode ~= PATCH_MODE_KEEP_EMPTY then
    return RESULT_INVALID
  end
end

for i = 1, arg_len - 1, 3 do
  local suffix = ARGV[i]
  local body = ARGV[i + 1]
  local mode = ARGV[i + 2]
  local full_key = key
  if suffix ~= "" then
    full_key = key .. ":" .. suffix
  end

  if body == "{}" and mode ~= PATCH_MODE_KEEP_EMPTY then
    redis.call("del", full_key)
  elseif ttl > 0 then
    redis.call("psetex", full_key, ttl, body)
  else
    redis.call("set", full_key, body)
  end
end

local vkey = key .. "@v"
redis.call("incr", vkey)
if ttl > 0 then
  redis.call("pexpire", vkey, ttl)
else
  redis.call("persist", vkey)
end
return RESULT_OK
