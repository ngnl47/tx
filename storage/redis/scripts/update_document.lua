redis.replicate_commands()
local FLAG_TRUE = "1"
local RESULT_NOT_EXISTS = "ne"
local PATCH_MODE_REPLACE = "1"
local PATCH_MODE_PARTIAL_REPLACE = "2"
local PATCH_MODE_OPERATIONS = "3"
local PATCH_MODE_TTL_ONLY = "-1"
local key = KEYS[1]
local arg_len = #ARGV
if arg_len <= 2 then
  return "ok"
end
local upsert = ARGV[arg_len - 1] == FLAG_TRUE

if not upsert then
  local exists = redis.call("exists", key)
  if exists == 0 then
    return RESULT_NOT_EXISTS
  end
end

local ttlMode = tonumber(ARGV[arg_len])
local ttl = ttlMode
if ttlMode == 0 then
  ttl = redis.call("ttl", key)
  if ttl == 0 then
    ttl = 1
  end
end

local function get_type(v)
  local t = type(v)
  if t == "table" and (v.__arr ~= nil or v[1] ~= nil) then
    return "array"
  end
  return t
end

local function is_empty_table(v)
  if type(v) ~= "table" then
    return false
  end
  return next(v) == nil
end

local function merge(old, patch)
  for k, v in pairs(patch) do
    if v == cjson.null then
      old[k] = nil
    elseif get_type(v) == "table" and get_type(old[k]) == "table" then
      merge(old[k], v)
      if is_empty_table(old[k]) then
        old[k] = nil
      end
    else
      old[k] = v
    end
  end
end

local function set_path(object, path, value)
  local target = object
  for index = 1, #path - 1 do
    local key = path[index]
    if type(target[key]) ~= "table" then
      target[key] = {}
    end
    target = target[key]
  end
  local key = path[#path]
  if value == cjson.null then
    target[key] = nil
  else
    target[key] = value
  end
end

local function delete_path(object, path)
  local target = object
  for index = 1, #path - 1 do
    target = target[path[index]]
    if type(target) ~= "table" then
      return
    end
  end
  target[path[#path]] = nil
end

local loaded = {}
local save = {}

local function load(full_key)
  local obj = loaded[full_key]
  if obj ~= nil then
    return obj
  end
  local raw = redis.call("get", full_key)
  if raw == false then
    obj = {}
  else
    local ok, decoded = pcall(cjson.decode, raw)
    if ok and type(decoded) == "table" then
      obj = decoded
    else
      obj = {}
    end
  end
  loaded[full_key] = obj
  return obj
end

for i = 1, arg_len - 2, 3 do
  local suffix = ARGV[i]
  local body = ARGV[i + 1]
  local mode = ARGV[i + 2]
  local full_key = key
  if suffix ~= "" then
    full_key = key .. ":" .. suffix
  end

  if mode == PATCH_MODE_REPLACE then
    save[full_key] = body
  elseif mode == PATCH_MODE_PARTIAL_REPLACE then
    local old = load(full_key)
    local patch = cjson.decode(body)
    for k, v in pairs(patch) do
      if v == cjson.null then
        old[k] = nil
      else
        old[k] = v
      end
    end
  elseif mode == PATCH_MODE_OPERATIONS then
    local old = load(full_key)
    local operations = cjson.decode(body).operations or {}
    for _, operation in ipairs(operations) do
      if operation.kind == "delete" then
        delete_path(old, operation.path)
      else
        set_path(old, operation.path, operation.value)
      end
    end
  elseif mode == PATCH_MODE_TTL_ONLY then
    save[full_key] = ""
  else
    local old = load(full_key)
    local patch = cjson.decode(body)
    merge(old, patch)
  end
end

for k, v in pairs(loaded) do
  if save[k] == nil then
    save[k] = cjson.encode(v)
  end
end

for k, v in pairs(save) do
  if v == "" then
    if ttlMode > 0 then
      redis.call("expire", k, ttl)
    elseif ttlMode < 0 then
      redis.call("persist", k)
    end
  elseif v == "{}" then
    redis.call("del", k)
  else
    if ttl > 0 then
      redis.call("setex", k, ttl, v)
    else
      redis.call("set", k, v)
    end
  end
end

local vkey = key .. "@v"
redis.call("incr", vkey)
if ttl > 0 then
  redis.call("expire", vkey, ttl)
else
  redis.call("persist", vkey)
end
return "ok"
