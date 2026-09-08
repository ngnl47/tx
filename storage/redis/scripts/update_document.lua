redis.replicate_commands()
if type(cjson.decode_array_with_array_mt) == "function" then
  cjson.decode_array_with_array_mt(true)
end
local FLAG_TRUE = "1"
local RESULT_NOT_EXISTS = "ne"
local RESULT_PATH_TYPE_MISMATCH = "pt"
local PATCH_MODE_REPLACE = "1"
local PATCH_MODE_PARTIAL_REPLACE = "2"
local PATCH_MODE_OPERATIONS = "3"
local PATCH_MODE_KEEP_EMPTY = "4"
local PATCH_MODE_TTL_ONLY = "-1"
local key = KEYS[1]
local arg_len = #ARGV
if arg_len <= 2 then
  return "ok"
end
local tail_len = 2
local upsert = ARGV[arg_len - tail_len + 1] == FLAG_TRUE

if not upsert then
  local exists = redis.call("exists", key)
  if exists == 0 then
    return RESULT_NOT_EXISTS
  end
end

local ttlMode = tonumber(ARGV[arg_len - tail_len + 2])
local ttl = ttlMode
if ttlMode == 0 then
  ttl = redis.call("pttl", key)
  if ttl == 0 then
    ttl = 1
  end
end

local function get_type(v)
  local t = type(v)
  if t == "table" and ((cjson.array_mt ~= nil and getmetatable(v) == cjson.array_mt) or v.__arr ~= nil or v[1] ~= nil) then
    return "array"
  end
  return t
end

-- cjson 默认只输出 14 位有效数字；安全整数必须按完整十进制写回。
-- 字符串转义、非整数及空容器仍委托 cjson，保留其空数组元表语义。
local function encode_document(value)
  local kind = get_type(value)
  if kind == "number" and value == math.floor(value) and math.abs(value) <= 9007199254740991 then
    return string.format("%.0f", value)
  end
  if kind ~= "table" and kind ~= "array" then
    return cjson.encode(value)
  end
  if next(value) == nil then
    return cjson.encode(value)
  end
  local parts = {}
  if kind == "array" then
    for i = 1, #value do
      parts[i] = encode_document(value[i])
    end
    return "[" .. table.concat(parts, ",") .. "]"
  end
  for k, v in pairs(value) do
    parts[#parts + 1] = cjson.encode(tostring(k)) .. ":" .. encode_document(v)
  end
  return "{" .. table.concat(parts, ",") .. "}"
end

local function is_empty_table(v)
  if type(v) ~= "table" then
    return false
  end
  return next(v) == nil
end

local function is_object(v)
  return get_type(v) == "table"
end

local function is_container(v)
  local value_type = get_type(v)
  return value_type == "table" or value_type == "array"
end

local function array_index(key)
  if type(key) ~= "string" or not string.match(key, "^%d+$") then
    return nil
  end
  return tonumber(key) + 1
end

local function path_value(target, key)
  if get_type(target) == "array" then
    local index = array_index(key)
    if index == nil then
      return nil, false
    end
    return target[index], true
  end
  if not is_object(target) then
    return nil, false
  end
  return target[key], true
end

local function set_path_value(target, key, value)
  if get_type(target) == "array" then
    local index = array_index(key)
    if index == nil or index > #target then
      return false
    end
    target[index] = value
    return true
  end
  if not is_object(target) then
    return false
  end
  target[key] = value
  return true
end

local function path_type_mismatch(path)
  return RESULT_PATH_TYPE_MISMATCH .. ":" .. path
end

local function merge(old, patch, prefix)
  for k, v in pairs(patch) do
    local path = tostring(k)
    if prefix ~= "" then
      path = prefix .. "." .. path
    end
    if v == cjson.null then
      old[k] = nil
    elseif is_object(v) then
      if old[k] == nil then
        old[k] = v
      elseif is_object(old[k]) then
        local ok, mismatch_path = merge(old[k], v, path)
        if not ok then
          return false, mismatch_path
        end
        if is_empty_table(old[k]) then
          old[k] = nil
        end
      else
        return false, path
      end
    else
      old[k] = v
    end
  end
  return true
end

local function set_path(object, path, value)
  local target = object
  for index = 1, #path - 1 do
    local key = path[index]
    local child, readable = path_value(target, key)
    if not readable then
      return false, table.concat(path, ".", 1, index)
    end
    if child == nil then
      if not is_object(target) then
        return false, table.concat(path, ".", 1, index)
      end
      target[key] = {}
      child = target[key]
    elseif not is_container(child) then
      return false, table.concat(path, ".", 1, index)
    end
    target = child
  end
  local key = path[#path]
  if not set_path_value(target, key, value) then
    return false, table.concat(path, ".")
  end
  return true
end

local function delete_path(object, path)
  local target = object
  for index = 1, #path - 1 do
    local key = path[index]
    local child, readable = path_value(target, key)
    if not readable then
      return false, table.concat(path, ".", 1, index)
    end
    if child == nil then
      return true
    end
    if not is_container(child) then
      return false, table.concat(path, ".", 1, index)
    end
    target = child
  end
  if get_type(target) == "array" then
    return false, table.concat(path, ".")
  end
  if not is_object(target) then
    return false, table.concat(path, ".")
  end
  target[path[#path]] = nil
  return true
end

local loaded = {}
local save = {}
local keep_empty = {}

local function load(full_key)
  local obj = loaded[full_key]
  if obj ~= nil then
    return obj, true
  end
  local raw = redis.call("get", full_key)
  if raw == false then
    obj = {}
  else
    local ok, decoded = pcall(cjson.decode, raw)
    if ok and is_object(decoded) then
      obj = decoded
    else
      return nil, false
    end
  end
  loaded[full_key] = obj
  return obj, true
end

for i = 1, arg_len - tail_len, 3 do
  local suffix = ARGV[i]
  local body = ARGV[i + 1]
  local mode = ARGV[i + 2]
  local full_key = key
  if suffix ~= "" then
    full_key = key .. ":" .. suffix
  end

  if mode == PATCH_MODE_REPLACE then
    save[full_key] = body
  elseif mode == PATCH_MODE_KEEP_EMPTY then
    save[full_key] = body
    keep_empty[full_key] = true
  elseif mode == PATCH_MODE_PARTIAL_REPLACE then
    local old, ok = load(full_key)
    if not ok then
      return path_type_mismatch("$")
    end
    local patch = cjson.decode(body)
    for k, v in pairs(patch) do
      if v == cjson.null then
        old[k] = nil
      else
        old[k] = v
      end
    end
  elseif mode == PATCH_MODE_OPERATIONS then
    local old, ok = load(full_key)
    if not ok then
      return path_type_mismatch("$")
    end
    local operations = cjson.decode(body).operations or {}
    for _, operation in ipairs(operations) do
      if operation.kind == "delete" then
        local deleted, mismatch_path = delete_path(old, operation.path)
        if not deleted then
          return path_type_mismatch(mismatch_path)
        end
      else
        local updated, mismatch_path = set_path(old, operation.path, operation.value)
        if not updated then
          return path_type_mismatch(mismatch_path)
        end
      end
    end
  elseif mode == PATCH_MODE_TTL_ONLY then
    save[full_key] = ""
  else
    local old, ok = load(full_key)
    if not ok then
      return path_type_mismatch("$")
    end
    local patch = cjson.decode(body)
    local merged, mismatch_path = merge(old, patch, "")
    if not merged then
      return path_type_mismatch(mismatch_path)
    end
  end
end

for k, v in pairs(loaded) do
  if save[k] == nil then
    save[k] = encode_document(v)
  end
end

for k, v in pairs(save) do
  if v == "" then
    if ttlMode > 0 then
      redis.call("pexpire", k, ttl)
    elseif ttlMode < 0 then
      redis.call("persist", k)
    end
  elseif v == "{}" and not keep_empty[k] then
    redis.call("del", k)
  else
    if ttl > 0 then
      redis.call("psetex", k, ttl, v)
    else
      redis.call("set", k, v)
    end
  end
end

local vkey = key .. "@v"
local revision = redis.call("incr", vkey)
if ttl > 0 then
  redis.call("pexpire", vkey, ttl)
else
  redis.call("persist", vkey)
end
return "ok"
