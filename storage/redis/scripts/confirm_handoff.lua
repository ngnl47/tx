local RESULT_OK = "ok"
local RESULT_STALE = "stale"
local revision = redis.call("get", KEYS[1])
if revision == false or revision ~= ARGV[2] then
  return RESULT_STALE
end
local token = redis.call("hget", KEYS[2], ARGV[1])
if token == false or token ~= ARGV[3] then
  return RESULT_STALE
end
redis.call("hdel", KEYS[2], ARGV[1])
return RESULT_OK
