redis.replicate_commands()

local current = redis.call("get", KEYS[1])
if current == ARGV[1] then
  redis.call("del", KEYS[1])
  return "ok"
end
return "stale"
