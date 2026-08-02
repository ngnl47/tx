redis.replicate_commands()

local ok = redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
if ok == false then
  return "busy"
end
return "ok"
