redis.replicate_commands()

local expected_revision = ARGV[1] or ""
local ttl_milliseconds = tonumber(ARGV[2])
local current_revision = redis.call("get", KEYS[#KEYS]) or ""
if current_revision ~= expected_revision then
  return current_revision
end

for i = 1, #KEYS do
  redis.call("pexpire", KEYS[i], ttl_milliseconds)
end

return "ok"
