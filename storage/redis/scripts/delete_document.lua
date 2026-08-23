redis.replicate_commands()

local expected_revision = ARGV[1] or ""
local handoff_marker = ARGV[2] or ""
local delete_key_count = #KEYS
if handoff_marker ~= "" then
  delete_key_count = delete_key_count - 1
end
if expected_revision ~= "" then
  local current_revision = redis.call("get", KEYS[delete_key_count]) or ""
  if current_revision ~= expected_revision then
    return current_revision
  end
end

for i = 1, delete_key_count do
  redis.call("del", KEYS[i])
end

if handoff_marker ~= "" then
  redis.call("hdel", KEYS[#KEYS], handoff_marker)
end

return "ok"
