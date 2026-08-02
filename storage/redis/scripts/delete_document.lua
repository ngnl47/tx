redis.replicate_commands()

local expected_revision = ARGV[1] or ""
if expected_revision ~= "" then
  local current_revision = redis.call("get", KEYS[#KEYS]) or ""
  if current_revision ~= expected_revision then
    return current_revision
  end
end

for i = 1, #KEYS do
  redis.call("del", KEYS[i])
end

return "ok"
