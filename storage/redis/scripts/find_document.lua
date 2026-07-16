redis.replicate_commands()
local exists = redis.call("exists", KEYS[1])
if exists == 0 then
  return ""
end

local result = "{}"
for i = 2, #KEYS do
  local subResult = redis.call("get", KEYS[i])
  if subResult ~= false and subResult ~= "" then
    if result == "{}" then
      result = subResult
    else
      result = string.sub(result, 1, -2) .. "," .. string.sub(subResult, 2)
    end
  end
end
return result
