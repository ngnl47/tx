redis.replicate_commands()

local result = "{}"
local found = false
for i = 1, #KEYS do
  local subResult = redis.call("get", KEYS[i])
  if subResult == "@tx:deleted" then return subResult end
  if subResult ~= false and subResult ~= "" then
    found = true
    if result == "{}" then
      result = subResult
    else
      result = string.sub(result, 1, -2) .. "," .. string.sub(subResult, 2)
    end
  end
end
if not found then
  return ""
end
return result
