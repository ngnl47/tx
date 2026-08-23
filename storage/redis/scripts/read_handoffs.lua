local count = tonumber(ARGV[1])
local entries = redis.call("hrandfield", KEYS[1], count, "withvalues")
if type(entries) ~= "table" or #entries == 0 then
  return "[]"
end
return cjson.encode(entries)
