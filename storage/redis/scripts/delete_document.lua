redis.replicate_commands()

for i = 1, #KEYS do
  redis.call("del", KEYS[i])
end

return "ok"
