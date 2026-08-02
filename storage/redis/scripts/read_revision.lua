redis.replicate_commands()

return redis.call("get", KEYS[1]) or ""
