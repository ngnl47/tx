-- tx_task_retry
redis.replicate_commands()

local dueKey = KEYS[1]
local busyKey = KEYS[3]
local tokenKey = KEYS[4]

local taskKey = ARGV[1]
local token = ARGV[2]
local nextDueAt = ARGV[3]

if redis.call("hget", tokenKey, taskKey) ~= token then
	return "stale"
end

redis.call("zrem", busyKey, taskKey)
redis.call("hdel", tokenKey, taskKey)
redis.call("zadd", dueKey, nextDueAt, taskKey)
return "ok"
