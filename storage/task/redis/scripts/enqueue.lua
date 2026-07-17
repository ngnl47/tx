-- tx_task_enqueue
redis.replicate_commands()

local dueKey = KEYS[1]
local payloadKey = KEYS[2]
local busyKey = KEYS[3]

local taskKey = ARGV[1]
local dueAt = ARGV[2]
local body = ARGV[3]

redis.call("hset", payloadKey, taskKey, body)
if redis.call("zscore", busyKey, taskKey) then
	return "leased"
end
redis.call("zadd", dueKey, dueAt, taskKey)
return "ok"
