-- tx_task_retry
redis.replicate_commands()

local RESULT_OK = "ok"
local RESULT_STALE = "stale"

local dueKey = KEYS[1]
local busyKey = KEYS[3]
local tokenKey = KEYS[4]
local pendingKey = KEYS[6]

local taskKey = ARGV[1]
local token = ARGV[2]
local nextDueAt = ARGV[3]

if redis.call("hget", tokenKey, taskKey) ~= token then
	return RESULT_STALE
end

redis.call("zrem", busyKey, taskKey)
redis.call("hdel", tokenKey, taskKey)
redis.call("zrem", pendingKey, taskKey)
redis.call("zadd", dueKey, nextDueAt, taskKey)
return RESULT_OK
