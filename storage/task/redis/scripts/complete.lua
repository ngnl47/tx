-- tx_task_complete
redis.replicate_commands()

local RESULT_OK = "ok"
local RESULT_STALE = "stale"

local dueKey = KEYS[1]
local payloadKey = KEYS[2]
local busyKey = KEYS[3]
local tokenKey = KEYS[4]
local attemptsKey = KEYS[5]

local taskKey = ARGV[1]
local token = ARGV[2]

if redis.call("hget", tokenKey, taskKey) ~= token then
	return RESULT_STALE
end

redis.call("zrem", dueKey, taskKey)
redis.call("zrem", busyKey, taskKey)
redis.call("hdel", payloadKey, taskKey)
redis.call("hdel", tokenKey, taskKey)
redis.call("hdel", attemptsKey, taskKey)
return RESULT_OK
