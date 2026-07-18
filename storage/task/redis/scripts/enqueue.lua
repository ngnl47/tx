-- tx_task_enqueue
redis.replicate_commands()

local RESULT_OK = "ok"
local RESULT_STALE = "stale"
local RESULT_LEASED = "leased"

local dueKey = KEYS[1]
local payloadKey = KEYS[2]
local busyKey = KEYS[3]

local taskKey = ARGV[1]
local dueAt = ARGV[2]
local body = ARGV[3]

redis.call("hset", payloadKey, taskKey, body)
if redis.call("zscore", busyKey, taskKey) then
	return RESULT_LEASED
end
redis.call("zadd", dueKey, dueAt, taskKey)
return RESULT_OK
