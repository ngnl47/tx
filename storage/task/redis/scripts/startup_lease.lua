-- tx_task_startup_lease
redis.replicate_commands()

local RESULT_ACQUIRED = "acquired"
local RESULT_RUNNING = "running"
local RESULT_COMPLETE = "complete"
local RESULT_OK = "ok"
local RESULT_STALE = "stale"
local RESULT_INVALID = "invalid"

local key = KEYS[1]
local operation = ARGV[1]
local token = ARGV[2]
local ttl = tonumber(ARGV[3])
local runningValue = "running:" .. token
local completeValue = "complete:" .. token
local current = redis.call("get", key)

if operation == "acquire" then
	if not current then
		redis.call("psetex", key, ttl, runningValue)
		return RESULT_ACQUIRED
	end
	if string.sub(current, 1, 8) == "running:" then
		return RESULT_RUNNING
	end
	if string.sub(current, 1, 9) == "complete:" then
		return RESULT_COMPLETE
	end
	return RESULT_INVALID
end

if operation == "renew" then
	if current ~= runningValue then
		return RESULT_STALE
	end
	redis.call("pexpire", key, ttl)
	return RESULT_OK
end

if operation == "complete" then
	if current ~= runningValue then
		return RESULT_STALE
	end
	redis.call("psetex", key, ttl, completeValue)
	return RESULT_OK
end

if operation == "release" then
	if current ~= runningValue then
		return RESULT_STALE
	end
	redis.call("del", key)
	return RESULT_OK
end

return RESULT_INVALID
