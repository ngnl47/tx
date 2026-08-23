-- tx_task_claim
redis.replicate_commands()

local dueKey = KEYS[1]
local payloadKey = KEYS[2]
local busyKey = KEYS[3]
local tokenKey = KEYS[4]
local attemptsKey = KEYS[5]
local pendingKey = KEYS[6]

local now = ARGV[1]
local limit = tonumber(ARGV[2])
local leaseUntil = ARGV[3]
local tokenPrefix = ARGV[4]

local expired = redis.call("zrangebyscore", busyKey, "-inf", now)
for _, taskKey in ipairs(expired) do
	redis.call("zrem", busyKey, taskKey)
	redis.call("hdel", tokenKey, taskKey)
	redis.call("zrem", pendingKey, taskKey)
	redis.call("zadd", dueKey, now, taskKey)
end

local taskKeys = redis.call("zrangebyscore", dueKey, "-inf", now, "limit", 0, limit)
local result = {}
for i, taskKey in ipairs(taskKeys) do
	local body = redis.call("hget", payloadKey, taskKey)
	if body then
		local token = tokenPrefix .. "-" .. tostring(i)
		local attempts = redis.call("hincrby", attemptsKey, taskKey, 1)
		redis.call("zrem", dueKey, taskKey)
		redis.call("zadd", busyKey, leaseUntil, taskKey)
		redis.call("hset", tokenKey, taskKey, token)
		table.insert(result, {body = body, token = token, attempts = attempts})
	else
		redis.call("zrem", dueKey, taskKey)
	end
end

if #result == 0 then
	return "[]"
end
return cjson.encode(result)
