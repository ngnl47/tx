-- 单角色许可 CAS；退出保留票据水位，不因邮箱到期使旧票据重新有效。
local previous = redis.call('GET', KEYS[1]) or ''
if previous ~= ARGV[1] then
    return 0
end
if ARGV[2] == '' then
    redis.call('DEL', KEYS[1])
else
    redis.call('SET', KEYS[1], ARGV[2])
end
return 1
