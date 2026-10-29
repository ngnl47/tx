-- 同一记录锁内调用：删除热文档并持久保留删除意图，阻止旧落盘和回填。
redis.replicate_commands()
for i = 1, #KEYS do
  redis.call("del", KEYS[i])
end
redis.call("set", KEYS[1], "@tx:deleted")
return "ok"
