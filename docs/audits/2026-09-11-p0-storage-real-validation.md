# P0 持久化管道：真实存储首批验证（2026-09-11）

## 发现与修正

未发现本批覆盖范围内的生产持久化缺陷。发现一个验收可靠性问题：txgame/server/tests/integration/change_storage_test.go 的客户端使用 defer 关闭，但测试数据用 t.Cleanup 清理。Go 先执行测试函数 defer，再执行测试清理回调，导致清理时连接已经关闭；清理助手忽略返回错误，因此业务断言通过仍留下数据。

在本轮独立 Mongo 库中实测残留 roles=12、storage_probes=1。将该文件 13 处 Redis Close、8 处 Mongo Disconnect 改为在创建连接后立即注册 t.Cleanup，利用后注册的数据清理先执行的顺序，保持连接存活到清理完成。没有新增 helper、运行时开关或生产代码修改。

例如测试保存角色成功后，原先先关 Mongo，再 DeleteOne；现在先删除角色，再断开 Mongo。使用第二个独立库重跑相同 13 个测试后，实测 roles=0、storage_probes=0。

## 当前验证结果

| 范围 | 当前运行结果 | 覆盖 |
|---|---|---|
| tx Mongo driver 探针 | 2 个顶层测试、含子测试共 9 个 pass，0 skip/fail | 计数器并发区间与溢出；direct 对照、incremental、incremental-ack-loss、fusion、fusion-ack-loss、fusion-create、fusion-create-ack-loss |
| txgame 真实 GD 存储 | 13 个测试 pass，0 skip/fail；关闭顺序修复后同样再次通过 | 三后端变更落地与加载、显式空集合/嵌套更新、Redis revision/TTL、Fusion 租约期间新写入和并发单条/批量回填 |
| txgameslg 世界命令 | TestSLGWorldCommandWritesMongo pass，0 skip/fail | 真实生成世界对象经默认 Procedure/WriteBack 写入 Mongo，含实际服务启动及归属配置 |

合计 16 个不同顶层测试、含子测试 23 个 pass，不重复累加修复前后的两次 13 项运行。direct 是用于证明客户端超时不等于服务端取消的对照，不把未加版本条件的直接写当成安全重试方案。

现有 Fusion 租约测试使用真实 Redis task backend；Mongo 故障探针使用真实 Redis/Mongo 与受控传输延迟/确认丢失，部分探针使用内存任务队列控制时刻。txgame 部分测试用 immediateDelaySubmitter 直接登记队列；这些结果不能替代真实 DelayTaskDispatcher 与任务 backend 联合发布失败/恢复的证据。

## 命令与隔离

```sh
# tx
TX_MONGO_LATE_WRITE_ADDRESS=127.0.0.1:27017 \
TX_MONGO_URI=mongodb://127.0.0.1:27017 \
GOCACHE=/private/tmp/tx-go-build go test ./storage/mongo/mongodriver \
  -run '^(TestMongoLateWriteAfterClientTimeout|TestCounterReserveMongoIntegration)$' \
  -count=1 -timeout=120s -json

# txgame/server；修复后 Mongo 库改为 tx_p0_storage_20260911_after
TXGAME_REDIS_URI='redis://127.0.0.1:6379?db=15' \
TXGAME_MONGO_URI='mongodb://127.0.0.1:27017/tx_p0_storage_20260911' \
GOCACHE=/private/tmp/tx-go-build go test ./tests/integration \
  -run '^Test(Redis|Fusion|GeneratedStorageProbeDocumentContracts|GeneratedChangesApplyToRedisMongoAndFusion)' \
  -count=1 -timeout=120s -json

# txgameslg/server
GOCACHE=/private/tmp/tx-go-build go test ./tests/integration \
  -run '^TestSLGWorldCommandWritesMongo$' -count=1 -timeout=90s -json
```

显式依赖地址使依赖不可达直接失败。Redis 使用测试 DB15，未执行 FLUSHDB；Mongo 使用上述独立库、driver 自动生成的 probe/counter 库，以及 SLG 测试自动隔离库。探针使用随机键，GD 测试使用自身测试记录范围。

原始 JSON 日志、修复前后清理检查和清理脚本在 /private/tmp/tx-p0-storage-real-20260911。修复前残留复现结果为 12/1，修复后 cleanup-after.txt 为 0/0。复现库与修复后库随后移除；只根据复现库中已确认测试记录定位并删除对应 Redis 数据键，未扩大到全 DB 清理。脚本完成结果见 cleanup.txt。

## 边界与下一项

本轮并非完整六工程验收，没有重跑全量单元测试或 race，没有测吞吐。生产与生成物变化 0 行，测试代码净行数不变；无需性能 benchmark。

第一项 P0 的真实存储关键路径已有本轮证据，但不宣称崩溃下无损、跨库 ACID 或所有故障窗口已覆盖。下一项验证真实 DelayTaskDispatcher 到 Redis task backend 的发布失败、pending 保留和恢复排空，再转入 Procedure 多根锁、子过程和异常回滚。不会为通过验收隐式升级 Fusion 已确认的崩溃窗口契约。

后置 ponytail-review：Lean already. Ship. 测试修改仅调整资源清理顺序，无新增抽象。六工程 diff 检查通过；全部变更未提交，提交建议累计此前 P0 计划与本轮验证、修复。

## 追加：实际分发器到 Redis backend 发布恢复

新增 TestFusionDispatcherRecoversRedisPublicationFailure，补上直接 submitter 无法覆盖的联合链路。使用真实 Redis/Mongo 与生成 GDRole，在独立任务 payload 键放入错误类型，令入队 Lua 的第一个写入失败，不使用假队列或新增生产故障开关。

同一角色 Create、Save 热写均成功并清理变更；随后实际 Dispatcher.Drain 返回 DeadlineExceeded 和 WRONGTYPE。移除故障后不再次 Submit，Drain 能发布保留的 pending，热数据 revision 不变。实际 Scheduler 只领取一个任务，Mongo 取得第二次保存的最新名称；再次 Drain、RunOnce 无任务。这验证进程存活期间的发布恢复，不证明进程崩溃后 pending 可恢复。

本次新测试单独运行通过，随后与已有用例一起回归：14 个不同测试通过、0 skip/fail，不重复累计独立运行。新测试净增 135 行，生产和生成物变化 0 行；累计未提交的 Go 差异为新增 156、删除 21、净增 135 行（含此前 21 处清理顺序替换）。未运行全量测试、race 或 benchmark；本次无生产性能变化。

```sh
# txgame/server；单测将 -run 改为 ^TestFusionDispatcherRecoversRedisPublicationFailure$
TXGAME_REDIS_URI='redis://127.0.0.1:6379?db=15' \
TXGAME_MONGO_URI='mongodb://127.0.0.1:27017/tx_p0_dispatcher_20260911' \
GOCACHE=/private/tmp/tx-go-build go test ./tests/integration \
  -run '^Test(Redis|Fusion|GeneratedStorageProbeDocumentContracts|GeneratedChangesApplyToRedisMongoAndFusion)' \
  -count=1 -timeout=120s -json
```

日志在 /private/tmp/tx-p0-dispatcher-20260911：focused.jsonl、storage-regression.jsonl。测试使用唯一角色 ID 和任务前缀，按自身范围清理；cleanup-check.txt 实测独立 Mongo 库 roles=0、storage_probes=0。未执行 Redis FLUSHDB。

后置 ponytail-review：Lean already. Ship. 保留关键失败恢复断言，复用既有适配器和清理助手，没有新增抽象。下一项进入 Procedure 多根锁、子过程和异常回滚验收；全部改动保持未提交。
