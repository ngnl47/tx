# 旧审计 P0/P1 当前状态复核

范围：复核 2026-08-31 六工程审计的 12 项发现，采用当前代码与既有回归证据。本文是代码路径复核，不是新一轮故障复现；普通集群通过不能证明竞争窗口已消失。未修改生产实现。

## 优先结论

下一项优先复现 Fusion 旧任务覆盖新 Mongo 数据的交错。它属于持久化正确性，不是低频 I/O 吞吐优化。先用可控阻塞验证旧写晚于新写完成的结果，再确定条件写入与版本模型，不能只延长 Redis 锁或增加写前检查。

## 逐项状态

| 原项 | 当前状态 | 当前代码证据与下一动作 |
| --- | --- | --- |
| P0 Fusion 旧任务覆盖 | 高风险路径仍在 | `storage/runtime/runtime.go` 直接把 Mongo 适配器传给延迟 handler；`storage/fusion/handler.go` 仍调用普通 `ReplaceDocument`，只有正 DelayDelete 才读取 revision；`storage/mongo/adapter.go` 使用记录键过滤替换，未带版本条件。优先做确定性交错复现。 |
| P0 认证身份贯穿 | 信封已补齐，授权闭环待复核 | `modules/agent/actor_dispatcher.go` 注入 Identity/UpSeq，`modules/actor/delivery.go` 跨进程编码，示例 `CommandContext.Identity()` 可读；但 `commandroute.ValidatePayloadRoute` 只比较载荷与路由键，不能据此宣布越权问题已解决。需核对当前角色路由的绑定/授权边界，与允许显式跨世界路由区分。 |
| P0 Topic 部分成功 | 已有针对性修复与证据 | `modules/msgq/broadcast_grpc_test.go` 覆盖远程部分结果，当前共享不可变载荷契约和角色分发已进入稳定文档；最近真实 Redis race 与双集群回归通过。不重复改造成 UDP 或全 MsgQ 广播。 |
| P1 Discovery Watch 联合就绪 | 风险结构仍在 | `modules/discovery/runtime.go` 的 watchOnce 仍等待第二个 Watch 返回，外层再失效目录；WaitReady 仍分别等待两份 Ready。需重现延迟退出和跨轮次就绪交错。 |
| P1 Actor provider 竞态 | 依赖风险仍在，需重新运行针对性 race | 当前仍直接使用锁定版本的 etcd provider；本地依赖中的 shutdown 为普通 bool，Shutdown 写、keepalive 循环读。常规非 race 集群通过不能关闭此项。 |
| P1 TryRelease 取消链 | 风险路径仍在 | `modules/writeback/manager.go` 的 SPI 仍无 context，内部用 Background 等待实体锁。下一步需组合实体释放任务与关闭顺序复现，不仅测试手动解锁。 |
| P1 World_StartMarch 内外契约复用 | 仍存在 | `txgameslg/server/services/slggame/world_caller.go` 仍构造公开 World_StartMarch 并经 Actor Call 消费公开响应；调用没有携带 Session Identity。是否拆分需结合上一项身份授权闭环评审，公开客户端协议本身不应删除。 |
| P1 Promote 后本地可见性 | 屏障缺口仍在 | `txgameslg/server/internal/clustere2e/suite.go` 晋升后读取 dependencies.Slot；该方法读取 etcd 持久值，不是 Agent/owner 本地 Watch 快照。需可控 Watch 延迟复现。 |
| P1 TS null/omitempty | 模型缺口仍在 | `txgen/gen/ts_model.go` 的 schema 递归剥掉指针；需重新执行合法 Go null/零值响应的客户端交叉用例。修复保持语言无关输入语义，不把 TS 约束反绑 Go。 |
| P1 TS 整数位宽/符号 | 校验缺口仍在 | 两客户端 `src/protocol/codec.ts` 的整数分支仍统一使用 requireSafeInteger，没有按 scalar 位宽和符号拒绝值。需复现 uint32 负数与窄整数越界。 |
| P1 失败生成消耗 CommandID | 顺序问题仍在 | `txgen/gen/render_ts.go` 仍先 prepareCmdRoutes，再做模型和保留名校验。需验证失败前后 manifest 不变；相同命令名重复失败不等于每次消耗新 ID。 |
| P1 nil 日志输出 | 缺口仍在 | `reportTypeScriptFallbackTypes` 没有 nil 检查，README 仍承诺 nil 关闭提示。需用存在 fallback 的输入复现；不增加额外配置容错框架。 |

## 工作顺序

1. Fusion 旧写覆盖的确定性交错复现与修复设计。
2. 认证身份与当前角色路由授权闭环，明确哪些跨业务身份操作交给项目 SPI。
3. Discovery/Actor 生命周期、TryRelease 取消及晋升可见性。
4. 生成器与客户端协议契约；独立维护两套示例，不交叉引用。

此顺序是评估优先级，不构成对全部方案的批量实现授权。具体 API/存储版本设计需在复现后确认。本文未执行新的测试，不把代码检查标记为实测失败或全量审计完成。

## Fusion 交错复现补充

后续诊断已执行：`go test -race ./tests -run '^TestProbeFusionLateWriteMustNotOverwriteNewDocument$' -count=3 -v`，三次均因最终数据错误失败，未报告 Go 数据竞态。

```text
new-write-completed="new" final="old" old-error=<nil> new-error=<nil>
late old worker overwrote new document: final="old"
```

复现使用生产 `StorageTaskHandler`，受控 loader/replacer 和记录锁替身模拟锁已过期、两个 worker 均可执行的状态；旧 worker 在执行替换前等待，新 worker 完成后才释放旧 worker。场景使用 `DelayDelete=0`，没有后续 revision/TTL 检查，两次 handler 都返回成功。此结果不是实际 Redis 租约过期或 Mongo 网络故障实测。

代码核对表明实际装配没有外层补救：`storage/runtime/runtime.go` 直接把 Mongo 适配器传给 handler；`storage/mongo/adapter.go` 按记录键替换；`storage/mongo/mongodriver/database.go` 以 `upsert=true` 调用 ReplaceOne，没有单调版本条件。旧写隔离应落到 Mongo 原子写入边界，不能只在发送前检查 Redis 锁。

临时复现源保留于 `/private/tmp/tx-fusion-stale-probe.SHWkMe/fusion_stale_write_probe_test.go`，已移出常规测试目录；它断言正确行为并故意暴露当前缺陷，不把“旧值覆盖成功”固化成绿色回归契约。生产代码未修改。正式修复时复用该交错，扩展到真实生成模型及 Mongo 条件写入。

下一步明确为修复设计：先核对 Redis 热数据过期/回填时的版本连续性，再确定 Mongo 持久版本、条件写入、冲突重试和变更清理的闭环。不能直接把可能随热键回收的 Redis revision 当作全生命周期版本，也不能把一次 Mongo 冲突无条件当作任务成功。设计确认后再实施。

## 真实租约、超时与最终纠正复核

采用 txgameslg 生成的 GDRole（DelaySave=1s、DelayDelete=24h）、真实 Redis DB 15、真实 Mongo `txgameslg_fusion_lease_probe`、默认 30s 延迟任务租约和默认 10s 记录锁。旧 worker 在调用 Mongo 适配器前受控暂停，实际等待 31s 后由第二个 Scheduler 重新领取；没有停止 Mongo，没有伪造时钟或缩短租约。服务端进程暂停/客户端调用前停顿是本次注入点，不等同于 Mongo 服务端执行阻塞或已发请求的网络超时。

运行 `go test ./tests/integration -run '^TestProbeFusionRealLeaseLateWrite$' -count=1 -v`，得到：

```text
delaySave=1s delayDelete=24h0m0s reclaimed=1 before=new after=old oldErr=<nil>
repairTasks=1 repairErr=<nil> repairedValue=new
FAIL: late old write overwrote Mongo: old
```

结论：Mongo 全程在线也能出现旧写回退，不要求 Mongo 服务下线；但当前正 DelayDelete 且 Redis 热数据仍在时，旧 worker 的 revision 比较会补登记一次保存，随后确实纠正为 new。这支持“后续保存可最终纠正”的判断。本次没有证明该配置下永久丢失，也未测量实际环境出现长暂停的概率。不能把受支持零 DelayDelete 分支的替身复现等同于当前角色配置的永久丢失结论。

超时与闸门代码事实：

- `storage/fusion/scheduler.go` 将服务生命周期 context 传给 handler，没有单任务 deadline，也没有自动续任务租约。
- `storage/runtime/runtime.go` 的 Mongo 装配使用 URI 与 Monitor；当前本地 URI 没有 timeoutMS/socketTimeoutMS。锁定驱动 v1.17.9 的默认 ServerSelectionTimeout 为 30s，但这是选服等待，不是所有操作总时限；默认 SocketTimeout=0、Client Timeout=nil。
- 健康探测有独立超时；`requireStorageMode(Fusion)` 同时要求 Redis/Mongo 健康，项目有状态写入准入使用该闸门。它可拒绝后续写入，不会撤销已在途的 Mongo 替换；后端存活也不代表某个 worker 不会暂停。
- 旧任务的 Complete/Retry 由 token 条件保护，不会结算新租约，但这不能撤销已经完成的 Mongo 写入。

本次测试精确删除了唯一角色记录和唯一任务前缀下六个已知 key，未扫描或清空数据库。临时源位于 `/private/tmp/tx-fusion-real-lease.PDn54w/fusion_lease_probe_test.go`，未加入常规用例；生产实现未修改。

另一个独立诊断发现：首次以 RoleID=1788843635943283 运行时，Redis 热文档回读后 Mongo canonical 解码报 `cannot unmarshal number 1.7888436359433e+15 ... into ... int64`，阻止写回；改用 UnixMilli 范围的唯一 ID 后上述交错完成。该现象需单独核对整数转换路径，尚未确定是表示转换还是伴随精度损失，不应混入本次旧写修复。

下一步先评审“禁止 Mongo 暂时回退”所需的最小条件写入方案，并单独评估新发现的大整数落库失败。操作超时属于资源边界，不能把超时取消当作 Mongo 原子版本保护的替代品；不增加外部服务故障补救系统或并行写回机制。

## 大整数精度根因与优先级调整

真实 Redis 无 key 读写的 Lua 编解码诊断已确认：

```text
input=1788843635943283 encoded={"roleId":1.7888436359433e+15} precision=14 decodedInteger=1788843635943283
input=9007199254740991 encoded={"roleId":9.007199254741e+15} precision=14 decodedInteger=9007199254740991
input=1788843635943 encoded={"roleId":1788843635943} precision=14 decodedInteger=1788843635943
```

这里 decodedInteger 是 cjson 解码后、重新编码前用整数格式输出的值，说明 Lua number 在这组安全整数上仍精确，损失发生在 cjson.encode 的十进制输出。第一个编码结果实际表示 1788843635943300，与原值相差 17；第二个结果表示 9007199254741000，甚至超出约定安全范围。诊断源为 `/private/tmp/tx-cjson-number-probe.go`。

生产 `storage/redis/scripts/update_document.lua` 对 loaded 文档调用 cjson.encode；局部变更会重新编码同一文档中没有修改的其他数值。因此不只是 RoleID 字段新写入时有风险，修改 Name 也可能改变已有的大整数。`storage/redis/document.go` 只按 2^53-1 检查输入，无法阻止此处 14 位十进制编码损失。此前真实 Mongo 回写解码错误是这条链路的后果，不能仅通过允许科学计数法解决。

优先级调整：先设计并修复 Redis JSON 增量更新的安全整数保真，再推进 Fusion 旧写条件保护。不得缩小已约定的 53 位安全整数范围，不把业务整数改成字符串，不在 Mongo 解码时吞掉格式错误或对已损失的值取整。最小修复应限定在脚本序列化边界，并用真实生成对象验证未修改字段、正负边界和最终 Mongo 数值；还需比较既有增量基准，避免为精度引入整条写链路重构。

Fusion 版本设计仍有待确认的关键约束：当前 Redis `@v` 与热数据一起过期，回填脚本只 INCR 当前键，不从 Mongo 恢复持久版本。不能直接以 `@v` 大小做全生命周期排序；需要先决定持久版本与一次 Redis 热数据驻留期间版本之间的关系。该项尚未进入实现。

## 安全整数修复与验收

`update_document.lua` 在增量文档序列化时，将 ±(2^53−1) 内的整数按完整十进制输出；字符串转义、非整数和空容器继续使用 cjson。未调整业务类型、安全整数范围、Mongo 解码契约或写入路径。空容器保留原生编码是必要边界：本地 Redis 的空数组带内部元表，但不暴露 `cjson.array_mt`，不能仅按 table 和首元素判断后重新编码。

新增一个真实生成角色回归 `TestRedisPartialUpdatePreservesSafeIntegers`，覆盖修改普通字段后未修改的大整数、正负安全边界、subKey 数值更新和 Mongo 最终 BSON 值。该用例先在旧实现上复现精度错误，再在修复后通过。复用无状态示例已有用例验证空数组，不增加重复测试矩阵。

最终验证通过：

- tx：`go test ./... -count=1`。
- txgame/server：`go test ./... -count=1`，包含真实存储集成与空数组回归。
- txgameslg/server：`TX_WRITEBACK_IO=1 go test ./... -count=1`。
- 新增真实整数回归：`TX_WRITEBACK_IO=1 go test -race ./tests/integration -run '^TestRedisPartialUpdatePreservesSafeIntegers$' -count=3`。

性能记录：既有 `BenchmarkWriteBackStorageIO/redis/0s` 的修复前 100 次采样为 768530 ns/op，修复后同规模为 845102 ns/op，均为 19100 B/op、212 allocs/op。最终代码另做 1000 次、3 轮采样，为 846206 / 854779 / 839327 ns/op。短样本提示存在编码成本，不能据此声称性能无回退；尚缺同规模、交替运行的前后对照。下一步先补齐此项对照，确认成本，再继续 Fusion 持久版本及旧写条件保护设计。

## 安全整数修复的同规模性能对照

本轮完成上述对照。固定 txgameslg `4e8bfc1` 的同一基准源码，分别链接 tx 修复前 `b1195c8` 与修复后 `8d472eb`；两者生产代码差异仅为增量文档编码修复。旧版本通过 `git archive` 解到临时目录，使用独立 modfile 编译；未切换工程分支、修改工作区源码或加入旧方案兼容代码。

运行环境：本地 Intel Core i9-9880H、Darwin/amd64、真实 Redis DB 15。每次操作是两个生成角色经 WriteBack 串行完成增量写入，修改 Name；不含延迟注入。角色结构及变更相同，唯一 RoleID 按各轮启动时间分配，清理只删除本轮精确 key。该基准验证最终 Name，但不验证旧版本的大整数精度，因此旧版本的 PASS 仅代表性能采样完成，不能代表旧实现正确。

每轮均使用：

```sh
TX_WRITEBACK_IO=1 <对应版本的测试二进制> \
  -test.run='^$' \
  -test.bench='^BenchmarkWriteBackStorageIO/redis/0s$' \
  -test.benchtime=5000x -test.count=1
```

| 执行顺序 | 版本 | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| 1 | 修复前 | 785840 | 19157 | 214 |
| 2 | 修复后 | 864841 | 19155 | 214 |
| 3 | 修复后 | 829912 | 19154 | 214 |
| 4 | 修复前 | 844192 | 19156 | 214 |
| 5 | 修复前 | 843172 | 19158 | 214 |
| 6 | 修复后 | 822180 | 19154 | 214 |
| 7 | 修复后 | 847012 | 19156 | 214 |
| 8 | 修复前 | 813638 | 19157 | 214 |

均值从 821710.5 增至 840986.25 ns/op（+2.35%）；中位数从 828405 增至 838462 ns/op（+1.21%）。两组波动区间明显重叠，未稳定复现此前约 10% 的短测差异；这不是零成本证明，也不是统计显著性结论。Go 分配次数一致，但该指标不包含 Redis 内部 Lua 分配。仅覆盖当前小文档完整链路，不外推到大数组、大文档或 Redis 极限吞吐。

八轮均通过；随后当前二进制运行 `TestRedisPartialUpdatePreservesSafeIntegers` 三次均通过，真实 Redis→Mongo 保真契约仍成立。本轮没有新增测试、生产代码或存储格式。

结论：现有证据不支持为了这次整数修复扩大到 MessagePack 或存储链路重构，保留局部正确性修复。下一任务明确为 Fusion 持久版本与旧写条件保护的最小设计评估；先解决热数据版本跨过期/回填的连续性，再确认实现，不直接复用会过期的 `@v` 作为全生命周期版本。
