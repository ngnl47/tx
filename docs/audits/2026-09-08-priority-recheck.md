# 旧审计 P0/P1 当前状态复核

范围：复核 2026-08-31 六工程审计的 12 项发现，采用当前代码与既有回归证据。本文是代码路径复核，不是新一轮故障复现；普通集群通过不能证明竞争窗口已消失。未修改生产实现。

## 优先结论

Fusion 已按确认的最终纠正边界完成异常重新登记修补，见本文末尾验收。角色路由复核发现有状态示例启动入口已经校验绑定身份，不因 Handler 内没有重复校验就判定可绕过；后续重点为公开世界命令与内部调用的身份边界。下文复现章节保留各次实验条件，当前决策以最后的复核结论为准。

## 逐项状态

| 原项 | 当前状态 | 当前代码证据与下一动作 |
| --- | --- | --- |
| P0 Fusion 旧任务覆盖 | 按最终纠正边界完成 | 失效 Retry 返回 ErrLeaseLost，Fusion 复用 Enqueue 恢复任务；真实交错中补登记失败一次后最终值恢复。不承诺禁止短暂回退或连续入队失败后的恢复。 |
| P0 认证身份贯穿 | 角色入口已有校验；世界业务授权待复核 | `modules/agent/actor_dispatcher.go` 注入 Identity/UpSeq；有状态示例 `cmd/server.debugSlotID` 校验 role 路由等于可信 Identity.RoleID，不能仅看 Handler 宣布越权。显式世界目标与其中操作主体分开评审。 |
| P0 Topic 部分成功 | 已有针对性修复与证据 | `modules/msgq/broadcast_grpc_test.go` 覆盖远程部分结果，当前共享不可变载荷契约和角色分发已进入稳定文档；最近真实 Redis race 与双集群回归通过。不重复改造成 UDP 或全 MsgQ 广播。 |
| P1 Discovery Watch 联合就绪 | 已修补并通过受控时序回归 | 每轮独立快照写入入口，任一 Watch 返回即废弃本轮并清空目录，拒绝迟到发布；WaitReady 仅接受当前轮次两份快照。Discovery race 重复 10 轮通过。 |
| P1 Actor provider 竞态 | 真实 etcd 已复现，并发现成员目录并发问题 | 锁定依赖的 Shutdown 写 shutdown，与 keepAliveForever 读取冲突；同一生命周期用例还报告 MemberList.BroadcastEvent 与 UpdateClusterTopology 竞争。需要依赖级修补决策，tx 外部关闭锁不能覆盖内部协程。 |
| P1 TryRelease 取消链 | 风险路径仍在 | `modules/writeback/manager.go` 的 SPI 仍无 context，内部用 Background 等待实体锁。下一步需组合实体释放任务与关闭顺序复现，不仅测试手动解锁。 |
| P1 World_StartMarch 内外契约复用 | 身份与响应路径已分离 | 公开请求仅携带世界、单位目标；操作角色来自 Identity。内部调用透传可信身份但使用 UpSeq=0，MessageType 区分响应路径；两条路径经真实多进程验收。无需为相同行军语义新增一套内部协议。 |
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

## 旧租约失效后补登记失败的对照验证

后续决策调整：接受现有异常重新登记实现最终纠正，暂不采用新增 Mongo 持久版本及每次写回前读取；`DelayDelete=0` 作为测试配置不扩展生产补偿。本轮仅验证旧租约失效后的补登记失败，不改变 30s 任务租约、24h 角色热数据保留期或生产逻辑。

执行 `go test -race /private/tmp/fusion_requeue_failure_probe_test.go -run '^TestProbeFusionRequeueFailure$' -count=1 -v`。使用真实 Redis DB 15、Mongo `txgameslg_fusion_lease_probe`、生成 GDRole 和两个 Scheduler 对象（同一测试进程，不是两台独立服务）。旧 worker 在 Mongo 适配器调用前暂停，实际等待 31s 后由新 worker 重领并落库；确认新 worker 完成后该任务六个已知 Redis 队列 key 均不存在，再释放旧 worker。失败组只在旧 worker 的补登记调用进入 Redis 前注入错误一次；没有制造真实 Redis 断网、停止 Mongo 或伪造租约时钟。

| 组别 | 新 worker 完成后的 Mongo | 旧 worker 返回后的 Mongo | 后续领取数 | 最终 Mongo / Redis | 剩余任务 key |
| --- | --- | --- | ---: | --- | ---: |
| 补登记正常 | new | old | 1 | new / new | 0 |
| 补登记失败一次 | new | old | 0 | old / new | 0 |

失败组 `failedEnqueueCalls=1`，旧 Scheduler 的 `RunOnce` 返回 nil，日志却输出 `delay task failed, retry scheduled`。根因是 `retry.lua` 拒绝旧 token 并返回 stale，Backend.Retry 将 stale 映射为 nil；因此本次并未实际安排重试。用例最终断言 Mongo 应为 new，失败组按预期暴露缺陷；没有 Go 竞态报告。只运行一轮对照，不据此估计生产故障概率，也不宣称已测试真实网络故障。

结论：正常补登记路径有效；但“失效旧任务迟到落库 + 补登记失败”确实可能留下 Redis 新值而没有后续任务，不能认为下一实例总能重领。仍有最新 Redis 数据，尚未证明数据已永久丢失；后续业务写入可能重新触发落库。本轮精确清理两组创建的角色记录及已知 key，无扫描、无清库，诊断源保留在临时目录，不加入常规测试集。

下一步仅评审异常路径最小修补：区分 Retry 实际安排成功与旧租约失效，明确失效旧任务的重新登记责任，避免虚报重试成功；不直接取消 token 隔离，不新增正常写回 Mongo I/O。修复尚未实施。

### 异常重新登记修复验收

上述修补已实现：Redis/内存任务后端 Retry 对失效租约返回 `task.ErrLeaseLost`；Fusion Scheduler 仅在 handler 失败且遇到该错误时，通过现有 Enqueue 重新登记，并使用当前时间计算退避。成功写回路径、租约时长、热数据 TTL、Mongo 操作均未改变。连续入队失败仅如实报告，未新增常驻补偿系统。

新增一个三分支核心回归 `TestFusionLostLeaseRecovery`，先在旧实现上全部失败，再验证：已完成任务恢复、当前新 owner 不受干扰且保留后续任务、再次入队失败返回原始错误。未把临时 64 秒交错探针加入常规测试集。

相同真实依赖探针在 `-race` 下复跑通过：正常组与注入一次补登记失败组均为 `repairTasks=1 repairedValue=new redisValue=new taskKeys=0`，没有 Go 竞态报告。失败组首次 Enqueue 错误后，失效 Retry 触发重新登记并成功；不等同于任意网络故障保证。tx、txgame/server、txgameslg/server 的 `go test ./... -count=1` 全部通过。

本项按已确认的最终纠正边界完成。下一任务尚需重新检查剩余审计项优先级，不继续扩展本问题的故障组合。

## Session 绑定角色与显式路由复核

本轮沿实际装配重新检查，修正仅观察 Handler 得出的风险判断：txgameslg 的 debug/test 入口通过 `debugAgentHandlers` 安装 `debugSlotID`，它对 actor=role 要求帧头 int64 路由键等于可信 `Identity.RoleID`。随后业务入口再确认载荷路由键与帧头一致。因而客户端同时篡改帧头和 Role_Rename.RoleId 仍会被前一层拒绝。release 不安装该开发 SPI，需要项目自行注入；不将开发入口结论外推到任意自定义 SPI。

复用已有测试，无新增用例或生产实现：

- txgameslg/server：`go test -race ./cmd/server -run 'TestDebugAgentEntryAcceptsOnlyLoginTickets|TestDebugSlotIDUsesTargetIdentityAndRequestedWorld' -count=3 -v` 通过。覆盖可信票据、普通/多 Session 目标身份、错误角色目标拒绝以及 WorldID=1001/1002 显式路由允许。
- txgame/server：`go test -race ./services/agent -run 'TestVirtualTargetResolverUsesRoleForDefaultGameRoute|TestVirtualTargetResolverUsesGeneratedActorAndRouteKey|TestTicketIdentityResolverUsesTicketPrincipalInsteadOfClientClaims' -count=3 -v` 通过。默认无显式路由键时使用可信 RoleID，room 等业务目标保持显式路由。

这是入口组件级验证，不是一次完整网络登录及数据库越权攻击测试，也不代表所有 handler 业务权限已审计。当前没有证据支持为 Role_Rename 再增加一套重复校验。下一项独立评估 World_StartMarch 的载荷 RoleId 与可信操作主体，以及 RoleActor→WorldActor 内部调用身份传递；允许跨 WorldID 不等于允许伪造操作主体，不将这两类字段混为一谈。

## 世界命令可信身份真实集群验收

公开 World_StartMarch 已移除操作角色字段，RoleActor 内部调用透传 HandlerContext.Identity，两个示例的命令入口按服务端 MessageType 决定响应路径，不以是否携带 Identity 判断来源。

第一轮真实运行在内部行军路径等待通知超时：`/private/tmp/txgameslg-cluster-e2e/20260908-165802.080860000`。定位为 tx Delivery.Validate 仍将 Identity 与正 UpSeq 强绑定，内部调用在序列化前即失败。现允许合法身份配合 UpSeq=0，保留负序号及无身份携带序号的拒绝；既有信封回归增加真实往返断言，先失败后通过。

修复后完整套件通过，证据目录：`/private/tmp/txgameslg-cluster-e2e/20260908-170128.842179000`。generation 1、2 均记录 `role_world_call` 与 `direct_world_call` 成功，使用不同角色到达同一世界，分别收到通知与业务响应；多 Session 跨两个 Agent 的共享世界响应与恢复也通过。四轮客户端探针全部通过，套件覆盖 WAL 恢复、MsgQ 故障重启、存储断言和优雅关闭。

tx、txgame/server、txgameslg/server 全量 Go 回归通过；txclientslg 96 项测试与构建通过。本轮复用现有测试，不增加测试数量。以上不代表任意世界单位的业务所有权均已授权，目标单位权限仍由具体业务实现。

下一步复现 Discovery Watch 联合就绪的失效窗口；先确认取消与就绪跨轮次交错，再决定最小修补，不以本轮普通集群成功替代竞态验证。

## Discovery Watch 联合就绪窗口复现

使用临时包内探针驱动真实 Runtime、ServiceDirectory、SlotDirectory，只替换 Watch 数据源。以通道控制先后顺序，不停止 etcd、不模拟网络故障、不使用随机等待。执行 `go test ./modules/discovery -run '^TestProbeRuntime' -count=1 -timeout 10s`，两项均按预期失败：

1. 槽位 Watch 完成空快照后持续运行；服务 Watch 返回错误；槽位 Watch 收到取消后暂停退出。此时 ResolveSlot 返回 ErrSlotNotFound 而非 ErrSlotDirectoryUnavailable，说明失效的联合目录仍被视为可用。空快照用于直接区分“同步完成但没有目标”与“目录不可用”，不宣称已发生实际错误投递。
2. 服务目录 Replace 完成后使两份目录 Invalidate，再仅让槽位目录 Replace。WaitReady 返回 nil，错误组合了服务目录的历史 Ready 与槽位后来的 Ready。根因是 Ready 只关闭一次，Invalidate 不重置首轮同步信号。

进一步代码检查：另一条 Watch 接到取消前可能已取出快照、正在 Replace；仅提前 Invalidate 无法阻止该旧快照稍后重新填充目录。因此下一步修补需要同一 Watch 轮次的写入有效性和联合就绪约束，不能仅改清空顺序。

本轮没有修改生产代码；临时失败探针已移除，不把失败测试加入常规套件。修补时保留覆盖上述关键窗口的精简回归。建议边界：首次 Watch 退出即让联合目录不可用，旧轮次不能再次发布快照；新一轮必须两份快照均完成后才能让 Runtime.WaitReady 成功。重试间隔、etcd 租约及业务路由 API 不变，不增加 Redis/Mongo I/O。

### Watch 轮次隔离修补验收

已按上述边界实现。两条 Watch 使用本轮专属目录写入入口，通过 Runtime 的发布锁写入共享目录；第一条返回后，在同一锁内关闭本轮发布资格并清空目录，再取消并等待另一条 Watch。Runtime 单独记录本轮联合就绪，不改变独立目录 Ready 的首次同步语义。未增加高频路由读锁或外部 I/O。

新增一个组合回归 TestRuntimeWatchRejectsLateSnapshotAndRequiresCurrentPair：旧实现上复现“目录仍可用、WaitReady 返回 nil”；修复后覆盖取消后的迟到 Replace、另一条 Watch 延迟退出、下一轮仅槽位就绪仍等待、两份就绪后恢复。`go test -race ./modules/discovery -count=10` 通过；tx、txgame/server、txgameslg/server 的全量 Go 测试通过。本轮未执行真实 etcd 断网或多进程故障注入，不将受控时序结果描述为实际网络故障验证。

下一步复核 Actor etcd provider 的关闭与续租并发风险，先运行针对性竞态复现，再决定是否需要修补；不扩展本次 Watch 修复为通用快照事务系统。

## Actor provider 真实竞态复核

本地锁定依赖为 protoactor-go `v0.0.0-20240822202345-3c0e61ca19c9`。复用既有 TestActorRuntimeLifecycleIntegration，连接真实 etcd `127.0.0.1:2379`，使用用例生成的唯一 `/tx/tests/actor-<时间戳>` 前缀；未新增用例、未关闭 etcd、未修改模块缓存。

执行：`TX_ETCD_ENDPOINT=127.0.0.1:2379 GORACE='log_path=/private/tmp/tx-actor-provider-race history_size=0' go test -race ./tests -run '^TestActorRuntimeLifecycleIntegration$' -count=1 -timeout 30s`。结果失败，3.131s；精确竞态报告保留在 `/private/tmp/tx-actor-provider-race.90459`。

- provider 的 Shutdown 在 etcd_provider.go:117 写普通 bool shutdown，与 keepAliveForever 在 :168 读取发生竞争。tx Runtime 自身关闭锁无法保护第三方后台协程。
- 同次报告还发现 MemberList.BroadcastEvent（member_list.go:228 附近）与 UpdateClusterTopology（:147 附近）并发访问拓扑状态；来自 etcd Watch 的拓扑更新与 Actor 终止广播交错。该问题不是把 shutdown 改成原子变量就能一并解决。

此前宽范围 `^TestActorRuntime` 执行也报告竞态，同时日志测试出现预期日志与初始化日志不匹配；本轮用单个生命周期用例排除了该额外日志断言对上述结论的干扰，不顺带修改日志测试。

本轮只更新审计记录，不宣称修复完成。后续需评估依赖级补丁范围：至少包括 provider 关闭标记同步和 MemberList 拓扑读取同步；不直接复制整套依赖，不修改 Go 模块缓存，不通过延时关闭或关闭 race 检测掩盖问题。是否采用固定修订的依赖分支，需要明确维护方式后再实施。

## 严格排水第一步：统一准入与可重试等待

当前决策：不维护第三方补丁分支，暂缓替代 Actor 库评估；允许滚动更新期间请求失败、断线重连，先完成现有运行时的严格排水闭环。

已将 Actor BeginDrain 拆成不阻塞的准入封口，Server.Drain 先关闭服务和 Actor 本地新业务准入，再发布 draining，最后等待服务与 Actor 已接收工作完成。控制面 WaitDrained 因此包含 Actor 邮箱工作，不再仅在 Stop 阶段等待。控制面及服务 Drain 回调失败后允许重试；每轮独立保存结果，成功回调不因仅等待在途请求超时而重复执行。

验证：控制面超时后重试的既有测试先失败后通过；新增一个服务回调失败后重试测试；既有真实 etcd Actor 排水用例补充 BeginDrain 后立即拒绝新请求、等待超时后保留原工作并再次排空成功。`TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./tests -run '^TestActorRuntimeDrainWaitsForQueuedMessages$' -count=1 -timeout 30s` 通过。framework 与 actor 包 race 三轮通过，未声称第三方真实集群竞态已消失。

严格关闭失败路径已补齐：正常 Server.Stop 在入口或持久化失败时保留资源并返回，组件 Drain 从失败位置重试。后台任务先退出，最终刷盘成功后才调用项目 Service.Stop 和释放资源。Run 首次失败后间隔 1 秒重试三次，仍失败则记录错误与数据丢失风险，交由进程入口非零退出；不无限等待，也不冒充安全交接。运行身份丢失仍执行异常终止，包括租约失效恰好发生在普通关闭过程中的情况。

下一验收节点：真实多进程滚动关闭与失败注入，检查旧节点拒绝上行、已有请求完成、刷盘后释放，以及重试耗尽后的非零退出与日志留存。本轮单进程生命周期回归不能替代该验收，亦未修改第三方 Actor 库。

本轮验证：关闭失败保留资源、Run 恢复成功与重试耗尽、组件失败位置续刷、关闭期间运行身份丢失四类核心回归覆盖；`go test -race ./internal/framework ./component -count=2`、tx 全量 `go test ./... -count=1` 与两套示例 server 全量测试均通过。重试不是固定三秒退出承诺，各轮等待仍使用对应阶段的超时。

后续重评条件：有限重试是当前 protoActor 方案的明确基线。若决定采用 GoAkt，且验证其生命周期机制能解决上述排水状态协调问题，再按 GoAkt 的方案重构关闭策略，不为旧策略保留兼容分支。评估须覆盖滚动更新、故障恢复及状态交接，并单独确认最终持久化失败的处理与日志留存；目前不据此启动选型替换，下一步仍是真实多进程验收。

## 真实多进程关闭验收（2026-09-08）

串行执行三套验收，均使用真实 etcd、Redis、Mongo 和对应客户端，结果均为 passed：

- 无状态标准集群：`/private/tmp/txgame-strict-drain-e2e/20260908-192531.677555000/summary.json`。覆盖双 Agent/Login/MsgQ、Game 重启、消息链路与优雅退出。
- 有状态标准集群：`/private/tmp/txgameslg-strict-drain-e2e/20260908-192658.685850000/summary.json`。覆盖 World 故障后的 WAL 恢复、generation 2 接管、checkpoint 及正常进程排水退出。
- 有状态关闭失败分支：`/private/tmp/txgameslg-shutdown-failure-e2e/20260908-192902.944193000/summary.json`。通过 `go run ./cmd/cluster-e2e --shutdown-failure --artifact-root /private/tmp/txgameslg-shutdown-failure-e2e --timeout 8m` 执行。

新增分支只复用专用 Mongo world_buckets validator 阻断 checkpoint，不停止共享 Mongo 服务。向旧 slgworld-a 发送 SIGTERM，最终刷盘每轮 15 秒；日志在 19:30:10.517、19:30:26.521、19:30:42.527、19:30:58.533 记录 attempt 1～4，最后记录重试耗尽与未刷盘数据风险，进程自行返回 exit status 1。编排器未向此进程发送 SIGKILL。解除 validator 后 slgworld-b 恢复 WAL，摘要记录 shutdownRetries=3、walRecovered=true、worldGeneration=2、worldCheckpointSeq=1。全部原始日志和退出码保留在同目录 processes 中。

补充真实 etcd 用例 `TestActorRuntimeDrainWaitsForQueuedMessages` 通过，验证封口后新请求拒绝、已接受的排队工作继续完成；该精确用例是单进程运行时验证，不冒充跨进程在途消息故障注入。txgameslg/server 全量测试与 diff 检查通过。三套集群成功后由既有 ownership 校验流程清理专用外部测试数据，artifact 保留。

本节点完成正常滚动关闭与 checkpoint 失败重试耗尽的真实进程验收；未证明网络分区下全局唯一 Actor，未消除已记录的第三方竞态，也未新增真实进程“中途恢复后重试成功”故障分支（该路径目前由生命周期回归覆盖）。下一步进入 GoAkt 替代可行性讨论：先核实生命周期保证、路由/激活能力与固定槽位适配，再评估迁移成本，不直接更换依赖。
