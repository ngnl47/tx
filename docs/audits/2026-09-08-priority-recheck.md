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
