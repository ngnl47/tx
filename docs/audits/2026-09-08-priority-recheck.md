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
