# tx 六工程整体架构审计报告（2026-08-31）

本报告按 [`../architecture_audit_standard.md`](../architecture_audit_standard.md) 执行。审计阶段只记录证据和建议，不修改实现，也不提前把尚未确认的整改方案写入实施计划。

## 审计基线

| 工程 | Revision | 审计前状态 |
| --- | --- | --- |
| `tx` | `06445d33d40ac036189d06d9c67cf71d61c3d1ff` | clean |
| `txgen` | `29d30afb30a04ebf7c2a9de38460315e9870efb1` | clean |
| `txgame` | `2275874a7b84189d280b8a4034162eea7fde3fd1` | clean |
| `txgameslg` | `e77f56f4ed649592055997b75d5f261e1d8c1f3f` | clean |
| `txclient` | `7d177a04d1dbd84e1d361f472e24faab65082ee3` | clean |
| `txclientslg` | `96175c0a5524b414779531a4c9211834fc03773a` | clean |

审计环境：`go1.27.0 darwin/amd64`，macOS `15.7.9`，`x86_64`。

审计范围包括六工程当前代码和生效文档，重点覆盖 Delta/Store/Fusion/延迟任务、WriteBack/EntityCache、WAL/checkpoint、Agent/MsgQ/Seq-Ack/恢复、RPC/Actor、etcd 服务发现与槽位迁移、txgen 命令编号和 Go/TypeScript 协议，以及两套真实多进程集群验收。`World_StartMarch` 按客户端公开 request/response 协议审查；它的公开可达性本身不是问题。

## Findings

### [P0] 过期 Fusion worker 可在新 worker 完成后无条件覆盖 Mongo

- 位置：`storage/fusion/handler.go:77-120`、`storage/fusion/strategy.go:16-19`、`storage/task/redis/const.go:5-9`、`storage/task/redis/scripts/enqueue.lua:17-20`、`storage/task/redis/scripts/claim.lua:16-39`、`storage/mongo/adapter.go:201-227`、`storage/mongo/mongodriver/database.go:110-127`、`storage/task/redis/backend.go:155-199`。
- 触发条件：worker W1 取得默认 30 秒任务 lease 和 10 秒记录锁，读取旧文档 A/revision 1 后阻塞于 Mongo；业务随后写入 B/revision 2。W1 的记录锁和任务 lease 依次过期后，W2 取得新 token、把 B 写入 Mongo 并完成；W1 随后恢复并以仅含记录键的无条件 `ReplaceOne(upsert=true)` 把 Mongo 覆盖回 A。
- 影响：任务 token 只隔离队列结算，没有隔离已经开始的 Mongo 副作用。旧 lease 的 `Complete`/`Retry` 把 `stale` 视作成功是合理的幂等队列结算，但它不能 fence 或撤销已经发出的 Mongo Replace。`DelayDelete <= 0` 时 handler 完全不读取 revision，B 可永久丢失；正值时，W2 完成并缩短 Redis TTL 后，若 W1 在该 TTL 到期后才恢复，它会先把 Mongo 覆盖回 A，随后 revision 比较因热数据已消失而报错，旧 lease 的 Retry 只能得到 `stale`，而任务已由 W2 删除，A 因此永久保留。W1 在 TTL 到期前恢复时通常会重排任务，但 Mongo 仍存在旧值窗口。该事件属于并发覆盖和错误成功。
- 证据：设计文档 `docs/design.md:1524-1532` 要求延迟保存始终读取并比较 revision，仅 TTL 缩短步骤受 `DelayDelete > 0` 控制；当前实现把 revision 检查整体放进该条件。`tests/fusion_task_handler_test.go:193-260` 只覆盖同步 revision 变化，并断言零延迟不读取 revision；`tests/task_backend_redis_test.go:427-488` 只验证 lease 可被重新领取，没有让 W1 在 W2 后恢复并完成。
- 建议：在 Mongo 写入边界增加基于单调 revision/generation 的条件替换，使过期 worker 的外部副作用也被 fence；同时所有 `DelayDelete` 分支都读取并比较 revision，revision 变化时显式重排保存任务，并增加 W1/W2 交错故障测试。

### [P0] Agent 认证身份没有穿过 Actor 边界，可造成跨角色脏写

- 位置：`modules/agent/session.go:81-104,190-223`、`modules/actor/proto/actor.proto:20-30`、`modules/actor/delivery.go:18-28`、`txgameslg/server/services/agent/actor_dispatcher.go:61-84`、`txgameslg/server/services/internal/actorcmd/command_actor.go:254-286`、`txgameslg/server/services/internal/commandroute/route.go:54-66`、`txgameslg/server/services/slggame/handler.go:74-89`。`txgame/server/services/agent/actor_dispatcher.go:61-84` 存在同构边界。
- 触发条件：角色 A 使用有效凭据建立会话，再发送 payload 与帧头都指向角色 B 的命令。Agent 的 `Identity` 只参与目标解析；构造 `Delivery` 后身份被丢弃。Actor 端只能检查 CommandID 以及“帧 RouteKey 等于 payload RouteKey”，无法确认该角色是否等于已认证角色 A。
- 影响：`Role_Rename{RoleId:B}` 会加载并持久修改 B；`txgame` 的 `System_StorageProbe{RoleId:B}` 也会写入 B。访问日志仍归因于 A，且按认证角色分槽时还可能把 B 的实体加载到错误槽位。`World_StartMarch` 作为客户端公开协议没有问题，但其 handler 同样无法取得认证角色来授权 payload 中的 `RoleId`。
- 证据：独立最小复现以认证角色 `470001`、payload/route 角色 `470002` 调用现有 dispatcher 和 actor 校验，返回 `dispatch-error=<nil>` 并进入 handler。现有 `txgameslg/server/services/agent/bon_dispatcher_test.go:136-178` 和真实客户端验收只覆盖身份与角色字段一致的正向路径。
- 建议：把可信身份/claims 作为 Actor `Delivery` 和生成 `HandlerContext` 的一等字段跨进程传递；对“当前角色”命令在统一边界校验身份、Actor route 与业务角色键。允许操作其他角色的命令应通过显式授权契约表达，不能全局假定每个 `RoleId` 都等于登录角色。

### [P0] Topic 广播部分成功时违反 payload 所有权和远程结果契约

- 位置：`modules/msgq/mailbox.go:141-153,553-598`、`modules/msgq/grpc.go:99-110`、`modules/msgq/sender.go:116-133`、`README.md:451-456`。
- 触发条件：订阅者 S1 有队列容量、S2 已满。`BroadcastTopic` 先把同一 `[]byte` 引用放入 S1，再因 S2 溢出返回统计和 `ErrDownMsgOverflow`。循环中 context 取消也可在部分投递后直接返回 error。公开契约只声明“成功后”转移 payload 所有权，因此调用方会合理地认为错误返回后仍可复用 buffer。
- 影响：调用方在错误后修改 buffer 会直接污染 S1 已受理的下行消息；本地最小复现得到 `Delivered:1, Overflowed:1` 后把 `fresh` 改为 `Xresh`，S1 随后确实取到 `Xresh`。跨进程时 gRPC handler 返回 `response + error`，客户端遇到 error 直接丢弃 response，调用方看不到部分成功统计并可能重试，进一步造成重复下行。
- 证据：`modules/msgq/mailbox_test.go:202-231` 只覆盖全成功时共享引用；`modules/msgq/grpc_client_test.go:107-129` 只覆盖成功响应。没有“一个成功、一个溢出、错误后复用 payload”和 gRPC 部分结果用例。
- 建议：把单会话溢出建模为正常 RPC 结果中的逐批状态，传输层在已经发生任何投递时不再以普通 error 丢弃响应；同时把所有权契约改为明确的无歧义规则。应继续共享同一只读 payload，不为每个玩家复制正文。

### [P1] Discovery 的两份 Watch 没有按同一 epoch 完成即时失效和就绪

- 位置：`modules/discovery/runtime.go:92-143`、`modules/discovery/directory.go:68-75,101-155`、`modules/discovery/slot_directory.go:10-65`、`docs/design.md:2199-2205`。
- 触发条件一：Service Watch 先返回，`watchOnce` 虽然立即 cancel Slot Watch，却要等待它返回后，外层才同时 `Invalidate` 两份目录；若第二个 Watch 退出延迟，旧快照在窗口内仍可被读取。
- 触发条件二：Service 在 epoch 1 已关闭永久 `Ready`，但 Slot 尚未首轮同步时 Service Watch 失败；epoch 2 的 Slot 先同步并关闭自己的永久 `Ready`。此时 `WaitReady` 会把 epoch 1 留下的历史 Service Ready 关闭事件与 epoch 2 的 Slot Ready 组合为成功，而当前 ServiceDirectory 仍可能处于 invalid 状态。
- 影响：进程可用不同 epoch 的历史 readiness 信号完成启动就绪，或者在 Watch 断口继续尝试旧路由；这违背“任一 Watch 中断即共同 fail closed”的路由一致性域契约。即使最终重试会收敛，启动和中断窗口的生命周期语义仍不完整。
- 证据：`modules/discovery/runtime_test.go:53-63` 只验证已取消 context，没有首轮快照交错、单 Watch 延迟退出或重建 epoch 的测试。
- 建议：由 Runtime 管理可重建的联合 epoch/ready 状态；任一 Watch 返回时先原子失效两份目录，再等待另一个 goroutine 收尾。只有同一 epoch 的两份完整快照都生效后，联合 readiness 才可完成。

### [P1] Actor etcd provider 关闭标志存在确定数据竞态

- 位置：`modules/actor/runtime.go:171-180,297-314`；依赖 `github.com/asynkron/protoactor-go@v0.0.0-20240822202345-3c0e61ca19c9/cluster/clusterproviders/etcd/etcd_provider.go:116-129,163-180`。
- 触发条件：服务关闭调用 `cluster.Shutdown(true)` 写入 provider 的普通 `shutdown bool`，keepalive goroutine 同时在循环中无同步读取该字段。
- 影响：`go test -race ./... -count=1` 在 `txgame/server` 和 `txgameslg/server` 都稳定失败；keepalive/重新注册还可能与注销交错，使 Actor 成员在租约 TTL 内继续以陈旧状态可见。该问题位于第三方 provider，但当前 Runtime 直接承担它的生命周期，因此仍是当前受支持链路的门禁问题。
- 证据：race 报告的写端是 provider `Shutdown:117`，由 `modules/actor/runtime.go:313` 触发；读端是 `keepAliveForever:168`。审计当日[官方开发分支中的 provider](https://github.com/asynkron/protoactor-go/blob/dev/cluster/clusterproviders/etcd/etcd_provider.go)仍使用同一无同步标志，不能仅假定升级即可消除。
- 建议：评估升级后仍需以本地最小 patch/替代 provider 修正：用 cancelable context 或原子状态关闭 keepalive，等待后台 goroutine 退出后再完成注销；不得通过跳过 race 用例掩盖。

### [P1] `TryRelease` 丢弃取消上下文，可与关闭顺序形成不可退出环

- 位置：`modules/writeback/manager.go:43-50,302-317,361-373,571-585`、`txgameslg/server/services/internal/entitycache/release.go:74-86,133-159`、`internal/framework/shutdown.go:102-140`、`internal/framework/service_host.go:145-166`。
- 触发条件：CMod WriteBack 的后台 Flush 持有实体锁，后端 I/O 阻塞且只随传入的 CMod lifecycle context 取消；EntityCache 的单 release worker 同时调用 `TryRelease`，后者用 `context.Background()` 等待同一实体锁。框架关闭先取消并无上限等待 service tasks，只有该步骤返回后才会取消 CMod/root 生命周期。
- 影响：release worker 无法观察 service context，Flush 又等尚不可达的 CMod cancellation，`Server.Stop` 即使收到已超时 context 也不能返回；进程最终被强杀时可能跳过最后一次 Flush。
- 证据：写回测试和 EntityCache 串行释放测试都通过人工解除阻塞完成，没有组合真实关闭顺序和到期 Stop context。
- 建议：首先把取消链贯穿 `EntityCache -> 项目 facade -> Runtime.TryRelease -> txn.WithLocks`，将 SPI 改为 `TryRelease(context.Context, txn.LockTarget)`，release worker 传入自身 service context。若后续要让 service task wait 受 Stop deadline 约束，必须先明确超时后的资源存活或进程强制终止语义，不能在任务仍可能使用 CMod/存储时继续关闭依赖。增加上述锁等待/关闭顺序的回归测试。

### [P1] 客户端公开 `World_StartMarch` 被复用为内部 Actor/RPC 契约

- 位置：`txgameslg/gen/cmd/world.go:22-33`、`txgameslg/server/services/slggame/world_caller.go:41-87`、`txgameslg/server/services/slgworld/handler.go:120-147`、`txgameslg/README.md:45-63`、`txclientslg/README.md:49-60`。
- 触发条件：客户端直接发送公开 `World_StartMarch` 时，Agent 按生成的 request/response 描述路由到 WorldActor；另一路 `Role_StartMarch` 又在 RoleActor 中构造同一 `World_StartMarch`，把它作为内部 Actor Call 的 payload，并在内部消费同一个 `World_StartMarchResp`。
- 影响：同一类型同时承担外部协议和内部服务契约，文档甚至把公开客户端命令称为“内部命令”。这使调用来源、身份授权、演进兼容和响应归属无法在类型中区分；修复认证身份时也难以判断 handler 应校验客户端 claims 还是信任内部调用方。公开可达性本身正确，缺陷是内部复用导致的边界混淆。
- 证据：生成客户端已公开该 request/response 和 CommandID，但 README 又要求客户端通常不要使用；现有测试只有内部 `world_caller` 路径，没有客户端直发协议的 route/response 回归。
- 建议：保留 `World_StartMarch`/`World_StartMarchResp` 作为客户端公开协议；RoleActor→WorldActor 若仍需要二次调用，应使用单独的内部 Actor message/response 契约，并显式携带已经验证的调用主体和必要业务字段。

### [P1] 有状态真实集群 Promote 后没有等待进程内 Watch 可见性屏障

- 位置：`txgameslg/server/internal/clustere2e/suite.go:221-232,288-302`、`txgameslg/server/internal/clustere2e/dependencies.go:250-271`、`txgameslg/README.md:219-221`。
- 触发条件：套件通过控制 RPC 完成 CAS Promote 后，只用独立 etcd client `Get` 等待持久 assignment 变为 generation 2，然后立即启动客户端流量；Agent 和新 owner 的本地 `SlotDirectory` 仍可能停留在 generation 1。
- 影响：验收可能因 Watch 延迟偶发路由到旧/不可用 owner；更重要的是，它没有验证 README 要求的“本进程 `ResolveSlot` 已看到目标 generation 后才能放流”，因此当前绿色结果不能证明发布屏障已经闭合。
- 证据：本轮真实验收通过，但等待函数读的是 etcd 持久值，不是任一服务进程的本地缓存；`server/internal/clustere2e/suite_test.go:10-35` 也只校验进程拓扑。
- 建议：为验收控制面提供只读的本地 slot generation 等待/探针，在放流前至少确认 Agent 和目标 owner 的本地 Watch 已观察到 generation 2；增加可控 Watch 延迟测试，不能用固定 sleep 替代屏障。

### [P1] TypeScript 模型丢失 Go 指针 null 和 `omitempty` 线协议语义

- 位置：`txgen/gen/ts_model.go:266-333`、`txgame/gen/cmd/rank.go:14-18`、`txclient/src/gen/cmd.ts:2061-2065`、`txclient/src/gen/schema.ts:3516-3520`、两客户端 `src/protocol/codec.ts:55-95`；零值响应示例位于 `txgameslg/server/gen/cmd/World_StartMarchResp.go:6-8` 和 `txclientslg/src/gen/cmd.ts:133-136`。
- 触发条件一：Go 发送 `[]*data.GDRankMember{nil}`；生成器递归剥掉 `*`，schema 要求对象，TypeScript decoder 遇到 BON null 后报“解码结果不是对象”。
- 触发条件二：Go 的生成字段都带 `omitempty`。例如 `World_StartMarchResp{Accepted:false}` 编码为空 body，TypeScript 实际解码得到 `{}`，但接口仍声明必填 `accepted: boolean`。
- 影响：合法 Go 载荷可让客户端运行期解码失败；即使不失败，静态类型也会把实际可能缺失的字段承诺为必定存在，破坏跨语言强类型契约。
- 证据：Go→TypeScript 交叉复现分别得到 pointer element 解码异常，以及 `{ command: 'World_StartMarchResp', body: {} }`。当前生成测试反而固定了非空、必填输出，两客户端没有 nil pointer 或 omitted zero 的交叉测试。
- 建议：在 txgen 类型模型中保留指针层级和字段 presence 信息，让 schema 能表达 nullable；对 `omitempty` 要么生成可选属性，要么由 decoder 按 Go 零值规则规范化缺失字段，二者必须统一。补齐指针字段、slice/map 指针元素、零值标量和空容器的 Go↔TypeScript 矩阵测试。

### [P1] TypeScript 整数只校验 JS 安全范围，没有校验 Go signedness 和位宽

- 位置：两客户端 `src/protocol/codec.ts:122-143,201-217`，代表 schema 为 `txclientslg/src/gen/schema.ts:95-102`。
- 触发条件：客户端把 `bucketId: -1` 编码为 `uint32`，或向 `int8/int16/int32/uint8/uint16/uint32` 字段传入超出目标类型范围但仍属于 JS safe integer 的值。
- 影响：TypeScript 编码器接受并发送服务端必然拒绝的协议，错误从客户端边界推迟到 Go 解码；decode 方向也没有检查 schema 声明的目标整数边界。
- 证据：最小复现成功编码 `World_MoveUnit.bucketId=-1`，同一字节流交给 Go 解码返回 `bon: -1 overflows uint32`。现有测试只覆盖超出 JS 安全整数范围。
- 建议：按每个 scalar name 在 encode/decode 两侧校验 signedness 和精确位宽；`int64/uint64` 在当前 `number` 方案下继续限制在相应符号且不超过 JS safe range，并增加全部边界值与越界一单位测试。

### [P1] TypeScript 生成失败仍会永久修改 CommandID manifest

- 位置：`txgen/gen/render_ts.go:9-35`、`txgen/gen/cmd_route.go:33-52,211-254`、`txgen/docs/design.md:296-302`。
- 触发条件：新增请求命令名与 TypeScript 生成保留名冲突。`GenerateTypeScript` 先调用 `prepareCmdRoutes`，在持锁事务中分配并原子写回 CommandID；随后才构建模型并执行保留名校验。
- 影响：生成明确返回失败，却在稳定编号清单中永久占用新 ID；修正输入后编号空间和 retired 语义已被失败尝试改变。这违反“可确定输入错误在第一次产物写入前发生”的生成契约。
- 证据：最小复现以 `CommandName` 触发保留名错误，函数返回 error 后 manifest 仍出现 `CommandName: 100`。现有保留名测试没有配置 route/manifest，因而看不到副作用。
- 建议：在取得 manifest 写锁并提交之前完成所有扫描、模型构建和名称/协议校验；或把编号分配保持为内存计划，所有生成产物成功规划后再一次提交 manifest。增加失败前后 manifest 字节完全一致的测试。

### [P1] 文档承诺的 `Config.LogOutput=nil` 会在真实生成输入上 panic

- 位置：`txgen/README.md:390-393`、`txgen/gen/render_ts.go:73-76`。
- 触发条件：调用方按 README 设置 `LogOutput=nil`，且生成输入存在需要报告的 TypeScript fallback 类型；`reportTypeScriptFallbackTypes` 直接把 nil writer 传给 `fmt.Fprintf`。
- 影响：公开配置明确承诺“nil 关闭提示”，实际却发生 nil pointer panic，生成工具无法用 error 处理该输入。
- 证据：以当前 `txgame` 输入和 `LogOutput=nil` 运行 TypeScript 生成，可稳定在 `render_ts.go:75` 触发 panic。其他提示函数已有 nil guard，问题仅在此路径。
- 建议：在该 reporter 入口对 nil 直接返回，并增加“存在 fallback 且 LogOutput=nil”回归测试；不需要引入额外配置容错层。

## 已验证通过

- 六工程审计开始时均为 clean；审计复现只写入 `/private/tmp`。
- `tx`、`txgen`、`txgame/gen`、`txgame/server`、`txgameslg/gen`、`txgameslg/server` 的 `go test ./... -count=1` 全部通过；两个 server 模块的真实 Redis/Mongo 集成测试也执行通过。
- 上述六个 Go module 的 `go vet ./...` 全部通过。
- `txclient` 的 75 项测试和 `txclientslg` 的 80 项测试全部通过；两端 `npm run build` 均通过类型检查和 Vite 构建。
- `tx`、`txgen`、`txgame/gen`、`txgameslg/gen` 的 `go test -race ./... -count=1` 通过；`txgame/server` 与 `txgameslg/server` 因同一 Actor etcd provider 关闭竞态失败，不能把全量 race 记为通过。
- 两套 data/cmd/server/client 生成入口在临时目录重生成后与仓库产物零差异，CommandID manifest 也没有正常生成漂移。
- 无状态真实多进程验收通过，产物位于 `/private/tmp/txgame-cluster-e2e-audit/20260831-103440.264027000`。它独立启动 `agent`、两个 `msgq`、两个 `game`，并验证实例摘除、重启、跨进程链路和会话重新分布。
- 有状态真实多进程验收通过，产物位于 `/private/tmp/txgameslg-cluster-e2e-audit/20260831-103630.170399000`。它独立启动 `agent`、两个 `msgq`、两代 `slggame`、两代 `slgworld`，验证 generation 迁移、WAL 恢复与清理、checkpoint、旧 generation 写 fence、MsgQ 崩溃后新登录和最终优雅关闭。其本地 Watch 发布屏障缺口已单独列为 P1，不能被成功结果抵消。

## 当前性能基线

以下结果由 Go benchmark 自报 `Intel Core i9-9880H @ 2.30GHz`、后缀 `-16`；全部使用 `-count=3 -benchmem`，Procedure 组合额外使用 `-benchtime=500ms`。没有同环境旧 revision 原始结果，因此只固化当前范围，不据此判断跨版本提升或回退。

| 路径 | 三次结果范围 |
| --- | --- |
| BON envelope Encode / Decode | `1926-1951 ns/op, 1244 B/op, 12 allocs/op` / `3143-3160 ns/op, 2336 B/op, 43 allocs/op` |
| SlotRouter cached resolve 串行 / 并行 | `108.7-113.6 ns/op` / `49.30-56.40 ns/op`，均 `0 B/op, 0 allocs/op` |
| WriteBack commit participant | `2593-2639 ns/op, 2234 B/op, 25 allocs/op` |
| 事务锁单 key / 4 key | `233.7-242.6 ns/op` / `946.2-1103 ns/op`，均 `0 B/op, 0 allocs/op` |
| 事务锁共享 key / 不相交 key 并行 | `1145-1220 ns/op` / `86.51-111.0 ns/op`，均 `0 B/op, 0 allocs/op` |
| Procedure RootOnly / 复用根锁 / 动态加锁 | `925.3-943.7 ns/op` / `1129-1159 ns/op` / `1731-1762 ns/op` |
| 生成 Store 重复 map key | `64.85-67.50 ns/op, 0 B/op, 0 allocs/op` |
| 生成 Store SuccessfulFlush1024 | `258.7-266.3 us/op, ~194.94 KB/op, 2194 allocs/op` |
| SnapshotMap100 / StoreMap100 | `55.49-56.68 us/op, ~26.9 KB/op, 320 allocs/op` / `7.63-8.18 us/op, 12.85 KB/op, 157 allocs/op` |
| txgame 1024 叶子变化 Redis / Mongo 文档边界 | `905.9-941.1 us/op, 291.98 KB/op, 2743 allocs/op` / `1.012-1.060 ms/op, 263.37 KB/op, 3768 allocs/op` |
| Actor delivery lifecycle | `62.07-63.01 ns/op, 0 B/op, 0 allocs/op` |
| 固定 Actor 既有实例串行 / 并行 | `10.017-10.097 us/op, 729-737 B/op, 12 allocs/op` / `1.193-1.859 us/op, 636-642 B/op, 10 allocs/op` |
| RPC direct / loopback gRPC 串行 / loopback gRPC 并行 | `18.44-18.89 ns/op` / `187.1-195.0 us/op` / `44.08-45.55 us/op` |
| EntityCache hit：IdleDisabled / IdleEnabled / Parallel | `20.28-20.41 ns/op` / `178.0-190.2 ns/op` / `71.91-76.36 ns/op`，均 `0 B/op, 0 allocs/op` |

复现命令：

```text
cd /Users/a47/workspace/tx
go test ./tests -run '^$' -bench '^(BenchmarkBONEnvelopeEncode|BenchmarkBONEnvelopeDecode|BenchmarkSlotRouterCachedResolve|BenchmarkSlotRouterCachedResolveParallel|BenchmarkTxnRuntimeCommitToWriteBackManager)$' -benchmem -count=3
go test ./modules/txn -run '^$' -bench '^BenchmarkLockTableAcquireRelease$' -benchmem -count=3
go test ./modules/txn -run '^$' -bench '^BenchmarkProcedureCall$' -benchmem -benchtime=500ms -count=3
go test ./modules/actor -run '^$' -bench '^(BenchmarkDeliveryLifecycleAccounting|BenchmarkFixedActorExisting)$' -benchmem -count=3
go test ./modules/rpc -run '^$' -bench '^BenchmarkDeliveryTransport$' -benchmem -count=3

cd /Users/a47/workspace/txgen
go test ./tests -run '^$' -bench '^BenchmarkGeneratedStoreDirtyRuntime$/(RepeatedMapKey|SuccessfulFlush1024)$' -benchmem -count=3
go test ./tests -run '^$' -bench '^BenchmarkGeneratedPersistenceModes$/(SnapshotMap100|StoreMap100)$' -benchmem -count=3

cd /Users/a47/workspace/txgame/server
go test ./tests -run '^$' -bench '^BenchmarkGeneratedStorageChangeBoundary$' -benchmem -count=3

cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run '^$' -bench '^(BenchmarkCacheHit|BenchmarkCacheParallelHit)$' -benchmem -count=3
```

当前 `modules/msgq` 没有包内 benchmark；两个客户端只有长时间真实集群性能入口。本轮没有运行它们，也没有足够证据形成新的 P2 finding。后续应补无外部依赖的 MsgQ 单播、Topic fanout、overflow、Ack 裁剪，以及两客户端 BON codec 微基准，形成稳定回归门禁。

## 明确排除的误报

- `World_StartMarch` 是客户端公开 request/response；生成客户端公开它是正确行为。finding 只针对服务内部复用同一公开类型。
- 同一 `SessionID` 的 Restore 更换 `ConnectionID` 后，已受理旧请求的迟到下行仍属于同一逻辑会话。`Pub` 不校验 `ConnectionID` 与当前既定恢复语义一致；新登录使用新 `SessionID`，旧会话结果会被拒绝。
- txgen 对无法扫描但明确允许保留的外部 TypeScript 类型生成 `unknown` 并告警，是当前文档明确的降级，不作为 finding。
- 固定槽位的异常自动接管、网络分区仲裁、MsgQ 进程崩溃后恢复未确认正文以及控制面生产认证/TLS，仍是当前文档明确未完成或非本阶段能力，不伪装成本轮已验收功能。

## 残余风险

- 本轮真实集群仍运行在单机单节点 Redis、Mongo 和 etcd 上；没有覆盖原生 Redis Cluster、Mongo 副本集选主、etcd quorum 丢失、跨机器网络分区和时钟异常。
- WAL/checkpoint 验收覆盖了进程 crash 和 generation 接管，但没有做磁盘损坏、部分写、fsync 故障和 checkpoint 文档损坏注入。
- MsgQ 弱网恢复覆盖既定 180 秒窗口和正常 Restore 语义；进程本身崩溃时未确认正文仍只在内存，当前不承诺跨进程恢复。
- 本轮没有发现新的 P2 结论。代表性 H0/H1 benchmark 仅用于固化当前机器基线；没有同环境旧 revision 对照时，不据此宣称性能提升或回退。

## 建议实施顺序

1. 先处理三个 P0：Fusion 外部副作用 fencing、认证身份贯穿 Actor、Topic 广播部分成功的所有权/结果闭环，并为每项增加确定性交错或负向测试。
2. 修复 Discovery 联合 epoch、Actor provider 关闭竞态和 `TryRelease(ctx, ...)` 关闭环，恢复两个 server 模块的完整 race 门禁。
3. 固定协议边界：保留公开 `World_StartMarch`，拆分内部 Actor message；让有状态集群套件等待真实的本地 slot generation 屏障。
4. 统一修复 txgen/TypeScript 的 nullable/presence、整数范围、失败零副作用和 nil 日志契约，再重生成两个客户端并做 Go↔TypeScript 交叉测试。
5. 全部 P0/P1 方案确认后再写分阶段实施计划；每个阶段分别执行普通测试、vet、race、生成零差异和受影响的真实集群验收。
