# 上下行异步流重构实施计划

> **执行方式：** 当前会话内由我逐项实施；按每项的测试先行、逐仓库验证，保留所有既有未提交改动，不提交 Git。

**目标：** 在 tx Agent、MsgQ、Game 的实例直连拓扑中，以流替代目标路径的逐消息同步 RPC 等待，保持单 Session 顺序并按已确认边界允许丢失。

**架构：** Agent→Game 复用直连 `CallStream`，增加无需逐项响应的入队模式；Agent→每个实际 MsgQ owner 使用一条复用多 Session 的 `MailboxStream`；Game/SLGWorld→MsgQ 使用独立 `PublishStream`，仅在旧角色归属被明确拒绝时异步回报。流按 Discovery/owner 路由解析出的具体实例建立，不引入中间负载均衡，也不做定时等待合批。角色旧归属明确拒绝且未入任何邮箱时允许重新寻址单次投递；未知结果不重投。

**技术栈：** Go、gRPC-Go、Protocol Buffers；tx 是传输与会话实现，txgame/txgameslg 接业务调用。

**设计：** `docs/superpowers/specs/2026-09-26-async-session-streams.md`

## 全局约束

- 同一客户端 socket 上行顺序由 Agent 顺序写流、Game 顺序读流并立即投递 Actor 邮箱保证；不同 Actor 可并行。
- Agent 是活动 Session 的 Seq/Ack 权威；MsgQ 不逐条确认上行消息，客户端上行 Seq 可跳号，无服务端要求的上行重放。
- MsgQ 保留有界内存下行邮箱；Ack 按 Session 裁剪；gRPC `Send` 成功不等于对端接收或执行业务。
- 每条流仅一个串行写入者和一个串行读取者；对每个已选定的远端实例直连，不经服务名负载均衡已建流。
- 排水拒收、流断开和 MsgQ 内存丢失按已确认边界允许消息丢失；不自动重投，不增加持久化保证。
- 注释与维护说明使用中文；只改本需求涉及的文件；不提交变更。

## 文件与职责

- `tx/modules/rpc/proto/rpc.proto`、`tx/modules/rpc/delivery.go`、`tx/modules/rpc/sender.go`：CallStream 的单向入队项及现有同步调用兼容。
- `tx/internal/actorrpc/direct.go` 及其测试：把 Agent 上行 Enqueue 接到无逐条响应模式，验证目标 Actor 入队顺序。
- `tx/modules/msgq/proto/msgq.proto`、`grpc.go`、`client.go`、`sender.go`、`mailbox.go`、`session.go`：MailboxStream 多 Session 控制/下行分派、仅回报归属拒绝的 PublishStream，以及旧逐条 Rec/单向 FetchStream 路径的收敛。
- `tx/modules/agent/session.go`、`actor_dispatcher.go` 和相关测试：本地推进活动 Session 游标；通过共享 MailboxStream 更新上行游标和下行 Ack并接收下行；不等待 Game 每条入队响应。
- `txgame/server/internal/push`、`txgame/server/internal/app`、`txgame/server/services`：把无状态 Game 的会话/角色下行发布切到客户端发布流。
- `txgameslg/server/internal/push`、`txgameslg/server/internal/app`、`txgameslg/server/services`：把有状态 Game/SLGWorld 的发布切到客户端发布流。
- `tx/cmd/protoc/run.sh`：仅在编辑 `.proto` 后重新生成受影响的 Go 代码；生成文件不手改。

## 评审重点

- 同一 Session 的并发调用不得使 Game 邮箱入队乱序；测试覆盖连续和跳号 Seq。
- MailboxStream 上多 Session 的注册、Ack、会话活动更新和返回下行不得串错 Session。
- 多个业务服务实例并发发布到同一 MsgQ 时，只承诺 MsgQ 实际接收顺序，不伪称跨源全局发送顺序。
- 单流写入错误、对端排水、邮箱满和重连不得触发重复发布；错误不应被包装成已执行业务。
- 关闭 MsgQ/Agent 或取消流时，读写 goroutine 与每 Session 下行等待必须退出，不能遗留阻塞项。

---

### 任务 1：Agent→Game 无逐项回执入队

**文件：** `tx/modules/rpc/proto/rpc.proto`、`tx/modules/rpc/delivery.go`、`tx/modules/rpc/sender.go`、`tx/internal/actorrpc/direct.go` 及相邻测试。

**接口：** 保留现有 Call/Future 和 Accepted-only 请求语义；增加或明确一个不产生逐项响应的流入队模式，供 `txactor.Enqueuer` 使用。服务端在流读取循环中按顺序执行 Actor 邮箱 `Submit`，不为该模式逐项创建等待业务响应的 goroutine。

- [x] 先添加测试：对同一目标连续提交多条上行消息，接收端按写入顺序进入邮箱，异步模式无逐项响应；验证同步 Call 仍收到结果。
- [x] 运行目标测试并确认新断言因当前逐项回执/分派行为失败。
- [x] 最小改动协议、客户端与接收端；运行 `go test ./modules/rpc ./internal/actorrpc ./modules/agent`。

### 任务 2：Agent↔MsgQ 共享 MailboxStream

**文件：** `tx/modules/msgq/proto/msgq.proto`、`grpc.go`、`client.go`、`sender.go`、`mailbox.go`、`session.go`、`tx/modules/agent/session.go` 及相邻测试。

**接口：** 每个 Agent 到每个实际 MsgQ owner 共用一条双向流；应用帧以 `SessionHeader` 区分会话，Agent→MsgQ 传会话注册/结束、Ack 和会话活动 Touch，MsgQ→Agent 推送带序号的下行消息。MsgQ 收到 Ack 后只裁剪对应 Session。Agent 活动游标不再逐条通过 Rec 同步。

- [x] 先添加 MailboxStream 集成测试：同一流交错注册至少两个 Session，分别发布、收取、确认；断流后仍未确认且仍留在 MsgQ 的消息可重发，Ack 不跨会话。
- [x] 运行测试确认当前每 Session `StreamFetch` 协议无法承载共享双向控制流。
- [x] 实现单读者/单写者的多 Session 流与 MsgQ mailbox 唤醒分派；把 Agent 下行读取、上行游标和下行 Ack 更新切换到该流。
- [x] 正式 Agent 使用 MailboxStream，每次上行提交及客户端 Ack 都异步同步游标，不等待逐项回执；旧 RecStream/StreamFetch 不作为兼容路径保留。没有 Agent Rec lease。
- [x] 运行 `go test ./modules/msgq ./modules/agent ./tests`，再运行 `go test -race ./modules/msgq ./modules/agent`。

### 任务 3：业务服务→MsgQ 异步发布流

**文件：** `tx/modules/msgq/proto/msgq.proto`、`grpc.go`、`sender.go`、`client.go` 及 Sender 测试。

**接口：** 增加双向发布流，按具体 MsgQ owner 路由；每个流只有一个写入者和一个读取者，MsgQ 顺序处理流内请求、不返回逐项成功回执。旧角色归属明确拒绝时异步回报完整版本，发送方失效匹配缓存；后续收尾补充拒绝载荷和单次重新寻址。会话发布和角色发布继续使用现有邮箱逻辑与 Seq 分配。

- [x] 先添加 gRPC 测试：一条发布流连续发布 Session 与 Role 消息后，MsgQ 邮箱可读到各自消息且每项不产生应答；流内顺序可观察。
- [x] 运行测试确认当前 Unary PubSession/PubRole 仍逐条等待响应。
- [x] 实现发布流和实例级 Sender 复用；遵循直连实例 owner，仅对旧归属明确拒绝且尚未入邮箱的载荷重新寻址单次投递，其他结果不重试。
- [x] 运行 `go test ./modules/msgq ./tests`。

### 任务 4：切换 txgame 与 txgameslg 业务发布入口

**文件：** 两工程的 `server/internal/push`、`server/internal/app`、Game/SLGWorld 服务注册与发布调用测试。

**接口：** 业务门面切换到新的异步发布接口。角色广播和精确 Session 发布保留目标语义；逐项 `PubResult`/广播计数不再作为“MsgQ 已接收”返回给业务。

- [x] 更新门面测试，验证业务发布接口切换到无逐条回执的 SendSession/SendRole，并保留 Topic 广播统计语义。
- [x] 切换两个工程的 Sender 工厂与下行发布使用点；删除失效的逐条应答统计断言，保留业务 payload 与寻址断言。
- [x] 在 `txgame` 与 `txgameslg` 分别运行 `go test ./internal/push ./services/... ./internal/app`；login listener 测试已单独授权运行。

### 任务 5：协议生成、跨服务验证与基线

**文件：** 受影响的 `.proto` 和生成代码；已有集群 E2E/压测测试及本轮短结果记录。

- [x] 用 `./cmd/protoc/run.sh rpc msgq` 生成 tx 的 RPC 和 MsgQ 代码，并在两个下游工程按其 Go module 依赖方式验证。
- [x] 运行 tx、txgame、txgameslg 的相关包测试；业务入口使用异步 SendSession/SendRole；后续本地八进程回归见 2026-09-28 验收记录。
- [x] 当前方案的云端吞吐和延迟结果统一见 `docs/audits/2026-09-28-seven-host-2050-acceptance.md`；本计划不保留中间候选版本的压测数字。
- [x] 最终对本轮相关变更执行 `ponytail-review`，检查是否保留了多余兼容层、重复流管理或无用配置；未发现有依据可删减的本轮流管理代码；不提交。

## 最终收尾（2026-09-29）

会话契约、接管隔离、本地排水与异常退出验收已收口。历史断流在最终关闭时记 error，不阻断 Game；本次排水新发生的失败仍返回。结果及生产环境保留项见 [feature 收尾记录](../../audits/2026-09-29-feature-closeout.md)。当前未提交、未执行主干合并。
