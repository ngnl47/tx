# MsgQ 精确 `SessionID` 邮箱审计（只读）

审计日期：2026-09-01

范围：`modules/msgq` 的进程内 `Server`、会话绑定、Sender/Client、protobuf/gRPC，以及直接 MsgQ 回归测试。未修改模块代码、测试或已生成 protobuf 文件。

## 结论

推荐把本阶段严格做成一次**破坏性但小范围的 SessionID 定向迁移**：

1. 进程内 `Server.sessions`、初始化锁、`SessionStore`/Redis key、`SessionRouter` 和 `SessionClientRouter` 全部以正 `SessionID` 精确寻址。
2. 删除旧的按角色当前会话投递 `Pub`/`PubRequest`/gRPC `Pub`；新增唯一的 `PubSession(ctx, sessionID, payload)`，以及 protobuf RPC `PubSession(PubSessionRequest)`。
3. **本阶段不实现 `PubRole`，也不保留“RoleID 为零表示当前会话”的兼容分支。** `RoleID -> 主 SessionID` 是实施计划明确列在下一阶段的能力。
4. `SessionHeader` 暂时保留 `RoleID`，作为认证后的业务身份/日志关联元数据；它不再决定邮箱、owner 或 Sender 路由。`SessionBinding` 也保留它为该会话的主体元数据。控制类请求可继续校验其为正并与已安装会话的元数据一致；这不是主键依赖。`PubSession` 不接收 header，也不检查 `ConnectionID`。

这与 `docs/IMPLEMENTATION_PLAN.md:24-26` 的顺序一致：先精确投递到一个全局唯一的 Session，再在后续阶段增加 Role 到“唯一主 Session”的专用路由。当前的按 RoleID 投递会让未来借用只读会话与正常会话发生覆盖/误投递，不能作为兼容 API 留下。

## 建议的公共形状

Go 进程内 API：

```go
func (s *Server) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error)
```

没有必要再增加一个只包一层的进程内 `PubSessionRequest`。它应复用当前 `PubResult{Seq}`、队列上限、payload 只读所有权和 `ErrDownMsgOverflow` 语义。

protobuf/gRPC 使用显式类型，避免把 Session 定向藏在可选 header 中：

```proto
message PubSessionRequest {
  int64 session_id = 1;
  bytes payload = 2;
}

message PubSessionResponse { int64 seq = 1; }

rpc PubSession(PubSessionRequest) returns (PubSessionResponse);
```

删除 `PubRequest`、`PubResponse` 和 `rpc Pub`，而不是把旧 `Pub` 偷偷改为按 `SessionID` 解释。后者会使旧调用在源代码仍能编译的情况下改变投递对象，是最危险的兼容路径。此变更要求所有调用方随服务端原子升级；旧 gRPC method path 将明确得到 `Unimplemented`，比误投递安全。

`SessionClient`、`Connection`、`grpcConnection` 和 `localSessionClient` 对应改为：

```go
PubSession(context.Context, int64, []byte) (PubResult, error)
```

`Sender.Pub` 改名为 `Sender.PubSession(ctx, sessionID, payload)`；`SessionRouter.Resolve` 改为只接收一个正 `sessionID`（可命名为 `ResolveSession` 以消除旧 Role 语义）。`SessionClientRouter.Acquire` 与 `FixedSessionClientRouter.Acquire` 也改接收 `sessionID`，使 Agent 在 `Init` 前能恢复到原来的 owner。

## 当前 RoleID 依赖清单

| 层 | 当前位置/符号 | 当前依赖 | 本阶段迁移 |
| --- | --- | --- | --- |
| 进程内邮箱 | `modules/msgq/mailbox.go`: `Server.sessions`, `initLocks`, `installFresh`, `lookup`, `removeLocked`, `touch`, `Init`, `Close` | `map[RoleID]*mailboxSession`；恢复、读写和关闭都先按 Role 查 | 改为 `map[SessionID]*mailboxSession`；锁分片和所有 lookup/delete 均用 SessionID。`mailboxSession.roleID` 仅保留身份元数据。 |
| 旧投递 | `mailbox.go`: `Pub(PubRequest)` | `PubRequest.Header.RoleID` 定位当前会话；SessionID 只是可选防陈旧校验 | 删除，替换为 `PubSession(sessionID,payload)` 的精确 lookup；不接受零值 session。 |
| 绑定接口 | `modules/msgq/session.go`: `SessionStore.Find`, `SessionRouter.Resolve`, `SessionBinding.Validate` | `Find(ctx, roleID)` 与 `binding.RoleID == roleID` | `Find(ctx, sessionID)`，验证 `binding.SessionID == sessionID`。保留 `RoleID` 为 binding 主体元数据，正值/一致性校验不参与查找。 |
| Redis | `modules/msgq/session_redis.go`: `SessionRedisResolver`, `RedisSessionStore.client/key/Find/decodeSessionBinding` | resolver、hash tag、key 和 JSON 解码输入都取 RoleID；record 不存 role | 全部以 SessionID 路由/哈希/取 key。record 新增 `roleId`，并验证 key 的 SessionID 等于 record.SessionID。推荐使用新命名空间如 `:session:v2:{<sessionID>}`，避免新 SessionID 恰与旧 RoleID 冲突而误读旧 JSON。 |
| Redis 适配 | `modules/msgq/msgqredis/adapter.go: ResolveSessionRedis` | 传给 storage route 的是 RoleID | 参数和 route key 改为 SessionID。 |
| Agent 选 owner | `modules/msgq/client.go`: 两个 `Acquire`；`modules/agent/session.go:100` | Agent 用 RoleID 查已有 owner | 改用 `identity.SessionID`；新会话无 binding 时仍选择一个运行中的 msgq，恢复时命中同一 Session 的 owner。 |
| Sender | `modules/msgq/sender.go`: `Sender.Pub`, `JoinTopics`, `BroadcastTopic` | `Resolve(roleID,0)`；`Pub` 构造 Role header | `PubSession` 通过 SessionID Resolve 后调用远端 `PubSession`。topic 的 owner 锚点也改为 `sessionID`，不创造 Role 路由。 |
| gRPC | `modules/msgq/grpc.go`, `modules/msgq/proto/msgq.proto` | `mailboxService.Pub` 解读 header；`SessionHeader` 含 RoleID | 移除该 endpoint，增加 `mailboxService.PubSession`，只映射 session_id/payload。Header 继续服务 Init/Rec/Fetch/Confirm/Touch/Topic/Close。 |

此外，`modules/msgq/service/service_test.go`、`modules/agent/service/service_test.go`、`tests/msgq_mailbox_test.go` 中的内存 `SessionStore` fake 都以 `map[RoleID]` 保存 binding，必须同批改为 `map[SessionID]`。这类 fake 若未改，会掩盖多 Session 同 Role 的核心回归。

## 生命周期与 fencing 影响

### Init / Restore

`Init` 的锁和本地查找按 `Header.SessionID` 进行。`Restore=true` 必须只命中相同 SessionID 的现存 mailbox，随后像现在一样：条件 `Bind` 写入完整 binding、保存旧 `ConnectionID`、更新为新 ConnectionID、重置 sent cursor、确认 `Ack`。同 Role 的另一个 Session 不得被覆盖、失活或清空。

`Restore=false` 对同一个 SessionID 仍明确新建/覆盖其本地状态（按现有“非恢复必建新状态”契约）；全局唯一 SessionID 是前提。对同 Role 但不同 SessionID，两个 mailbox 可共存——这正是尚未引入“角色唯一主会话”前应有的状态。

### ConnectionID

ConnectionID 仍是连接级 fencing，而不是邮箱主键：`Rec`、`Fetch`、`FetchSome`、`Confirm`、`Touch`、`JoinTopics`、`LeaveTopics`、`Topics`、`Close` 都先按 SessionID 找 mailbox，再检查当前 SessionID/ConnectionID（以及建议保留的 RoleID 元数据一致性）。旧物理连接不能续期、确认、管理 topic 或关闭恢复后的新连接。

`PubSession` 故意不携带 ConnectionID：它投递给逻辑 Session；同一 Session 恢复到新连接后，未确认队列和后续下行都应继续属于它。对不存在/已被 Reap、Close 或 overflow 驱逐的 Session 返回 `ErrSessionOffline`；零/负 SessionID 返回 `ErrInvalidHeader`（也可以单列新错误，但没有必要）。

### Close / Reap / overflow eviction

`Close`、`Reap` 和 `evict` 构造的 conditional `SessionBinding` 仍包含 `(RoleID, SessionID, ConnectionID, InstanceID)`，但 Redis 的定位 key 改为 SessionID。它们从 `sessions[sessionID]` 删除，绝不影响同 Role 的其他 Session。
Reap 的两种时限（最后客户端活动或最早未确认下行）与 payload/Seq 语义无须变化。风险点是先前 Role keyed store 的 `Unbind` 会在两个同 Role 的 Session 间相互删除；该接口和所有 fake 必须一起改为 Session keyed 后，条件完整 binding 才真正发挥 fencing 作用。

### Topic

`Server.topics` 已是 `map[string]map[*mailboxSession]struct{}`，不以 RoleID 建索引，内部可以保持不变。`Join/Leave/Topics` 的 session lookup 改为 SessionID 后，同 Role 的两个 Session 可独立订阅并可同时收到本实例 topic 广播；关闭、Reap 和 overflow 仍用指针从 topic 集合移除。

跨进程 `Sender.JoinTopics` 的定位参数必须由 `roleID` 改为 `sessionID`；`Sender.BroadcastTopic` 的“挑选一个承载实例”锚点也改为 SessionID（例如命名 `ownerSessionID`）。它仍只是该实例内的广播，不是 `PubRole`，也不应建立 Role 到主 Session 的隐含目录。

## SessionHeader 是否保留 RoleID

推荐**保留**，但降级为业务身份元数据，原因是：

- `protocol.SessionPrincipal` 与 Agent 的认证/分发日志当前以 RoleID 表达已认证主体；保留 header 避免在本阶段把可信身份模型与 mailbox 主键迁移捆绑。
- Init 后保存的 `mailboxSession.roleID`、`SessionBinding.RoleID` 和日志仍能关联业务主体；在后续 Phase 2 构建专门的 Role 主会话目录时也不需重新定义身份载体。
- 仅移除它在查找、map key、Redis hash key、Sender resolve key 和 `PubSession` 输入中的用途。控制请求可检查 header RoleID 与会话记录相同，防止错误的可信内部调用混淆日志/身份。

因此不要在本阶段从 `SessionHeader` 或其 protobuf 字段中删除 `role_id`。删除它会额外破坏 Init/Agent/日志和每个控制 RPC，却不能减少本阶段的任何 Session 定向语义；真正必须删除的是会把 RoleID 当投递选择器的 `Pub` API。后续 Phase 2 再将 RoleID 到唯一主 Session 的读模型做成显式、受 AccessMode 约束的路由。

## 精确文件与测试迁移

### 必改实现/协议

- `modules/msgq/mailbox.go`：Server map/key/locks/lookup/remove/touch/Init/Close/Reap/evict；删除 `PubRequest` 和 `Server.Pub`，新增 `Server.PubSession`；更新注释、日志和控制请求的 lookup。
- `modules/msgq/session.go`：SessionStore Find 签名、SessionRouter 的精确 resolve 语义和参数名；保留 binding Role 元数据但不把它当索引。
- `modules/msgq/session_redis.go`：resolver 形状、Redis key/hash tag、JSON record、encode/decode 与 Find/conditional 操作的 SessionID 一致性。
- `modules/msgq/msgqredis/adapter.go`：把 storage 分片路由参数从 RoleID 改为 SessionID。
- `modules/msgq/client.go`：SessionClient 的 `PubSession`；本地适配；两个 Acquire 的参数/Find 迁移。
- `modules/msgq/sender.go`：Connection、grpcConnection、Sender 的 `PubSession`，以及 topic 入口的 SessionID owner 解析。
- `modules/msgq/grpc.go`：服务端 PubSession 转换、客户端错误路径保持使用现有 response 可信度校验。
- `modules/msgq/proto/msgq.proto`：新 PubSession message/RPC，移除旧 Pub RPC/message；执行 `PROTOC="$(command -v protoc)" ./cmd/protoc/run.sh msgq` 后提交生成的 `modules/msgq/pb/msgq.pb.go` 与 `modules/msgq/pb/msgq_grpc.pb.go`，不要手改生成文件。
- `modules/agent/session.go`：`Acquire(identity.SessionID)`；当前 Agent 内部的 dispatcher response 改用 `client.PubSession(ctx, header.SessionID, payload)`。

### 必改/新增测试

- `modules/msgq/mailbox_test.go`：把每个旧 `PubRequest{Header:{RoleID:...}}` 改为 PubSession；新增“同 Role、两个不同 Session 都存活且各自精确收取”；新增 restore/close/reap/overflow 只影响目标 Session；更新直接 `lookup` 断言为 SessionID。
- `tests/msgq_mailbox_test.go`：与上同，并把 Redis 集成 key 断言改为新的 SessionID key。它是基础端到端 lifecycle 覆盖。
- `tests/msgq_session_test.go`：Router 以 SessionID resolve；Redis fake JSON 包含 roleId；断言 hash tag、resolver 参数及条件 Bind/Refresh/Unbind 都为 SessionID；新增同 Role 不同 Session 的两个 binding 不互相覆盖。
- `tests/msgq_client_test.go`：SessionClientRouter 的 existing owner/reuse/no-owner 改以 SessionID Acquire；local 和 gRPC SessionClient 都调用 PubSession；fake 实现新接口。
- `tests/msgq_grpc_test.go`：protobuf 客户端直接调用 `PubSession({session_id:99,payload})`；覆盖错误 SessionID 的 NotFound/InvalidArgument 映射。
- `tests/msgq_sender_test.go` 与 `modules/msgq/sender_test.go`：`Sender.PubSession` 按 SessionID resolve并复用 endpoint；topic anchor 改为 SessionID；断言不再构造 Role 定向发布。
- `modules/msgq/grpc_client_test.go`：所有嵌入 `MailboxServiceClient` 的 fake 添加 `PubSession`，替换 Pub 成功响应/无效响应测试；验证 grpcConnection 发出的 request 只有 session_id 与 payload。
- `modules/msgq/service/service_test.go`、`modules/agent/service/service_test.go`：memory store 改 `map[SessionID]`。
- `tests/agent_session_test.go`、`tests/agent_transport_test.go`：直接外部下行替换为 `mailbox.PubSession(..., 99, ...)`，并验证 Agent 内部 dispatcher response 在新的 SessionClient 方法下仍写回同一会话。

还应同步更新 `README.md` 的 MsgQ binding、进程内 Pub、gRPC Pub、Sender、Topic 和 Agent 路由示例（现有内容从约 364 行开始明确宣称 RoleID key/`Sender.Pub(roleID)`），因为 README 被实施计划定义为稳定公共 API 说明。

## 验收最小集与风险

建议按以下顺序验证：

1. 单元：`go test ./modules/msgq ./tests -run 'Test(MsgQ|GRPCConnection|Sender|AgentSession|AgentTransport)' -count=1`（实际实现时可拆分运行，以免模糊失败来源）。
2. protobuf：使用项目要求的 protoc 36.0 重新生成 msgq，再确认 generated diff 只反映 proto 修改。
3. Redis 环境可用时：设置 `TX_REDIS_ENDPOINT`，运行 `TestMsgQRedisSessionStoreIntegration` 与 `TestMsgQServerRedisIntegration`，重点确认两个相同 RoleID、不同 SessionID 的 key 同时存在且条件解绑互不影响。

主要风险：

- **跨版本 API 破坏**：删除 gRPC `Pub` 后老客户端必然失败。不要把旧 Pub 静默改义；部署要原子升级 MsgQ 调用方，或先安排明确的版本切换窗口。
- **Redis 旧 key 碰撞/残留**：旧 `:session:{roleID}` 与新按 SessionID 的同名 key 不能复用；使用版本化 namespace，旧 key 交由原 TTL 过期或按明确运维计划删除。
- **只改 Server 不改 Store/Router**：会造成本机多个 mailbox 可以共存但跨进程 owner 覆盖，最终仍误投递。Store、Router、Sender、Agent Acquire 是一个原子迁移单元。
- **ConnectionID 被误加入 PubSession**：会使恢复后的业务消息被当成旧连接拒绝；PubSession 只能 fence Session existence，不 fence 某个物理连接。
- **主题锚点遗留 RoleID**：这会变相引入未设计的 Role 路由，并在借用会话到来时选择不确定实例。改为显式 SessionID anchor；全局/跨实例广播依旧由业务层分发。
- **现有测试盲点**：大部分数据使用“一 Role 对一 Session”，不能证明迁移正确。必须加入“同 Role 两个 Session”场景，这比批量替换 API 名更关键。

## 未执行项

本次为只读审计。未修改实现、测试、proto、生成文件或 README；也未运行测试。唯一写入是本审计报告。
