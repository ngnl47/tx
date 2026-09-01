# MsgQ SessionID binding / Router / Sender 审计

审计日期：2026-09-01  
范围：`modules/msgq` 中的 `SessionStore`、`RedisSessionStore`、`SessionRouter`、`SessionClientRouter`、`Sender`、gRPC dialer，以及它们和进程内 mailbox 的并发边界。  
约束：本次只读审计，未修改模块源码、测试、proto 或生成文件，未提交；唯一写入为本报告。本报告不设计 `PubRole`。

## 结论

本阶段应将“一个精确 mailbox”的所有寻址轴从 `RoleID` 原子性迁移到 `SessionID`：

- Redis 语义 key 应为 `session:{SessionID}`，即 `<prefix>:session:{<sessionID>}`；有旧 key 共存的滚动升级应使用 `<prefix>:session:v2:{<sessionID>}` 或全新 prefix，避免数值恰好相等的旧 RoleID key 被新代码误读。
- `SessionBinding` **必须保留 `RoleID`**。它不再是查找键或分片键，但仍是 Session 的可信业务主体、远程头部组装、错误绑定检测、日志以及后续 Borrow/主会话投影的必要元数据。
- 当前 `Bind` 是 Redis 原子的“GET + 无条件 SET”，**不是 CAS**。新模型必须分成 create-only 与 expected-binding replace；`Refresh`/`Unbind` 继续比对完整 binding。
- `SessionRouter` 应暴露 `ResolveSession(ctx, sessionID)`；`Sender` 应暴露 `PubSession(ctx, sessionID, payload)`。精确发布不接受 RoleID 作为选路键，也不将 ConnectionID 作为消息归属；同一 Session Restore 后的新连接仍应收到该 Session 的下行。
- `Restore=true` 只允许“同 SessionID、同 RoleID、同 owner 内存 mailbox、新 ConnectionID”。不能把同一 Session 自动迁往新 owner，因为 Redis 只有 binding，未确认下行队列仍在旧进程内存；改 binding 并不等于迁移 mailbox。
- 干净登录必须由 Login 给出新 `SessionID`。新 Session 从空队列开始，旧 Session 的消息既不继承也不可见。在本阶段不能因为 RoleID 相同就删除所有旧 Session，否则会直接破坏“同 RoleID 多个独立 Session”与后续 Borrow。旧 Session 由自身 Close/Reap/TTL 清理。
- 现有 `touch` 存在一个必须在迁移时修复的 TOCTOU：旧连接先通过本地检查，Restore 再更新 ConnectionID，旧 `Refresh` 返回 false 后会无条件 `remove(session)`，它能把已恢复的新连接从本地表移除。

## 1. 现状：RoleID 是唯一维度

### Store 与 Redis I/O

`modules/msgq/session.go:46-52` 定义的 Store 是：

```go
Bind(ctx, binding)
Find(ctx, roleID)
Refresh(ctx, binding)
Unbind(ctx, binding)
```

`modules/msgq/session_redis.go` 的所有外部 I/O 都以 RoleID 完成：

| 操作 | 当前客户端选择 | 当前 Redis key | 原子性 |
| --- | --- | --- | --- |
| `Bind` | `resolve(ctx, binding.RoleID)` | `<prefix>:session:{RoleID}` | Lua 内 GET 旧值后无条件 SET PX |
| `Find` | `resolve(ctx, roleID)` | 同上 | 单次 GET |
| `Refresh` | `resolve(ctx, binding.RoleID)` | 同上 | GET 字符串全等时 PEXPIRE |
| `Unbind` | `resolve(ctx, binding.RoleID)` | 同上 | GET 字符串全等时 DEL |

Redis record 只存 `instanceId/sessionId/connectionId`，RoleID 从 key 参数回填（`session_redis.go:195-225`）。这在 Role 主键下勉强自洽，但迁到 Session 主键后必须把 `roleId` 写入 record，否则 `Find(sessionID)` 无法还原和验证主体。

现有 JSON 整串比较可以作为 CAS token，前提是该 key 只由本 adapter 以唯一规范编码写入。迁移后建议 record 始终包含全部四元组：

```json
{"roleId":470001,"sessionId":99,"connectionId":8,"instanceId":"msgq-1"}
```

SessionID 同时在 key 和 value 中存在是有意冗余：解码时可检测错 key/脏数据，条件脚本可以继续比较完整 binding。

### Router 与 Sender

`SessionRouter.Resolve(ctx, roleID, expectedSessionID)` 先 `Find(roleID)`，再把 SessionID 当作可选的防陈旧检查，并在返回前 `Refresh(binding)`（`session.go:77-110`）。`Sender.Pub/JoinTopics/BroadcastTopic` 全部用 RoleID 调它（`sender.go:237-296`）。因此当前模型最多只能表示“一个 Role 当前的一个 owner”。

`Sender.Pub` 远程头部包含 RoleID/SessionID，但故意不包含 ConnectionID（`sender.go:250-255`）。这一点在新语义下应保留：业务消息属于逻辑 Session，不属于某一条物理 Connection。

### 进程内 mailbox

`Server.sessions` 是 `map[int64]*mailboxSession`，但实际以 RoleID 索引（`mailbox.go:33-35, 734-755`）；Init 锁也按 RoleID 分片（`mailbox.go:193`）。`installFresh` 会替换同 Role 旧指针、将旧 session 失活并删除 topic。这是同 Role 多 Session 的直接阻断点，不只是 Redis key 命名问题。

## 2. 建议的 Store 契约与 Lua 线性化点

不建议继续保留当前“无条件覆盖并返回 previous”的 `Bind`。它无法区分全局 SessionID 冲突、合法 Restore 和非预期 owner 篡夺。建议最小契约为：

```go
type SessionStore interface {
    Create(ctx context.Context, binding SessionBinding) (created bool, err error)
    Rebind(ctx context.Context, expected, replacement SessionBinding) (rebound bool, err error)
    Find(ctx context.Context, sessionID int64) (SessionBinding, bool, error)
    Refresh(ctx context.Context, binding SessionBinding) (bool, error)
    Unbind(ctx context.Context, binding SessionBinding) (bool, error)
}
```

若项目必须保留 `Bind` 命名，也应把它改为 create-only，并另增明确的 `Rebind(expected,replacement)`；不应让 nil/zero expected 隐式切换语义。

### Create：SessionID 首次占有

```lua
local created = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
if created then return 1 end
return 0
```

线性化点是 `SET NX PX`。已存在的 SessionID 不得被“干净登录”覆盖，也不得因冲突刷新旧 key TTL。全局唯一 SessionID 是 Login 契约；重用 SessionID 应明确失败，避免 ABA 让迟到的旧连接命中一个“新的同 ID Session”。

### Rebind：Restore 换 ConnectionID

```lua
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
```

`ARGV[1]` 是恢复前的完整 binding，`ARGV[2]` 只改变 ConnectionID（InstanceID、RoleID、SessionID 不变）。线性化点是 GET 全等后的 SET。任何 stale expected 都返回 false，不得把当前 owner 覆盖回去。

### Refresh / Unbind

现有两个 Lua 的基本思路正确：只有 Redis 仍等于完整 binding 时续期或删除。迁移后它们必须：

1. 通过 `binding.SessionID` 选 Redis shard 并组 key；
2. 比较包含 RoleID/SessionID/ConnectionID/InstanceID 的规范值；
3. 未命中时不改 TTL；
4. 对 Redis 返回值不是 `0/1` 继续返回 `ErrSessionStoreResponse`，不静默成功。

### Redis resolver

`SessionRedisResolver.ResolveSessionRedis(context.Context, int64)` 的 int64 语义与 `msgqredis.resolver` 传给 `ScriptClientFor` 的 shard key 都必须改为 SessionID。否则 key 虽然按 SessionID 命名，不同操作却可能根据 RoleID 路由到不同 Redis 端点，CAS 将失去前提。

## 3. Router、Sender 和 Agent 选 owner

### `ResolveSession`

建议直接替换旧 `Resolve(roleID, expectedSessionID)`：

```go
func (r *SessionRouter) ResolveSession(ctx context.Context, sessionID int64) (SessionTarget, error)
```

步骤是：校验正 SessionID -> `store.Find(sessionID)` -> 校验 binding 与 `binding.SessionID == sessionID` -> 精确查 instance -> 校验它仍提供 msgq。旧的 `expectedSessionID == 0` 分支应删除；零值不再表示“当前 Role 的任意 Session”。

`ResolveSession` 不应代替客户端活动刷新 TTL。当前 Router 的 `Refresh` 会使业务发布者为已断开的会话续期；在目录短时陈旧时，还可能持续延长指向死实例的 key。TTL 应由 owner 上经完整 ConnectionID fencing 的 `Rec/Fetch/Confirm/Touch/Init(Restore)` 路径续期。Router 是只读定位器，不是会话活性来源。

### `PubSession`

建议的对外语义：

```go
func (s *Sender) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error)
```

Sender 使用 `ResolveSession`，从 binding 取 RoleID/SessionID 和 owner instance，按 endpoint 复用 gRPC connection，再精确投递。进程内 mailbox 的实际 lookup 也必须是 SessionID，否则 Router 已精确但远程仍会把消息放入同 Role 的另一 Session。

`PubSession` 是 Session 级操作：

- 必须检查 SessionID 正值、本地 mailbox 存活，并且如果传输中携带 RoleID，必须与 mailbox/binding 相等；
- 不应要求 ConnectionID，也不应因 Restore 已更换 ConnectionID 而拒绝仍属于同 Session 的消息；
- 不得在远程返回未知时自动重试，因为入队副作用可能已成功，重试会生成不同 Seq 的重复消息。

Topic owner 锚点如果本阶段一起迁移，也应用显式 owner SessionID，不应在 `JoinTopics/BroadcastTopic` 中暗藏 Role 当前 owner 查找。这仍是单实例 topic 语义，不是 `PubRole`。

### `SessionClientRouter.Acquire`

Agent 在 Init 之前已知 RoleID、SessionID 和 Restore 标志（`modules/agent/session.go:88-105`）。新 owner 选择不能只接受 RoleID。建议用不会混淆 bool 含义的请求结构：

```go
type AcquireSessionRequest struct {
    RoleID    int64
    SessionID int64
    Restore   bool
}
```

- `Restore=true`：按 SessionID Find，校验 RoleID，只连接 binding 指向的已知 owner；owner 不可用时返回失败，不回退到 `NextInstance`，因为新实例没有队列可恢复。
- `Restore=false`：SessionID 必须是新值，从 running msgq 选新 owner；Redis `Create NX` 是最终冲突权威。不再查 RoleID 旧 owner 以决定新 Session 位置。

`FixedSessionClientRouter` 也应校验正 RoleID/SessionID，但继续返回共享本地 client。

## 4. Init / Restore / 干净登录

### 干净登录

新的 non-restore Init 应当：

1. 按 SessionID 获取 init shard lock；
2. 拒绝本地已存在的同 SessionID；
3. 构造空 mailbox，通过 Redis `Create NX PX` 占有 binding；
4. 成功后安装到 `sessions[sessionID]`，UpMsg/DownMsg Seq 均从零状态开始。

同 RoleID 的其他 Session 不得被替换、失活、清 topic 或删 Redis key。因此“干净登录丢弃旧消息”的精确含义是：新 SessionID 不继承任何旧 Session 队列；旧 Session 本身仍是独立对象。要按 Role 主会话政策踢除某个旧普通 Session，需要下一阶段的 RoleID -> primary SessionID 模型，不能在本阶段推断。

### Restore

Restore 应按 SessionID 命中原 mailbox，并在持有该 `session.mu` 时：

1. 校验 active、RoleID/SessionID 相等和 Ack 合法；
2. 以本地当前四元组为 expected，以新 ConnectionID 为 replacement 调 `Rebind`；
3. CAS 成功后更新本地 ConnectionID，重置 DownMsg sent cursor，并应用 Ack；
4. CAS 失败返回 `ErrSessionChanged`，不覆盖 Redis 当前 owner。

在 session 锁内等待一次 Redis CAS 是有意的线性化边界：它只阻塞单 Session，不持有全局 `Server.mu` 或 topic lock，且防止旧 Connection 的 Touch/Close 与 Restore 交错成不可解释的本地/Redis 双状态。

## 5. 旧连接 fencing 与竞态

### 现存 TOCTOU

`mailbox.go:864-904` 的 `touch` 是：

1. lookup(RoleID)；
2. 加锁检查 SessionID/ConnectionID 后解锁；
3. 不持锁调 Redis `Refresh(oldBinding)`；
4. false 时直接 `s.remove(session)`；
5. true 时重新加锁二次检查。

可确定发生的交错：

```text
old Touch:    local check(old) -- unlock ---- Redis Refresh(old) => false -- remove(pointer)
Restore:                           Rebind(new) -- local connectionID = new
```

`remove(pointer)` 只看 map 里是否仍是这个指针，不再比较 ConnectionID，因此会将新 Connection 失活。`Confirm`先调 `touch`，也受同一问题影响。

建议在迁移时选择更易证明的规则：`touch` 持有 `session.mu` 完成“本地 full-header check -> Redis Refresh(exact binding) -> lastActivity”。失败时也只能在锁内确认 session 仍等于请求四元组后移除。不能保留无条件指针删除。

### 操作级别

| 操作 | 寻址 | fencing | 与 Restore 的要求 |
| --- | --- | --- | --- |
| `PubSession` | SessionID | active + SessionID，RoleID 如携带则一致 | 不检 ConnectionID；Restore 前后都是同 mailbox |
| `Rec/Fetch/FetchSome/Confirm/Touch` | SessionID | RoleID + SessionID + ConnectionID | Redis I/O 前后不得让 stale 请求移除新 connection |
| `Join/Leave/Topics` | SessionID | 完整 Connection header | 旧连接不得改新连接 topic |
| `Close` | SessionID | 本地完整 header + Redis exact Unbind | Restore 先线性化则 old Close 返回 Changed；Close 先则 Restore 返回 Offline/Changed |
| `Reap/overflow` | SessionID | 当前完整 binding | 只删目标 Session，绝不影响同 Role 其他 Session |

`Close` 当前已在持有 session 锁时完成 exact `Unbind`，这个方向正确。`Reap` 也在 session 锁内组当前 binding；Redis conditional delete 失败后仍清本地 stale mailbox 是可接受的，但 map delete 必须按 SessionID 和指针双重确认。

## 6. TTL 和 owner 迁移边界

### TTL

现有 constructor 拒绝小于 1ms 的 TTL，并向 Lua 传毫秒，这个校验应保留。每次成功 Create/Rebind/Refresh 重置为完整 TTL；失败 CAS 不得改 TTL。

项目示例配置的 Redis TTL 是 48h，而 mailbox `OfflineTimeout` 默认 180s（`README.md:519-526, 765-772`）。这意味着 Redis TTL 是异常退出的最终兜底，不是精确在线租约。正常断开主要由 Reap + exact Unbind 在分钟级清理；进程崩溃后才依赖长 TTL。因此：

- 只有通过 ConnectionID fencing 的客户端活动应调 `Refresh`；
- Sender/Router 查路不应延长 TTL；
- Redis key 存在不等于 mailbox 可恢复，还必须有目录中的 owner 和 owner 进程内存中的同 Session。

### owner 迁移

本阶段应明确禁止同 SessionID 的自动 owner 迁移：

- Restore 发现 binding owner 不在目录/不可达时返回 `ErrRouteUnavailable` 或 `ErrSessionOffline`，不选新实例再无条件 Bind。
- 需要继续登录时，客户端执行干净登录并获得新 SessionID；这个新 Session 可由 `NextInstance("msgq")` 放在任意 running owner。
- 同 Role 的两个不同 SessionID 可同主或异主，两个 Redis key 和本地 mailbox 完全独立。这是未来 Borrow 所需的空间，不是 owner 冲突。

若未来确实要对同 Session 进行无感 owner 迁移，必须另行设计队列状态转移/持久化、单调 generation fencing 和新旧 owner 切换协议；不能只增一个覆盖 binding 的 Lua。

## 7. gRPC dialer 审计

`GRPCDialer.Dial` 与 `DialSession` 共用 `dial`，显式要求 TLS/insecure credentials，用 `passthrough:///endpoint` 创建 `grpc.ClientConn`，并在 Sender/SessionClientRouter 层按 endpoint 复用连接。该缓存不携带 Role/Session 状态，因而改 SessionID 主键后仍然正确；不需要每 Session 建立 gRPC connection。

需要同步的是窄接口与 RPC 语义：`Connection.Pub`/`grpcConnection.Pub` 应成为精确 `PubSession`，请求只能表达一个正 SessionID 和 payload（若保留 header，RoleID 来自 binding，ConnectionID 为零）。不应在旧 `Pub` 签名下静默把参数从 RoleID 重解释为 SessionID。

当前 `Dial(ctx, ...)` 忽略 ctx。`grpc.NewClient` 本身是 lazy/non-blocking，真正 RPC 使用调用者 ctx，因此这不是网络超时绕过；但建议创建前检查 `ctx.Err()`，避免已取消的 Acquire/Pub 仍创建并缓存一个连接对象。不需要为此改用阻塞 dial。

Sender 并发创建连接的双重检查方向正确：多个 goroutine 可能同时 dial，只缓存一个，其余关闭；`Close` 后创建完成的 connection 也会被关闭。精确发布不得对“RPC 是否已入队”未知的错误自动重试。

## 8. 必须增加的竞态与行为测试

### Store / Redis 单元测试

1. `Find/Create/Rebind/Refresh/Unbind` 每次 resolver 入参都是 SessionID，key 是 `...:session[:v2]:{SessionID}`。
2. JSON 包含全部四元组；key SessionID 与 record SessionID 不同、RoleID/ConnectionID 非正、InstanceID 非法都返回 `ErrSessionStoreResponse`。
3. Create 第一次成功，同 SessionID 第二次失败且不改值/不刷 TTL。
4. Rebind 只允许 exact expected -> replacement；stale expected、错 Role、错 owner、错 ConnectionID 都失败。
5. old Connection Refresh/Unbind 在 Rebind 后返回 false，不改新 binding 的 TTL；new Connection 成功。
6. Redis Eval 未知返回值必须是 error，不视为 false 或成功。
7. 小于 1ms TTL 继续在任何 I/O 前拒绝。

### mailbox 确定性竞态测试

1. 同 RoleID 的 Session A/B 同时 Init，`SessionCount==2`；`PubSession(A)` 只能被 A Fetch，B 不可见；关闭/驱逐/Reap A 不影响 B。
2. Session A 有未确认队列，Restore 同 SessionID/新 ConnectionID 后 Seq、Ack 和未确认队列保留。
3. 同 RoleID 以新 SessionID 干净登录，新 Session 队列为空且 Seq 重置，旧 Session 队列不会泄漏给它。
4. 用可阻塞 fake Store 复现“old Touch 完成首次本地检查 -> Restore 更换 Connection -> old Refresh false”，断言新 session 仍 active、仍可 Touch/Fetch/Confirm。该测试是现存 TOCTOU 的关键回归。
5. old Confirm/Touch/Close 在 Restore 后均返回 `ErrSessionChanged`，不释放新 connection 队列、不刷新 TTL、不删新 binding。
6. Close vs Restore 和 Reap vs Restore 以 barrier 控制两种线性化顺序；最终状态只能是“Restore 成功且旧 Close 失败”或“Close 成功且 Restore 失败”，不能 Redis 新 binding 存在而本地新 mailbox 被删。
7. 两个并发 Restore 使用同一 expected binding 时 Redis 只有一个 CAS 成功；在同 owner 内由 session 锁串行读取最新 expected 时可按顺序都成功，但只有最后 ConnectionID 能后续操作。
8. `PubSession` vs Close 在 session 锁上线性化：成功表示在 Close 前入队，否则返回 Offline/Changed；不得在 inactive mailbox 返回成功。

上述测试除常规运行外必须使用 `go test -race`。不应依赖 sleep 碰运气；fake Store 应暴露进入/继续 channel 以确定交错。

### Router / Sender / gRPC 测试

1. `ResolveSession(99)` 只调 `Find(99)`，校验 `binding.SessionID==99`；不存在、损坏 binding、错 instance/非 msgq 都在 dial 前失败。
2. 同 Role 的两个 Session 可解析到不同 owner，`PubSession` 各只投递一次且 endpoint connection 可正常复用。
3. gRPC 往返传输精确 SessionID/payload，服务端不以 RoleID 再查“当前会话”；无效/NotFound/Changed/Overflow 错误保持可 `errors.Is`。
4. Pub RPC 返回空响应或非正 Seq 继续返回 `ErrInvalidMailboxResponse`。
5. Sender 并发首次 dial 只缓存一条 endpoint connection，多余 connection 关闭；Sender Close 与 dial 并发不泄漏。
6. 已取消 ctx 不创建/缓存新 gRPC client；不为 Pub 未知结果自动重试。
7. Agent `Restore=true` 在 owner 不可用时不回退新 MsgQ；non-restore 以新 SessionID 可选任意 running MsgQ。

## 9. 真实 Redis 回归

迁移后不能只用 fake Eval 验证脚本字符串，必须在 `TX_REDIS_ENDPOINT` 指向的真实 Redis 上覆盖：

1. 同 RoleID 的 Session 99/100 产生两个独立 key，Find 各返回正确 Role/owner/Connection；Unbind 99 不影响 100。
2. Create NX 冲突不改 value 与 PTTL；Rebind 成功刷新 PTTL，stale Rebind/Refresh/Unbind 不改 PTTL。
3. 多 goroutine 用同 expected 竞争不同 ConnectionID，只一个成功；最终 value 可解码，且只有获胜 binding 能 Refresh/Unbind。
4. TTL 实际过期后 Find=false，无永久 key；毫秒舍入契约与 constructor 校验一致。
5. 通过 routed store 实际记录的 shard key 是 SessionID，而不只检查最终 Redis key 字符串。
6. 用真实 Redis + 两个 mailbox Server 覆盖“同 Role 异 Session 可同/异 owner”、Restore 仅在原 owner 成功、stale owner 不能删新 binding。

测试 key 必须使用每次唯一 prefix，cleanup 显式列出已知的 SessionID key，不用 `KEYS`/`SCAN` 清共享 Redis。

## 10. 本次审计实际验证

已运行：

```text
go test -race ./modules/msgq ./modules/msgq/msgqredis ./tests \
  -run 'Test(MsgQ|SessionClient|GRPCSession|RedisSession|Sender|Mailbox)' -count=1
```

结果：三个 package 均通过。这只证明当前 RoleID 单 mailbox 基线在已有用例下通过，没有覆盖本报告列出的多 Session 和 TOCTOU 交错。

已用 `TX_REDIS_ENDPOINT=127.0.0.1:6379` 运行现有真实 Redis 用例：

- `TestMsgQRedisSessionStoreIntegration`：通过；证明现有 RoleID key 上的无条件 Bind、exact Refresh/Unbind 和 PTTL 可在真 Redis 执行。
- `TestMsgQServerRedisIntegration`：失败，`reap = 0, <nil>`。测试使用默认 `OfflineTimeout=180s`，却仅把 Reap 时钟推进 2 分钟（`tests/msgq_mailbox_test.go:314-340`），因而它的预期 `removed=1` 与生产默认值矛盾。这不是 Redis Lua 失败，但说明该门控用例平时并未形成稳定的真实 Redis 回归；迁移实现前应先让它显式配置短 OfflineTimeout 或把 now 推进超过 180s。

## 11. 建议的实施原子单元

本迁移不能分成“先改 key，以后再改 Router/mailbox”的可独立上线步骤。一个可交付的原子单元至少同时包含：

1. SessionStore API + SessionID Redis resolver/key/record + Create/Rebind/Refresh/Unbind Lua；
2. Server sessions/init lock/lookup/remove 转 SessionID，并修复 stale touch 无条件 remove；
3. `ResolveSession` + `PubSession` + gRPC/protobuf/本地 client 窄接口；
4. Agent Acquire 区分 fresh/restore，Restore 不自动迁 owner；
5. 所有 memory fake 改为 SessionID key，并加入同 Role 多 Session、stale connection 竞态与真 Redis CAS 回归。

只改其中任意一层都会保留隐式 Role 唯一性，或让本地状态与 Redis owner 产生新的撤销/误删窗口。
