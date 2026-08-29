# Agent 接入模块实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 tx 提供可独立部署或同进程装配的 TCP/WebSocket 玩家接入模块，并在 txgame 中完成模块化真实回环。

**Architecture:** `modules/agent` 只管理传输、逻辑 Session、Seq/Ack 和与 `modules/msgq` 的协作；认证与业务分发由项目注入。`modules/msgq` 继续是 owner binding 和下行队列的唯一所有者，Agent 通过本地或 gRPC 会话客户端访问它；新连接优先复用有效 owner，缺失时由服务目录选择 running msgq。

**Tech Stack:** Go 1.26、`net`、`net/http`、`encoding/binary`、`github.com/coder/websocket`、gRPC、etcd ServiceDirectory、Redis SessionStore、BON payload。

## Global Constraints

- tx runtime 不读取项目 yml、环境变量或 txgame 文件。
- tx 与 txgen 源码、常规文档不得出现无关的外部来源名称。
- 业务 payload 始终为 BON bytes；不生成 protobuf 版 GD 或客户端 DTO。
- TCP 和 WebSocket 使用同一 Agent Session 内核；一条普通物理连接对应一个逻辑 Session。
- `Rec` 校验成功即消费上行 Seq；关键业务使用 `CommandID` 自行幂等。
- msgq 独立于 agent；Agent 不保存 Redis binding 或下行 payload。
- 外部服务、网络和认证失败必须返回错误或关闭连接，禁止 panic 与静默成功。
- 不实现旁路 mock、物理连接多路复用、账号体系、JWT、具体 Cmd 或业务 Actor Kind。
- 使用 `/Users/a47/sdk/go1.26.5/bin/go`，不执行 git commit；每个可提交阶段仅提供中文 Angular Commit Message。

---

## 文件结构

- `tx/modules/msgq/client.go`：Agent 使用的本地/gRPC 会话客户端与按 owner、服务目录选择实例的路由器。
- `tx/modules/agent/types.go`：认证、业务分发、逻辑身份和传输抽象。
- `tx/modules/agent/frame.go`：统一二进制信封和最大帧校验。
- `tx/modules/agent/session.go`：传输无关的认证、Init、Rec、Dispatcher、Fetch/Confirm 和关闭状态机。
- `tx/modules/agent/tcp.go`：长度前缀 TCP Server 与 Transport 适配。
- `tx/modules/agent/websocket.go`：WebSocket HTTP Handler 与 Transport 适配。
- `tx/tests/agent_*_test.go`：会话内核、TCP、WebSocket、本地/远端 msgq 回归。
- `txgame/server/internal/config/model.go`：`runtime.agent` 严格 yml 配置与校验。
- `txgame/server/internal/modules/agent/module.go`：Host 模块装配、监听器生命周期和 local/remote msgq 选择。
- `txgame/server/internal/agent/handlers.go`：项目认证与 Dispatcher 注册点，默认缺失时启动失败。
- `txgame/server/internal/modules/agent/module_test.go`：Host 配置和 TCP/WebSocket/Redis 真实回环。
- `tx/docs/IMPLEMENTATION_PLAN.md`、`tx/docs/usage.md`、`txgame/README.md`：部署、协议、错误语义和项目装配示例。

### Task 1: 抽出 msgq 会话客户端与动态实例选择

**Files:**
- Create: `tx/modules/msgq/client.go`
- Modify: `tx/modules/msgq/grpc.go`
- Modify: `tx/modules/msgq/session.go`
- Test: `tx/tests/msgq_client_test.go`

**Interfaces:**
- Produces:

```go
type SessionClient interface {
    Init(context.Context, InitRequest) (InitResult, error)
    Rec(context.Context, RecRequest) (RecResult, error)
    FetchSome(context.Context, FetchSomeRequest) (FetchSomeResult, error)
    Confirm(context.Context, ConfirmRequest) error
    CloseSession(context.Context, SessionHeader) error
}

type SessionClientDialer interface {
    DialSession(context.Context, discovery.Instance) (SessionClient, error)
}

func NewSessionClientRouter(directory *discovery.ServiceDirectory, store SessionStore, dialer SessionClientDialer) (*SessionClientRouter, error)
func (r *SessionClientRouter) Acquire(ctx context.Context, roleID int64) (SessionClient, error)
```

- Consumes: `Server.Init/Rec/FetchSome/Confirm/Close`、`SessionStore.Find`、`ServiceDirectory.ResolveID/ResolveService`、`MailboxService`。

- [x] **Step 1: 写失败测试，锁定本地客户端与远端路由语义**

在 `tx/tests/msgq_client_test.go` 创建内存 `SessionStore`、两个 running `msgq` 实例与可记录 dial 的假 `SessionClientDialer`。覆盖：

```go
func TestSessionClientRouterPrefersExistingOwner(t *testing.T) {
    // 角色 470001 已绑定到 msgq-b；即使此次认证得到新的 SessionID，Acquire 仍只 Dial msgq-b。
}

func TestSessionClientRouterSelectsRunningServiceWhenNoOwner(t *testing.T) {
    // 无 binding 时按 ServiceDirectory 的 running msgq 轮转选择。
}

func TestLocalSessionClientDelegatesMailboxCalls(t *testing.T) {
    // Init -> Rec -> Pub -> FetchSome -> Confirm 走同一个 Server。
}
```

- [x] **Step 2: 运行测试，确认缺少接口而失败**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run 'Test(SessionClientRouter|LocalSessionClient)' -count=1
```

Expected: FAIL，提示 `SessionClient`、`SessionClientRouter` 或本地客户端尚未定义。

- [x] **Step 3: 实现最小客户端边界**

实现 `localSessionClient` 逐一委托 `Server`；为现有 gRPC 连接补齐 `Init`、`Rec`、`FetchSome`、`Confirm`、`CloseSession` 并统一使用 `mailboxClientError`。`SessionClientRouter.Acquire` 按以下固定顺序选择：

```go
binding, found, err := store.Find(ctx, roleID)
if err != nil { return nil, err }
if found {
    if instance, ok := directory.ResolveID(binding.InstanceID); ok {
        return dialer.DialSession(ctx, instance)
    }
}
instance, ok := directory.ResolveService("msgq")
if !ok { return nil, ErrRouteUnavailable }
return dialer.DialSession(ctx, instance)
```

binding 指向停止或消失实例时允许重新选择 running msgq；不在 Router 内写 binding，绑定必须由目标 `Server.Init` 原子创建。

- [x] **Step 4: 运行 msgq 测试**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run 'TestMsgQ|TestSessionClient' -count=1
```

Expected: PASS。

- [x] **Step 5: 提供提交信息**

```text
feat(msgq): 抽出会话客户端与动态实例路由

提供本地和 gRPC 会话客户端；Agent 可优先复用角色既有 owner，并在无有效绑定时从服务目录选择运行中的 msgq 实例。
```

### Task 2: 实现传输无关的 Agent Session 内核

**Files:**
- Create: `tx/modules/agent/types.go`
- Create: `tx/modules/agent/frame.go`
- Create: `tx/modules/agent/session.go`
- Test: `tx/tests/agent_session_test.go`

**Interfaces:**
- Consumes: `msgq.SessionClientRouter`、`msgq.SessionHeader`、Task 1 的 `SessionClient`。
- Produces:

```go
type AuthRequest struct { Token []byte; Restore bool; Ack int64 }
type Identity struct { RoleID int64; SessionID int64 }
type IdentityResolver interface {
    ResolveIdentity(context.Context, AuthRequest) (Identity, error)
}
type Dispatcher interface {
    Dispatch(context.Context, DispatchRequest) error
}
type IdentityResolverFunc func(context.Context, AuthRequest) (Identity, error)
func (f IdentityResolverFunc) ResolveIdentity(ctx context.Context, request AuthRequest) (Identity, error)
type DispatchFunc func(context.Context, DispatchRequest) error
func (f DispatchFunc) Dispatch(ctx context.Context, request DispatchRequest) error
type DispatchRequest struct {
    Identity Identity
    Seq      int64
    Payload  []byte
}
type Transport interface {
    Read(context.Context) (Frame, error)
    Write(context.Context, Frame) error
    Close() error
}
func NewServer(options ServerOptions) (*Server, error)
func (s *Server) Serve(ctx context.Context, transport Transport) error
```

- [x] **Step 1: 写失败测试，固定认证、上行和下行生命周期**

用内存 `Transport`、确定性 connection ID 生成器和 Task 1 的本地 SessionClient 测试：

```go
func TestAgentSessionAuthenticatesThenDispatchesAcceptedSequence(t *testing.T) {
    // Auth(token) -> Rec(seq=1, ack=0) -> Dispatcher 收到同一 BON payload。
}

func TestAgentSessionRejectsBusinessFrameBeforeHandshake(t *testing.T) {
    // 首帧不是握手时，不调用 Dispatcher，Serve 返回协议错误。
}

func TestAgentSessionFetchesOutboundAndConfirmsAckOnNextInbound(t *testing.T) {
    // msgq.Pub 后写出 outbound Frame(seq=1)；下一条上行 ack=1 释放队列。
}

func TestAgentSessionDispatchFailureDoesNotRollbackAcceptedSequence(t *testing.T) {
    // Dispatcher 返回错误后 receivedSeq 仍为 1，与既定 at-most-once 语义一致。
}
```

- [x] **Step 2: 运行测试，确认 Agent 尚不存在**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^TestAgentSession' -count=1
```

Expected: FAIL，提示 `github.com/ngnl47/tx/modules/agent` 不存在。

- [x] **Step 3: 实现统一帧与 Session 状态机**

`Frame` 仅包含 `Kind`、`Seq`、`Ack`、`Payload`；`KindHandshake` 的 payload 是不透明 token，`KindBusiness` 与 `KindOutbound` 的 payload 是 BON bytes。定义版本化二进制编码，固定拒绝未知 Kind、负 Seq/Ack、超过 `MaxFrameBytes` 的帧。

`Serve` 必须按下列顺序运行；`readHandshake`、`readBusinessFrame`、`fetchOutboundUntilClosed` 都是 `session.go` 内仅供状态机使用的私有函数：

```go
handshake := mustReadHandshake(transport)
identity := identityResolver.ResolveIdentity(ctx, handshake)
client := router.Acquire(ctx, identity.RoleID)
client.Init(ctx, msgq.InitRequest{Header: header, Restore: handshake.Restore, Ack: handshake.Ack})
go fetchOutboundUntilClosed(ctx, client, header, transport)
for businessFrame := range inboundFrames {
    client.Rec(ctx, msgq.RecRequest{Header: header, Seq: businessFrame.Seq, Ack: businessFrame.Ack})
    dispatcher.Dispatch(ctx, DispatchRequest{Identity: identity, Seq: businessFrame.Seq, Payload: cloneBytes(businessFrame.Payload)})
}
```

连接 ID 默认用 `crypto/rand` 生成正 int64；测试通过显式生成器保持确定性。连接异常只结束当前 Session，不伪造确认；正常显式注销才调用 `CloseSession`，以便断线后依赖 `OfflineTimeout` 与 `Init(Restore=true)` 恢复。

- [x] **Step 4: 运行 Agent 内核测试与竞态检查**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test -race ./tests -run '^TestAgentSession' -count=1
```

Expected: PASS，无 data race。

- [x] **Step 5: 提供提交信息**

```text
feat(agent): 增加传输无关的玩家会话内核

Agent 通过认证回调建立逻辑会话，按 Seq/Ack 调用 msgq 并分发 BON 上行；下行从 msgq 获取后写回当前连接，业务失败不回滚已接受的序号。
```

### Task 3: 提供 TCP 与 WebSocket 默认 Server

**Files:**
- Modify: `tx/go.mod`
- Create: `tx/modules/agent/tcp.go`
- Create: `tx/modules/agent/websocket.go`
- Test: `tx/tests/agent_tcp_test.go`
- Test: `tx/tests/agent_websocket_test.go`

**Interfaces:**
- Consumes: Task 2 的 `Server.Serve(ctx, Transport)` 与 `Frame` 编解码。
- Produces:

```go
func ListenTCP(address string, server *Server, maxFrameBytes int) (*TCPServer, error)
func (s *TCPServer) Serve(ctx context.Context) error
func (s *TCPServer) Close() error

func NewWebSocketHandler(server *Server, maxFrameBytes int) (http.Handler, error)
```

- [x] **Step 1: 写 TCP 与 WebSocket 的失败回环测试**

TCP 用 `127.0.0.1:0` 建立真实 listener，WebSocket 用 `httptest.NewServer` 和 `github.com/coder/websocket.Dial`。两套测试都发送相同握手和业务帧，并断言 Dispatcher payload 与 msgq 下行帧：

```go
func TestTCPAgentRoundTrip(t *testing.T) {
    // 客户端写握手和 seq=1 BON；服务分发；Pub 后客户端读到 seq=1 下行。
}

func TestWebSocketAgentRoundTrip(t *testing.T) {
    // WebSocket binary message 走与 TCP 相同的 Session 语义。
}

func TestTransportRejectsOversizedFrame(t *testing.T) {
    // TCP 长度前缀和 WebSocket binary payload 都超过 MaxFrameBytes 时关闭当前连接。
}
```

- [x] **Step 2: 运行测试，确认传输 Server 未定义**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^Test(TCPAgent|WebSocketAgent|TransportRejects)' -count=1
```

Expected: FAIL，提示 `ListenTCP`、WebSocket Handler 或依赖不存在。

- [x] **Step 3: 添加成熟 WebSocket 依赖并实现适配器**

执行：

```bash
cd /Users/a47/workspace/tx && /Users/a47/sdk/go1.26.5/bin/go get github.com/coder/websocket@v1.8.15
```

TCP 帧使用 4 字节 big-endian payload length 加 Task 2 的版本化 Frame bytes；读取前检查长度 `0 < n <= MaxFrameBytes`。WebSocket 只接受 binary message，用 `github.com/coder/websocket` 的 context-aware `Accept/Reader/Write`；不得把 JSON helper 用于 BON payload。TCP accept 和 HTTP handler 都为每条连接调用一次 `Server.Serve`，并将任务纳入调用方 context。

- [x] **Step 4: 运行传输测试和全量 tx 测试**

Run:

```bash
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

Expected: PASS。

- [x] **Step 5: 提供提交信息**

```text
feat(agent): 提供 TCP 与 WebSocket 默认接入

TCP 长度帧和 WebSocket binary message 复用同一 Agent Session；统一限制帧大小并通过成熟 WebSocket 库处理握手、关闭和上下文取消。
```

### Task 4: 在 txgame 装配 Agent 与可选本地/远端 msgq

**Files:**
- Create: `txgame/server/internal/agent/handlers.go`
- Create: `txgame/server/internal/modules/agent/module.go`
- Modify: `txgame/server/internal/config/model.go`
- Modify: `txgame/server/internal/config/loader_test.go`
- Modify: `txgame/server/cmd/server/main.go`
- Modify: `txgame/server/config/local/app.yml`
- Modify: `txgame/server/internal/modules/msgq/module.go`
- Test: `txgame/server/internal/modules/agent/module_test.go`

**Interfaces:**
- Consumes: Task 1 `msgq.SessionClientRouter`，Task 2 `agent.IdentityResolver/Dispatcher`，Task 3 TCP/WebSocket Server，`host.App`、`WaitGroupContext`。
- Produces:

```go
type AgentSection struct {
    Enabled       bool   `yaml:"enabled"`
    TCPListen     string `yaml:"tcpListen"`
    WebSocketListen string `yaml:"webSocketListen"`
    MaxFrameBytes int    `yaml:"maxFrameBytes"`
    FetchBatch    int    `yaml:"fetchBatch"`
}

type Handlers struct {
    IdentityResolver txagent.IdentityResolver
    Dispatcher    txagent.Dispatcher
}
func RegisterHandlers(handlers Handlers) error
func RegisteredHandlers() (Handlers, error)
```

- [x] **Step 1: 写失败配置和 Host 集成测试**

在 `loader_test.go` 加入严格 yml 校验，在 `module_test.go` 用内存或本机 Redis SessionStore 和真实 TCP/WebSocket 回环覆盖：

```go
func TestLoadRejectsInvalidEnabledAgent(t *testing.T) {
    // enabled 时至少一个 listen 地址、正 maxFrameBytes 和正 fetchBatch 必须存在。
}

func TestAgentModuleRequiresRegisteredBusinessHandlers(t *testing.T) {
    // 未注册 IdentityResolver/Dispatcher 时 Start 返回明确错误。
}

func TestAgentModuleUsesLocalMsgQWhenBothModulesRun(t *testing.T) {
    // host 共同选择 agent,msgq，Agent 使用本地 SessionClient，不发起额外 gRPC。
}

func TestAgentModuleUsesDirectoryForRemoteMsgQ(t *testing.T) {
    // 仅选择 agent，目录中存在 running msgq，认证后能经 gRPC Init/Rec。
}
```

- [x] **Step 2: 运行 txgame 定向测试，确认配置和模块不存在**

Run:

```bash
cd /Users/a47/workspace/txgame/server && env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./internal/config ./internal/modules/agent -count=1
```

Expected: FAIL，提示 `AgentSection` 或 `agentmodule` 不存在。

- [x] **Step 3: 实现项目装配**

在 `runtime.agent` 启用时严格校验监听地址、帧大小和批量 Fetch 数。`internal/agent` 只保存项目注册的认证与分发回调，不实现账号或 Cmd；缺失回调时 `agent` 模块启动失败。

`agentmodule` 选择会话客户端来源：同一 Host 已启动 `msgqmodule.Module` 时取其公开本地 SessionClient；否则要求 Discovery 目录与远端 SessionClientDialer 可用，通过 Task 1 Router 动态选择 running msgq。TCP listener 和 WebSocket `http.Server` 均由 `Lifecycle.StartTask` 托管；Stop 先停止监听，禁止接收新连接，再等待根 context 取消的连接循环退出。

主入口仅在 `runtime.agent.enabled` 为真时注册 Agent Module，使 `-module=agent,msgq` 与 `-module=agent` 均可由 Host 正确选择；服务发现中公布的模块列表继续由实际选择结果生成。

- [x] **Step 4: 运行 txgame 集成与真实 Redis 回归**

Run:

```bash
cd /Users/a47/workspace/txgame/server && env TX_REDIS_ADDR=127.0.0.1:6379 GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

Expected: PASS；Redis 不可用时相关测试应明确 skip，非 skip 的装配失败应返回错误而不是 panic。

- [x] **Step 5: 提供提交信息**

```text
feat(txgame): 装配可选 Agent 服务模块

项目可按模块选择启动 TCP/WebSocket Agent，并注入业务认证和分发回调；同进程复用本地 msgq，独立部署时通过服务目录路由远端 msgq。
```

### Task 5: 完善使用说明与全链路验证

**Files:**
- Modify: `tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `tx/docs/usage.md`
- Modify: `txgame/README.md`
- Test: `tx/tests/agent_*_test.go`
- Test: `txgame/server/internal/modules/agent/module_test.go`

**Interfaces:**
- Consumes: Tasks 1-4 的全部公开 API。
- Produces: 可直接复制的 TCP/WebSocket Agent、认证回调、Dispatcher、同进程/分进程 yml 与模块启动示例。

- [x] **Step 1: 写文档示例断言或编译示例**

为 usage 中的最小业务回调保留可编译片段：

```go
handlers := appagent.Handlers{
    IdentityResolver: txagent.IdentityResolverFunc(func(ctx context.Context, req txagent.AuthRequest) (txagent.Identity, error) {
        return txagent.Identity{RoleID: 470001, SessionID: 99}, nil
    }),
    Dispatcher: txagent.DispatchFunc(func(ctx context.Context, req txagent.DispatchRequest) error {
        // 项目在这里解析 BON payload 并投递业务 Actor 或调用本地服务。
        return nil
    }),
}
```

并在 txgame 集成测试中使用等价实现，防止文档与 API 漂移。

- [x] **Step 2: 补充文档**

`tx/docs/usage.md` 说明：TCP 长度帧、WebSocket binary message、握手 token、Seq/Ack、`Rec` 即消费、下行确认、断线重连、msgq 同进程/跨进程、错误关闭与 `CommandID` 边界。`txgame/README.md` 给出 `runtime.agent` yml、环境变量、`-module=agent,msgq` 与分离部署示例。实施计划删除已完成项，只保留多路旁路适配器为后续扩展点。

- [x] **Step 3: 运行所有工程验证**

Run:

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txgen && env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txgame/server && env TX_REDIS_ADDR=127.0.0.1:6379 GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
git -C /Users/a47/workspace/tx diff --check
git -C /Users/a47/workspace/txgame diff --check
```

Expected: 全部 PASS，且无 whitespace error。

- [x] **Step 4: 提供提交信息**

```text
docs(agent): 补充玩家接入与模块部署说明

说明 TCP/WebSocket 统一会话、认证回调、Seq/Ack、msgq 部署拓扑和断线恢复边界，并用 txgame 回环测试约束示例行为。
```
