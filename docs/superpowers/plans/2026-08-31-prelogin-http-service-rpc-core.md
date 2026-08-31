# 登录前 HTTP 与同步 Service RPC 核心实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Agent 增加项目普通 HTTP Handler 注入能力，并为普通 Service RPC 增加同步不透明结果。

**Architecture:** 根路径继续由现有 WebSocket Handler 处理，其他路径交给项目 HTTP Handler并进入服务 admission。普通 RPC 在现有 DeliveryService 上增加 Call，沿用 discovery 路由与 endpoint 连接缓存，结果保持编解码无关且不自动重试。

**Tech Stack:** Go 1.26、`net/http`、gRPC、Protocol Buffers、lifecycle admission、现有 discovery Directory。

**Spec:** `docs/superpowers/specs/2026-08-31-prelogin-http-service-rpc-core-design.md`

## Global Constraints

- 不提交 Git，由维护者统一审核提交。
- 新增或修改的注释、GoDoc 和维护说明使用中文。
- `Result.Payload` 保持不透明，不引入 BON、JSON 或业务错误码依赖。
- `Sender.Call` 只解析一次路由、只调用一次连接，不自动重试。
- 保留 `WebSocketListen` 与 `WebSocketOrigins` 命名；本阶段不支持纯 HTTP Agent。
- 普通 HTTP Handler 必须以请求为单位进入服务 admission。
- 不修改 `txgame`、`txgameslg`、`txclient`、`txclientslg`、`txgen` 和 `txgameslgworld`。

---

### Task 1: 增加 Call protobuf 与服务端接收契约

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/rpc/proto/rpc.proto`
- Regenerate: `/Users/a47/workspace/tx/modules/rpc/pb/rpc.pb.go`
- Regenerate: `/Users/a47/workspace/tx/modules/rpc/pb/rpc_grpc.pb.go`
- Modify: `/Users/a47/workspace/tx/modules/rpc/delivery.go`
- Create: `/Users/a47/workspace/tx/modules/rpc/call.go`
- Modify: `/Users/a47/workspace/tx/tests/rpc_proto_contract_test.go`
- Modify: `/Users/a47/workspace/tx/tests/rpc_delivery_test.go`
- Modify: `/Users/a47/workspace/tx/modules/rpc/host_test.go`

**Interfaces:**
- Produces: `rpc.Result`、`rpc.CallReceiver`、`rpc.CallReceiverFunc`、`rpc.NewAdmissionCallReceiver`、三参数 `rpc.RegisterDeliveryService`。

- [x] **Step 1: Write failing protobuf and receiver tests**

在 protobuf 契约测试中构造字面值 `Result{message_type:"Login_ServerListResp", command_id:10153, payload:...}` 并往返序列化；在 DeliveryService 测试中调用尚不存在的 `Call`，断言请求原样到达 `CallReceiver` 且结果原样返回。

- [x] **Step 2: Run tests and verify RED**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./tests ./modules/rpc`

Expected: FAIL，`rpcpb.Result`、`CallReceiver` 或生成的 `Call` 方法不存在。

- [x] **Step 3: Implement the minimal server contract**

增加：

```go
type Result struct {
	MessageType string
	CommandID   uint64
	Payload     []byte
}

type CallReceiver interface {
	Call(context.Context, Delivery) (Result, error)
}

type CallReceiverFunc func(context.Context, Delivery) (Result, error)
```

`RegisterDeliveryService(registrar, receiver, callReceiver)` 至少要求一个 receiver；对应方法未装配时返回 `Unimplemented`。使用仓库脚本生成 rpc protobuf：

Run: `PROTOC=$(command -v protoc) ./cmd/protoc/run.sh rpc`

- [x] **Step 4: Run tests and verify GREEN**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./tests ./modules/rpc`

Expected: PASS。

### Task 2: 增加 Sender.Call 且禁止隐式重试

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/rpc/sender.go`
- Modify: `/Users/a47/workspace/tx/modules/rpc/sender_test.go`
- Modify: `/Users/a47/workspace/tx/tests/rpc_delivery_test.go`
- Modify: `/Users/a47/workspace/tx/modules/rpc/host_benchmark_test.go`

**Interfaces:**
- Consumes: Task 1 的 `Result` 与生成 `DeliveryServiceClient.Call`。
- Produces: `Connection.Call(context.Context, Delivery) (Result, error)`；`Sender.Call(context.Context, Delivery) (Result, error)`。

- [x] **Step 1: Write failing sender tests**

新增 fake connection 记录 Call 次数与结果。分别断言：

```go
result, err := sender.Call(ctx, Delivery{
	Route: ServiceRoute("game"),
	MessageType: "Login_ServerList",
})
```

返回字面值结果；连续两次调用只 dial 一次；底层第一次返回错误时 Call 次数严格为一。

- [x] **Step 2: Run tests and verify RED**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./modules/rpc ./tests`

Expected: FAIL，`Connection.Call` 或 `Sender.Call` 不存在。

- [x] **Step 3: Implement minimal sender behavior**

`Sender.Call` 复用 `Resolve` 与 `connection`，只执行：

```go
return connection.Call(ctx, delivery)
```

`grpcConnection.Call` 负责 `Delivery`/`Result` protobuf 转换和结果校验，不增加循环、重试或默认 timeout。

- [x] **Step 4: Run tests and verify GREEN**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./modules/rpc ./tests`

Expected: PASS。

### Task 3: 复用 Agent HTTP Server 承载项目 Handler

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/agent/service/service.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/service/service_test.go`

**Interfaces:**
- Produces: `agentservice.Options.HTTPHandler http.Handler`。

- [x] **Step 1: Write failing routing and admission tests**

测试希望入口 Handler 具备以下行为：

- `/` 调用 WebSocket Handler。
- `/api/v1/login/server-list` 调用项目 Handler。
- 未配置项目 Handler 的非根路径返回 `404`。
- admission 排水后的项目请求返回 `503`。
- 已接纳且阻塞的项目请求使 `Ingress.Pending()==1`，完成后可以 drain。

- [x] **Step 2: Run tests and verify RED**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./modules/agent/service`

Expected: FAIL，`Options.HTTPHandler` 或入口复用函数不存在。

- [x] **Step 3: Implement minimal HTTP composition**

在 `Options` 增加 `HTTPHandler http.Handler`。创建内部组合 Handler：根路径直接转交 WebSocket；非根路径缺少项目 Handler 时 `http.NotFound`；存在项目 Handler 时调用 `Admission.Enter()`，失败返回 `503`，成功时 defer 完成函数后执行项目 Handler。

- [x] **Step 4: Run tests and verify GREEN**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./modules/agent/service ./tests -run 'Agent|HTTP|WebSocket'`

Expected: PASS。

### Task 4: 更新稳定说明并完成全量回归

**Files:**
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`

**Interfaces:**
- Consumes: Tasks 1–3 的最终公开 API 与生命周期语义。

- [x] **Step 1: Update current documentation**

记录 `DeliveryService.Call`、无自动重试、Agent 根路径 WebSocket/非根路径项目 HTTP Handler，以及下一阶段 `txgame + txclient` 业务接入边界。

- [x] **Step 2: Run format and generated-code checks**

Run: `gofmt -w`，只格式化本轮手写 Go 文件。

Run: `git diff --check`

Expected: 无输出且 exit 0。

- [x] **Step 3: Run full verification**

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test -count=1 ./...`

Run: `GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go vet ./...`

Expected: 两个命令均 exit 0。
