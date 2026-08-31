# 登录前 HTTP 与同步 Service RPC 核心设计

## 目标与阶段边界

本阶段只补齐 `tx` 框架的两个缺口：

1. Agent 已有 HTTP Server 在保留根路径 WebSocket Upgrade 的同时，可以接入项目提供的普通 HTTP Handler。
2. 普通 Service RPC 在现有单向 `Deliver` 之外，支持请求方同步取得不透明业务结果。

完成后，`txgame` 可以在下一阶段实现：

```text
HTTP JSON
  -> Agent 项目 Handler
  -> rpc.Sender.Call(ServiceRoute("game"), Delivery)
  -> 任一 running game 实例
  -> CallReceiver
  -> Result
  -> HTTP JSON
```

本阶段不实现 `Login_ServerList` Handler、HTTP JSON/BON 转换或 `txclient` 调用。

## 同步 Service RPC

### 传输契约

现有 `Delivery` 保持不变。`modules/rpc/proto/rpc.proto` 增加：

```proto
message Result {
  string message_type = 1;
  uint64 command_id = 2;
  bytes payload = 3;
}

service DeliveryService {
  rpc Deliver(Delivery) returns (DeliveryAck);
  rpc Call(Delivery) returns (Result);
}
```

`Result.Payload` 与 `Delivery.Payload` 一样保持不透明；RPC 层不感知 BON、JSON、业务错误码或生成协议。

Go 侧对应值类型：

```go
type Result struct {
	MessageType string
	CommandID   uint64
	Payload     []byte
}
```

有效结果必须具有非空且无首尾空格的 `MessageType`。`CommandID` 可以为零，`Payload` 可以为空，以允许无正文响应。

### 服务端接口

单向与同步入口保持两个独立接口：

```go
type Receiver interface {
	Deliver(context.Context, Delivery) error
}

type CallReceiver interface {
	Call(context.Context, Delivery) (Result, error)
}
```

`ReceiverFunc` 保持现有语义；新增 `CallReceiverFunc` 和 `NewAdmissionCallReceiver`。两种 admission wrapper 都以请求为单位调用 `lifecycle.Admission.Enter()`，排水前已接纳的调用可以完成，排水后新调用被拒绝。

注册入口调整为：

```go
func RegisterDeliveryService(
	registrar grpc.ServiceRegistrar,
	receiver Receiver,
	callReceiver CallReceiver,
) error
```

至少提供一个 receiver。未配置的 RPC 方法返回 gRPC `Unimplemented`；不通过虚构空实现隐藏装配错误。

### 客户端接口

`Connection` 增加：

```go
Call(context.Context, Delivery) (Result, error)
```

`Sender` 增加同签名 `Call`。它复用既有流程：校验 `Delivery`、解析一次 Route、复用 endpoint 连接、执行一次远端调用。`ServiceRoute` 继续使用 discovery 当前的 running 实例轮转策略。

`Sender.Call` 不自动重试。调用超时后远端是否已经产生业务副作用不确定，重试策略只能由明确具备幂等语义的上层决定。

RPC 层不强制 context 必须携带 deadline，也不设置隐藏默认超时；下一阶段的 HTTP 项目适配层负责建立明确超时。

### 错误映射

- 请求或结果传输契约无效：`InvalidArgument` 或本地 `ErrInvalidResult`。
- 未装配对应 receiver：`Unimplemented`。
- `lifecycle.ErrDraining`、`lifecycle.ErrStopping`：`Unavailable`。
- 业务 receiver 返回的其他 error：`Internal`。
- discovery、dial、context 与 gRPC 错误保持原错误链。

本阶段不在 `Result` 增加业务错误码字段；业务失败是否使用生成响应、统一错误响应或 HTTP 状态，由项目协议层决定。

## Agent 普通 HTTP Handler

`agentservice.Options` 增加：

```go
HTTPHandler http.Handler
```

当前 `WebSocketListen` 暂不重命名。它仍然表示浏览器 Agent 入口的监听地址，只是在同一个 `http.Server` 上允许附加项目 HTTP Handler。`WebSocketOrigins` 仍只约束 WebSocket Upgrade，不扩展成普通 HTTP CORS 配置。

入口分派规则固定为：

- `URL.Path == "/"`：交给现有 WebSocket Handler。
- 其他路径且配置了 `HTTPHandler`：先进入服务 admission，再交给项目 Handler。
- 其他路径且未配置 `HTTPHandler`：返回 `404`。
- admission 已排水或停止：返回 `503`，不进入项目 Handler。

项目 Handler 自己管理 `/api/...` 路由、HTTP method、CORS、认证、JSON 限制和错误正文。框架不为单个服务器列表接口制造通用 Web API 抽象。

本阶段不支持“只有普通 HTTP、没有 WebSocket”的 Agent。未配置 `WebSocketListen` 时仍不创建 HTTP Server。

## 生命周期

- 普通 HTTP 请求以一次 Handler 调用为 admission 工作单元。
- `Drain` 关闭 TCP 与 HTTP listener，阻止新连接。
- 排水前已进入项目 Handler 的请求保留 pending，项目返回后调用完成函数。
- `Stop` 沿用现有 `http.Server.Close`、listener 关闭和任务 context 取消顺序。
- 空闲 WebSocket 连接不计为 pending；只有已经进入业务帧或普通 HTTP Handler 的工作参与排水等待。

## 非目标

- 不修改 Actor/RPC 路由。
- 不实现自动重试、幂等键、业务错误码或默认超时。
- 不实现通用 HTTP Router、CORS 或认证中间件。
- 不修改 `txgame`、`txgameslg`、`txclient`、`txclientslg` 或 `txgen`。
- 不实现服务器列表业务闭环。

## 验收

- gRPC 同一 endpoint 上 `Deliver` 与 `Call` 均可工作。
- `Sender.Call(ServiceRoute(...))` 取得不透明 `Result`，连接继续按 endpoint 复用。
- 底层 `Call` 返回错误时只调用一次，不自动重试。
- Call admission 在排水期间拒绝新调用并等待已接纳调用完成。
- Agent 根路径仍进入 WebSocket Handler，其他路径进入项目 HTTP Handler。
- 普通 HTTP 请求参与 admission；排水后返回 `503`。
- `go test -count=1 ./...` 和 `go vet ./...` 通过。
