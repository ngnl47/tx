# Agent 接入模块设计

## 目标

`modules/agent` 是 tx 游戏服务生态中的可选玩家接入模块。它提供默认 TCP 和 WebSocket 接入、连接到逻辑会话的映射、Seq/Ack、重连、下行循环，以及与独立 `modules/msgq` 的协作。

它不包含账号体系、具体命令、GD、Store、事务或业务 Actor Kind。业务项目只负责认证回调、命令分发和模块拓扑装配。

## 模块边界

```text
客户端 TCP / WebSocket
        |
        v
modules/agent
  - 统一帧编解码与连接生命周期
  - Agent Session 与上、下行循环
  - 调用 msgq 会话服务
  - 调用项目 Dispatcher
        |                    |
        v                    v
modules/msgq            项目业务
  - owner binding        - Cmd 解码
  - Seq/Ack              - Actor / gRPC 投递
  - sndQueue             - CommandID 幂等
  - Topic
```

`agent` 依赖 `msgq` 的会话服务接口，不持有 Redis owner binding 或下行队列。`msgq` 可与 `agent` 同进程部署，也可作为独立、多实例服务部署：前者使用本地适配器，后者使用已有的 gRPC `MailboxService`。

Agent 在认证后通过会话客户端路由器获取 msgq：先查询角色既有 owner binding，绑定仍指向 running msgq 时无论新旧 SessionID 都回到该实例，由 `Init` 原子替换旧会话；没有有效 owner 时从服务目录轮转选择一个 running msgq，再由该实例建立新绑定。endpoint 不写入项目 yml。

## 传输与会话

`modules/agent` 同时提供：

- TCP Server：长度前缀二进制帧；
- WebSocket Server：binary message。

两种 Server 都将帧交给同一 Agent Session。业务 BON body 不因传输改变；统一信封承载握手、入站 Seq、下行 Ack、Session 身份与 BON payload。

一条普通 TCP 或 WebSocket 连接对应一个逻辑 Session。未来旁路服务可通过 `Connection` 适配器把一条物理 server-to-server 连接拆成多个逻辑 Session；此能力不进入首版实现。

## 认证与分发

Agent 定义通用 `IdentityResolver`：项目将不透明握手数据解析为 `RoleID`、`SessionID` 与可选业务身份信息。它可以校验 token，也可以消费上游已经认证的可信凭据；Agent 不解析 JWT、账号或角色选择规则。

认证成功后 Agent 调用 `msgq.Init` 建立或恢复会话。每条上行包先调用 `msgq.Rec` 校验 Seq 并携带客户端 Ack；校验成功即消费入站 Seq，然后调用项目注册的 `Dispatcher`。该语义与参考实现一致：进程在业务分发后崩溃时允许丢失该条上行，关键业务使用 `CommandID` 做幂等保护。

项目 Dispatcher 根据业务命令选择 `actor.Runtime`、`modules/rpc` 或本地处理；Agent 不预设路由规则。

## 下行与失败

任意业务节点通过 `msgq.Sender.Pub` 投递下行 BON body。Agent 下行循环调用 `Fetch` 或 `FetchSome`，写入客户端连接；客户端随后在下一条上行 Ack 或显式确认中推进 `msgq.Confirm`。

未确认下行只保存在 msgq 实例内存。msgq 实例崩溃时允许丢失，下次重连由业务幂等与重同步恢复。外部 Redis、gRPC、TCP 或 WebSocket 异常均向 Agent 返回错误、关闭或断开当前连接；不得 panic，也不得伪造成功。

## txgame 装配

txgame 新增可选 `agent` 模块。项目 yml 只声明静态监听参数和传输开关；实例 ID、endpoint 等部署身份仍由环境注入。模块启动时注入：

- 已装配的本地或远端 msgq `SessionClient`；
- 项目 `IdentityResolver`；
- 项目 `Dispatcher`；
- 全局 `WaitGroupContext` 生命周期。

`agent` 与 `msgq` 可单独选择、共同选择或分别部署。未装配所需 msgq 服务时，Agent 启动必须明确失败，不允许半可用运行。

## 验证

- tx：TCP 与 WebSocket 分别覆盖认证、Init、Rec、业务分发、下行 Fetch/Ack、重连和错误关闭；本地 Server 与 gRPC SessionClient 都覆盖。
- txgame：Host 使用 `-module=agent,msgq` 启动，覆盖 TCP/WebSocket 真实回环；可用本机 Redis 验证 owner binding 与跨模块下行。
- 所有测试使用 Go 1.26.5；真实 Redis 集成使用 `127.0.0.1:6379`。

## 非目标

- 不实现旁路 mock、多路复用连接或离线挂机。
- 不实现业务账号、JWT、Cmd 解析、角色加载或 Actor Kind。
- 不改变客户端协议为 protobuf；BON 保持业务 payload。
- 不将 msgq 内嵌到 agent，也不把 Agent 变为业务 Actor 的中心中继。
