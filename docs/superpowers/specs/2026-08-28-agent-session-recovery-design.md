# Agent 会话恢复与方向术语设计

## 目标

为 TCP 与 WebSocket 客户端建立一致的断线恢复契约。网络中断不得因为客户端已经分配、但 Agent 尚未接收的上行序号而形成永久缺口；重复重放必须由 msgq 的连续序号校验幂等吸收。

## 固定术语

协议方向以客户端和服务端拓扑为固定参照：

- `UpMsg`：客户端发送到 Agent，再进入业务服务。
- `DownMsg`：业务服务经 msgq 和 Agent 发送到客户端。
- `sent/received`：仅描述当前观察者看到的发送和接收事件，不作为协议方向名称。
- 调试界面使用 `UP/DOWN`，分别作为 `UpMsg/DownMsg` 的紧凑显示。

Frame 名称固定为 `FrameHandshake`、`FrameUpMsg`、`FrameDownMsg` 和 `FrameAck`。枚举数值和线协议布局保持不变。

## 双向未确认队列

客户端持有 `UpMsgQueue`，保存已经分配 Seq、但尚未被服务端 Ack 的完整只读 UpMsg 帧。服务端 msgq 持有 `downMsgQueue`，保存已经分配 Seq、但尚未被客户端 Ack 的只读 DownMsg 载荷。

```text
txclient UpMsgQueue
    UpMsg Seq=N
        -> Agent/msgq 接受
        <- DownMsg.Ack=N 或 Ack.Ack=N
    删除 <= N 的上行帧

msgq downMsgQueue
    DownMsg Seq=M
        -> txclient 处理完成
        <- UpMsg.Ack=M 或 Ack.Ack=M
    删除 <= M 的下行载荷
```

Agent 的连接 writer 只负责串行写帧，不是重放队列。Actor mailbox 只负责业务消息串行执行，也不参与传输重放。

## 新建与恢复

新登录使用新的 `SessionID`、`restore=false` 和从 1 开始的 UpMsg Seq。msgq 必须创建新的本机会话，即使 RoleID 或 SessionID 与旧记录相同，也不能复用旧序号和未确认载荷。

网络重连沿用原 `SessionID`，使用 `restore=true`，并在握手 Ack 中携带客户端已经处理完成的最后一个 DownMsg Seq。msgq 只允许恢复仍然存在且 SessionID 相同的会话；找不到旧会话时必须明确失败，不能创建空会话后等待下一条 UpMsg 暴露序号错误。

恢复成功后，Agent 立即用 Ack 告知服务端已经连续接受的最后一个 UpMsg Seq。txclient 删除 `UpMsgQueue` 中已经确认的前缀，并按原 Seq 重发剩余帧。重复帧由 msgq 返回当前 UpMsg Seq，不再次进入业务 Dispatcher。

进程已经退出或会话超过离线保留时间时，内存中的未确认队列不再可恢复。客户端必须重新认证取得新 `SessionID`，清空 Seq/Ack，并通过业务协议重新同步状态。

## 客户端门面

`AgentClient.connect(token)` 只建立新会话；已经产生过 UpMsg 的实例不得再次按新会话连接。`AgentClient.reconnect(token)` 只恢复同一个逻辑会话，并自动重放未确认 UpMsg。调用方不直接拼装 restore 标记或维护重放游标。

Web 调试页第一次连接创建新会话。同一页面、同一 endpoint 和 token 断开后再次连接时自动调用恢复入口；endpoint 或 token 改变时创建新的 `AgentSession` 与 `AgentClient`。

## 正确性边界

- UpMsg 在进入传输前加入客户端队列；发送失败不能丢失帧。
- 只有合法的服务端 Ack 才能释放 UpMsg 队列前缀。
- DownMsg 只有在客户端业务处理成功后才能推进客户端 Ack。
- 重放顺序严格保持原 Seq，不重新编码、不重新分配 Seq。
- UpMsg 与 DownMsg 的 Payload 在进入队列后只读，网络层不得 append 或修改。
- 协议违约、恢复不存在的会话和 Ack 越界继续作为连接级错误处理。

## 验证

- 单元测试覆盖 UpMsg 入队、确认释放、发送前断网、重连重放和重复 Ack。
- msgq 测试覆盖非恢复连接强制新建、恢复缺失失败、恢复保留上下行游标。
- Agent 测试覆盖恢复后立即返回 UpMsg Ack，并保证重复 UpMsg 不重复分发。
- txgame 使用真实 TCP 和 WebSocket 入口模拟断开，验证同一 SessionID 能继续 Ping/Pong 且 Seq/Ack 连续。
