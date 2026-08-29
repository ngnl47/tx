# 多进程真实集群闭环设计

## 背景

`tx` 同时支持两条并行的项目装配路线：

- `txgame` 使用 Bee 组装无状态服务集群，`txclient` 是其独立 TypeScript 协议参考实现和 Agent 调试客户端。
- `txgameslg` 使用 Ant 组装有状态 SLG/MMO 服务集群，`txclientslg` 是其独立 TypeScript 协议参考实现和 Agent 调试客户端。

现有项目级集成测试已经覆盖 Redis、Mongo、Fusion、Procedure、CacheManager、WAL 和 checkpoint 的大量真实后端行为，但服务通常聚合在同一测试进程内。它们不能证明独立进程间的服务发现、gRPC、Actor Cluster、MsgQ、进程重启和优雅关闭能够形成完整闭环。

本设计定义 P0 多进程真实集群验收的拓扑、工程边界、外部服务隔离、场景矩阵和通过条件。两套集群是并行能力，不互相依赖；本地开发允许依次启动和验收，不要求同时常驻。

## 目标

1. 使用真实 etcd、Redis 和 MongoDB，分别启动两套由独立 OS 进程组成的示例集群。
2. 使用与服务端生成输入一一对应的客户端完成真实 TCP、WebSocket、BON、Frame、Seq/Ack 和断线恢复验收。
3. 证明无状态服务的远程 Actor/RPC、MsgQ、服务发现和实例重启行为。
4. 证明有状态服务的固定逻辑槽位、RoleActor/WorldActor、Fusion、CacheManager、WAL、checkpoint、generation fence 和恢复行为。
5. 证明 SIGTERM 触发的优雅关闭会停止入口、等待已受理工作，并在依赖仍可用时完成最终持久化。
6. 提供可重复运行、失败时保留证据、成功时精确清理的项目级编排和验收入口。

## 非目标

- 不把 `modules/msgq` 提升为可靠业务消息队列，也不承诺 MsgQ 进程崩溃后未确认下行无损恢复。
- 不实现固定逻辑槽位的异常自动接管、网络分区仲裁或自动 placement 迁移。
- 不要求无状态和有状态两套集群同时运行。
- 不把业务服务名、固定槽位名或示例数据写入 `tx` 通用运行时。
- 不在 `txclient` 与 `txclientslg` 之间建立源码包、符号链接或共享生成目录。
- 不为所有 Redis key 增加项目级前缀。模块已有的稳定 key 结构保持不变，隔离依靠 Redis logical database。

## 六工程边界

| 工程 | P0 职责 |
| --- | --- |
| `tx` | 提供通用运行时、Actor、RPC、Discovery、MsgQ、生命周期、存储、Fusion 和 WAL 能力；只在真实验收暴露通用缺口时修改。 |
| `txgen` | 保证两套 Go 输入分别生成服务端 Go 产物和对应客户端 TypeScript 产物；生成器本身不参与集群编排。 |
| `txgame` | 持有无状态集群配置、独立服务进程、依赖检查、进程编排、存储断言和关闭/重启场景。 |
| `txclient` | 持有无状态协议端到端（End-to-End，E2E）验收，验证 TCP/WebSocket、BON、Frame、Seq/Ack、业务错误和断线恢复。 |
| `txgameslg` | 持有有状态集群配置、固定槽位 bootstrap、独立服务进程、状态准备、存储断言、恢复和关闭/重启场景。 |
| `txclientslg` | 持有有状态协议端到端验收，验证 Ping/Pong、角色命令、直接 WorldActor 命令、跨 Role/World Actor 命令、Seq/Ack 和断线恢复。 |

两条生成链固定为：

```text
txgame/gen     -> txgame/server/gen     + txclient/src/gen
txgameslg/gen  -> txgameslg/server/gen  + txclientslg/src/gen
```

公共 BON、Frame、Session、Transport、Seq/Ack 或断线恢复语义发生变化时，必须分别回归 `txclient` 和 `txclientslg`。客户端业务协议和路由只来自各自服务端的 txgen 输入，不跨模板复用。

## 验收套件拓扑

### 无状态套件

```text
请求响应：
txclient --UpMsg--> agent --Actor/RPC--> game-a / game-b
game-a / game-b --Actor result--> agent --Pub--> msgq --Fetch--> agent --DownMsg--> txclient

业务主动下行：
game-a / game-b --Pub/Broadcast--> msgq --Fetch--> agent --DownMsg--> txclient

持久化：
game-a / game-b -----------------> Redis/Mongo/Fusion
msgq ----------------------------> Redis owner binding

agent / msgq / game-a / game-b 均为独立 OS 进程
```

至少启动以下实例：

| 实例 | 服务选择 | 说明 |
| --- | --- | --- |
| `txgame-agent-a` | `agent` | 唯一玩家入口，监听 TCP 和 WebSocket。 |
| `txgame-msgq-a` | `msgq` | 独立会话邮箱进程。 |
| `txgame-game-a` | `game` | 第一个无状态业务实例。 |
| `txgame-game-b` | `game` | 第二个无状态业务实例，用于成员变化和重启验证。 |

验收必须证明 Agent 没有与 `game` 同进程聚合，玩家命令通过真实 Actor Cluster 或 gRPC 边界到达业务实例。请求响应由 Agent 发布到独立 MsgQ 后返回客户端；`game-a`、`game-b` 也可以绕过 Agent 的请求响应路径，直接通过 MsgQ 单播或广播业务主动下行，再由 Agent Fetch 并写回客户端。

### 有状态套件

```text
上行业务：
txclientslg --UpMsg--> agent --Actor/RPC--> slggame
                              +--Actor/RPC--> slgworld
slggame -------------------------Actor/RPC--> slgworld

请求响应下行：
slggame / slgworld --Actor result--> agent --Pub--> msgq --Fetch--> agent --DownMsg--> txclientslg

业务主动下行：
slggame / slgworld --Pub/Broadcast--> msgq --Fetch--> agent --DownMsg--> txclientslg

持久化：
slggame -------------------------> Fusion / Redis / Mongo
slgworld ------------------------> CacheManager / Mongo WAL / Mongo checkpoint
msgq ----------------------------> Redis owner binding

agent / msgq / slggame / slgworld 均为独立 OS 进程
```

至少启动以下实例：

| 实例 | 服务选择 | 固定逻辑槽位 |
| --- | --- | --- |
| `txgameslg-agent-a` | `agent` | 无 |
| `txgameslg-msgq-a` | `msgq` | 无 |
| `txgameslg-game-a` | `slggame` | `slggame-1` |
| `txgameslg-world-a` | `slgworld` | `slgworld-1` |

编排器必须显式创建初始槽位 assignment。重启固定槽位服务时使用新的 runtime instance ID，并通过 generation CAS 交接到同名逻辑槽位；不能依赖服务发现自动猜测 owner。

## 外部服务隔离

### Redis

两套验收使用同一个本地 Redis 实例时，必须连接不同的 logical database：

- 无状态验收套件默认使用 DB 12。
- 有状态验收套件默认使用 DB 13。

Redis URI 可以通过套件环境变量覆盖，但必须仍指向该套件固定的专用 DB；同一套件内的普通存储、Fusion、延迟任务和 MsgQ 路由必须全部落入该 DB。日常 debug 配置与验收专用配置相互独立，验收不会清理 debug DB。

功能 key 保持现有模块契约，不统一增加 `txgame:*` 或 `txgameslg:*` 前缀。编排器可以使用一个不参与业务查询的所有权标记确认 logical database 是否属于当前验收套件；只有 DB 为空或标记与套件匹配时才允许执行 `FLUSHDB`。标记缺失且 DB 非空时必须停止并报告，不得清理未知数据。

### MongoDB

MongoDB 使用不同 database，默认分别为：

- `txgame_cluster_e2e`
- `txgameslg_cluster_e2e`

所有普通存储、Fusion 回写、world bucket、WAL 和 checkpoint collection 都必须位于对应 database。清理只能删除准确匹配的验收 database；不执行跨 database collection 扫描或通配删除。

### etcd

Actor Cluster、普通服务发现和固定槽位使用套件专属 prefix：

```text
/tx/e2e/txgame/actor
/tx/e2e/txgame/discovery
/tx/e2e/txgameslg/actor
/tx/e2e/txgameslg/discovery
```

固定槽位 assignment 继续位于 Discovery prefix 派生的 `slots` 路径。准备和清理只操作当前套件 prefix，不删除 etcd 根目录或其他环境前缀。

### 端口与本地产物

两套套件顺序运行，可以复用 Agent 的 `127.0.0.1:9000` TCP 和 `127.0.0.1:9001` WebSocket 端口。套件内部每个进程必须使用独立 RPC、Actor 和控制面监听端口；端口由套件配置集中声明，不允许依赖随机端口后再从日志猜测。

每次运行创建独立的本地 artifact 目录，保存：

- 实际生效配置摘要；
- 每个进程的 PID、退出码和结构化日志；
- 客户端 E2E 输出；
- 关键 etcd assignment、Redis 状态和 Mongo 文档断言；
- 失败阶段和清理状态。

artifact 目录不提交 Git。验收失败时默认保留；全部通过后可以自动删除进程临时文件，但仍在终端输出通过摘要。

## 编排生命周期

每个套件使用相同的阶段模型：

1. **Preflight**：验证命令、真实依赖、端口、专用 database/prefix 和待运行二进制；任一依赖不可用时失败，不跳过。
2. **Prepare**：精确清理已确认归属本套件的状态，写入本次配置和测试夹具。
3. **Start**：按依赖顺序启动独立进程，并等待可观察的 ready 条件。
4. **Exercise**：运行对应客户端协议场景和项目侧状态场景。
5. **Restart/Drain**：执行正常排水、固定槽位交接和明确的异常退出场景。
6. **Assert**：查询真实 etcd、Redis、Mongo 和进程日志，检查路由、序号、持久化和关闭结果。
7. **Cleanup**：只终止编排器创建且仍存活的 PID；成功时精确清理专用资源，失败时保留证据并打印人工清理命令。

Ready 等待必须基于控制面状态、服务发现状态、端口握手或可重复查询的存储条件，不能使用固定 `sleep` 作为正确性屏障。轮询必须有明确超时，并在超时时打印最近日志和当前外部状态。

## 无状态验收场景

### 基础链路

1. 确认 etcd 同时存在 `agent`、`msgq`、`game-a` 和 `game-b` 的运行实例。
2. 使用 `txclient` 分别通过 TCP 和 WebSocket 完成 `System_Ping -> System_Pong`。
3. 检查 Pong 的下行 Seq、对上行的 Ack 和 `serverTime`。
4. 在同一个 `AgentClient` 上断开并恢复，确认未确认 UpMsg 使用原 Seq/正文重放，下行 Seq 连续推进。
5. 发送一个稳定的业务拒绝场景，确认收到 `System_Error` 后连接仍可继续 Ping。

### 成员变化

1. 记录多个角色命令实际落到的 `game` instance ID，证明远程 Actor 调用经过独立业务进程。
2. 优雅停止其中一个 `game`，等待 Discovery 不再把新激活路由到该实例。
3. 保持另一个 `game` 可用并继续完成新请求。
4. 使用新的 instance ID 重启已停止的 `game`，等待重新加入后再次完成请求。

本场景不要求框架迁移已经激活在旧实例上的 Actor 内存；只验证项目声明的钝化、成员变化和新请求行为，失败必须表现为明确错误而不是静默成功。

### MsgQ 与会话

1. 证明下行由独立 MsgQ 进程分配 Seq，并在业务 handler 成功后由客户端推进 Ack。
2. 让 `game-a` 或 `game-b` 直接通过 MsgQ 向指定角色发布业务主动下行，确认不需要先经过 Agent Dispatcher。
3. 使用至少两个在线会话验证一次本地 Topic 广播，并检查实际投递、跳过和积压结果；跨 MsgQ 实例广播仍由业务先做实例级分发，不把 Topic 当作全局 Pub/Sub。
4. 正常断开 Agent 后复用同一 Session 恢复，确认 MsgQ owner 和邮箱状态允许连续会话。
5. 优雅停止并重启 MsgQ，确认新会话可建立，旧会话结果符合当前明确契约。
6. 异常终止 MsgQ 时，不把未确认下行无损恢复作为通过条件；测试必须记录旧会话是明确失败、被清理还是需要新 Session，且后续新会话能够恢复服务。

## 有状态验收场景

### 固定槽位与跨进程 Actor

1. 启动 `slggame`、`slgworld` 后显式创建 `slggame-1`、`slgworld-1` assignment。
2. 使用 `txclientslg` 完成 `System_Ping -> System_Pong`，证明独立 Agent 能创建调试身份并路由到 RoleActor。
3. 执行 `Role_Rename`，验证请求进入 `slggame-1` 的 RoleActor。
4. 执行 `World_MoveUnit`，验证 Agent 根据生成路由把客户端请求直接投递到 `slgworld-1` 的 WorldActor，并校验 string route key 与固定 world 槽位一致。
5. 执行 `Role_StartMarch`，验证另一条合法链路：请求先进入 `slggame-1` 的 RoleActor，再由 RoleActor 通过真实远程 Actor/RPC 调用 `slgworld-1` 的 WorldActor。
6. 分别从 `slggame` 或 `slgworld` 直接向 MsgQ 发布至少一条业务主动下行，确认有状态服务不依赖 Agent 请求响应才能向客户端发送消息。

### Fusion 与 CacheManager

1. 为测试角色准备可重复的 Mongo 初始数据。
2. 把 CacheManager `flushInterval` 设置为足够长的验收值，在角色命令成功后先证明 Mongo 尚未被直接同步改写。
3. 等待或触发 CacheManager flush，验证 Redis 热数据、Fusion revision/延迟任务和最终 Mongo 回写。
4. 连续修改同一根对象，证明 pending 合并且最终值没有重复覆盖或脏写。

### WAL、checkpoint 与恢复

1. 为测试 world bucket 准备初始 Mongo checkpoint。
2. 执行 `World_MoveUnit`，确认事务提交后根对象进入 CacheManager pending。
3. flush 后确认 Mongo WAL 先出现连续记录，再确认 checkpoint worker 更新 world bucket 和 `CheckpointSeq`。
4. checkpoint 成功后确认已覆盖 WAL 按当前契约清理。
5. 在 WAL 已追加但 checkpoint 尚未完成的受控窗口重启 `slgworld`，使用新 instance ID 和新 generation 接管同一逻辑槽位。
6. 确认新 owner 从 Mongo checkpoint 加连续 WAL 重建状态；旧 generation 的迟到写被 fence 拒绝，不能覆盖新状态。

### 优雅关闭

1. 使用长 `flushInterval` 制造已经向客户端确认、仍停留在 CacheManager pending 的修改。
2. 向 `slggame` 或 `slgworld` 发送 SIGTERM。
3. 确认入口先停止接收新工作，已受理 Actor/Procedure 工作由 `WaitGroupContext` 等待完成。
4. 确认 CacheManager 在依赖仍存活时完成最终 flush；`slgworld` 还必须等待已经提交的 checkpoint 到达 idle。
5. 进程必须在配置超时内以预期退出码结束。超时、pending 未清空或 checkpoint 未推进都判定失败并保留现场。

## 当前实现缺口

P0 实现不能用脚本规避以下缺口：

1. `txgameslg/server/cmd/server` 当前直接调用 `app.New(config)`。独立选择 `agent` 时缺少 debug/test identity resolver 和固定槽位 target handlers，会以 `Agent handler SPI is nil` 失败。它应与 `txgame` 一样只在非 release 环境注入明确的项目 Agent handlers；release 继续要求生产项目显式注入。
2. `txgame` 与 `txgameslg` 的 debug Redis alias 当前都使用 DB 15。无状态套件必须迁移到独立 DB，不能依赖给全部业务 key 加前缀回避冲突。
3. 固定槽位控制面提供 promotion，但初始 assignment 仍需要项目编排器通过明确的 `CreateSlot` bootstrap 创建。脚本不能把直接写 etcd JSON 当作稳定公共 API。
4. `txclientslg` 当前 `npm run e2e` 只覆盖 Ping/Pong 和断线恢复。P0 需要扩展项目级场景以驱动角色、跨 World Actor 和持久化验证。
5. `Role_StartMarch` 当前只有生成协议和路由，`slggame` 尚未注册对应 Handler。要把 RoleActor 到 WorldActor 的跨服务调用纳入通过条件，必须先补齐该项目业务链路；`World_MoveUnit` 则用于验证 Agent 直接路由到 WorldActor。
6. 现有 `txgameslg/server/tests/integration` 聚合多个服务，继续作为快速项目级回归，但不能替代独立进程套件。

## 错误与证据规则

- 真实依赖不可达、版本不满足、端口占用或隔离目标不安全时，P0 套件必须失败；不能像普通本地集成测试一样跳过。
- 每个阶段失败都必须包含阶段名、退出码、最近进程日志、相关服务发现/槽位状态和人工重跑命令。
- 客户端只断言协议和会话观察值；Fusion、CacheManager、WAL 和 checkpoint 的通过证据必须来自服务端日志与真实 Redis/Mongo 状态，客户端不得伪造。
- 业务错误与基础设施错误分开断言。业务 `System_Error` 不断线；认证、路由、调用超时和传输错误遵循连接级失败契约。
- 异常终止只验证当前已经声明的恢复边界，不把尚未实现的自动接管或可靠消息语义写成预期结果。

## 文档同步范围

设计落地时同步更新：

1. `tx/docs/IMPLEMENTATION_PLAN.md`：把 P0 同步范围扩展为六工程，并把真实客户端改为两组明确配对。
2. `tx/README.md`：TypeScript 生成和客户端协议说明同时列出两条生成链。
3. `txgen/README.md`：工程布局、TypeScript 输出、生成后回归和客户端路径同时覆盖 `txclient`、`txclientslg`。
4. `txgen/docs/design.md`：系统边界与测试分层改为六工程。
5. `txgen/docs/IMPLEMENTATION_PLAN.md`：生成契约和每轮完成条件要求同时回归两个客户端。
6. `txgame`、`txgameslg`、`txclient`、`txclientslg` README：分别记录自己在对应套件中的启动、环境覆盖、验收入口和证据边界。

## 完成条件

P0 只有在以下条件全部满足时完成：

1. 两套验收均由独立 OS 进程组成，并依次在同一台开发机上通过。
2. 所有真实依赖检查均为硬失败，没有 skip。
3. `txclient` 只消费 `txgame` 协议，`txclientslg` 只消费 `txgameslg` 协议；两套客户端测试和构建都通过。
4. 无状态套件提供远程 Actor/RPC、MsgQ、Seq/Ack、断线恢复、成员变化和优雅关闭证据。
5. 有状态套件提供固定槽位、跨 Role/World Actor、Fusion、CacheManager、WAL、checkpoint、generation fence、恢复和优雅关闭证据。
6. Redis、MongoDB 和 etcd 的准备与清理仅作用于专用 logical database、database 和 prefix，没有全局删除。
7. 重复执行两套验收不会依赖上一次残留状态，失败后能够根据保留 artifacts 定位具体阶段。
8. 六个工程执行各自受影响的全量测试、构建与 `git diff --check`，结果全部通过。
