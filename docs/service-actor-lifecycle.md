# 子服务、Actor 生命周期与业务模块复用

状态：2026-10-27，子服务生命周期与 Actor 控制入口整理已接入；已完成启动装配与 Access 统一，宿主资源所有权保留。本文分别说明当前行为与后续边界。

## 目标与工程边界

让子服务启动阶段、Actor 激活状态及业务模块资源归属各自清晰；保留 Room 直接寻址、按需激活的默认调用方式。先完成这项整理，再继续合服相关工作。

| 工程 | 职责 |
| --- | --- |
| tx | 通用子服务生命周期执行、Actor 激活与邮箱、状态查询、稳定归属、首次放置和排水 |
| txgen | 生成类型化身份、Ref 与协议绑定；不产生另一套生命周期状态机或业务初始化规则 |
| txgame / txgameslg | 选择启动哪些服务、指定哪些 Actor 为启动必需、加载哪些数据、模块如何复用及业务资源如何收尾 |

不新增 GlobalRoomActor 转发层、模块权限系统、通用业务模块注册容器、业务手动 MarkReady、自动 Actor 热迁移或运行中随机改派。生命周期整理不改变 Call/Tell、上行无重放及下行邮箱契约。

## 现有能力与缺口

| 层次 | 当前实现 | 本轮确认的改进方向 |
| --- | --- | --- |
| 进程实例 | Discovery 的 Starting/Running/Draining 等状态；RPC、控制入口及连接由进程管理 | 不把进程 Running 当成全部业务 Actor Ready |
| 子服务 | Init/可选 Start/Drain/Stop；World 在 Start 中等待指定 Actor Ready | 显式表达子服务初始化与运行阶段，消除散落的隐式启动标记 |
| Actor | Unloaded/Initializing/Ready/Closing/Failed；EnsureActivation、Probe、RetryActivation 和 ctx.Deactivate | 系统说明请求、完成、等待和重试的语义；补充接口前先复用已有能力 |
| 业务模块 | Modules 启动构造模块，Handler/Service 内嵌固定 Access | 模块获取无 ctx，保留 I/O 上下文及资源关闭责任 |

源码核对入口：`internal/framework/server.go`、`internal/framework/service_host.go`、`internal/actorhost/runtime.go`、`modules/actor/runtime.go`；示例见 txgameslg 的 `services/slgworld/{service,bootstrap,registration}.go`。

## 子服务生命周期

标准 Bee/Ant MsgQ 在全部服务启动钩子成功前，通过 `StartupAdmission` 拒绝本地会话请求及远程邮箱、发布请求；未开放时不创建会话或领取角色归属。首次准入成功后锁存开放状态，长流不持有服务 Ingress 计数，后续收尾仍由邮箱 Stop 和既有归属协议管理。该检查不增加逐消息确认或重投，也不能用进程发现 Running 绕过启动闸门。

`lifecycle.ServiceState` 由 tx 维护，`Server.ServiceStatus(name)` 返回阶段及 Failure 快照；业务不能直接赋值：

```text
Created → Initializing → Initialized → Starting → Running
                                                  ↓
                                               Draining → Stopped
```

- Created：选中服务已登记，尚未执行 Factory/Init。
- Initializing：准备本服务资源、模块与 Handler。
- Initialized：装配完成，不表示项目指定的启动工作已完成。
- Starting：通信基础设施可用后，执行本服务的必要启动工作。
- Running：必要启动工作成功，允许常规业务准入；不意味着这个服务的全部潜在 Actor 已激活。
- Draining：拒绝新业务，等待已接受工作与必要收尾；超时或失败时保留资源，不直接跳到 Stopped。
- Stopped：实际资源收尾完成。启动失败保留失败原因并执行回滚；不能仅设置 Failed 就绕过清理。失败通过 ServiceStatus.Failure 保留，不增加掩盖关闭阶段的 Failed 状态。

`Service.Init(ctx)` 为必需接口，装配模块与 Handler；可选的 `ServiceStarter.Start(ctx)` 执行通信就绪后的加载。没有额外启动工作时不需要空 Start 方法。全部启动钩子成功、会话分配就绪后，框架统一开放组合内的业务入口。

### 基础设施可用与业务准入

全部 Service.Init 完成后，框架启动 RPC/控制监听并发布进程 Discovery Running，再逐个执行 Service.Start。启动钩子只等待控制面加载，不同步调用仍未开放的普通业务入口。

当前顺序为：初始化所选服务 → 通信基础设施可用 → 执行各服务必要启动工作 → 服务 Running。进程发现状态和子服务状态是不同事实；本轮不预设修改发现协议，也不把子服务状态机械复制为进程状态。

启动中的服务须允许已有生命周期控制路径完成指定 Actor 激活，同时常规客户端、内部业务和任务入口不能以“端口已监听”绕过服务准入。本地直调也应遵循相同规则；不得用一个业务可传入的布尔值放行普通消息。外部 Admission、Actor Submit/Post 共用本地子服务阶段；After 保留尚未入队的定时消息直到服务就绪或激活关闭。任务消费者在服务开放后才开始领取，避免启动窗口消耗投递次数。Starting 可使用控制面激活，Created/Initializing/Initialized 不触发业务工厂；排水仍使用既有入口、邮箱和任务屏障，不新增远程查询或第二套计数。

World 示例：GDWorld 加载完成且目标 Actor Ready 后，才完成指定 World 的服务启动工作。这个一次性启动要求不等于永久驻留；后续空闲卸载仍遵守既有配置。活动开启、玩家进入资格仍由项目判断。

## Actor 生命周期控制

Actor Ready 继续晚于工厂及全部初始化/激活回调成功。业务 Load 只是其中的数据加载步骤，不独立创建 Actor，也不设置框架状态。

| 操作 | 当前入口 | 保持或补齐的契约 |
| --- | --- | --- |
| 查询状态 | Probe | 无副作用，不分配归属、不激活、不读取未就绪 GD |
| 确保激活 | EnsureActivation | 已存在则复用；并发首次请求共享初始化；返回当前状态，不承诺已经 Ready |
| 等待就绪 | actor.WaitReady(ctx, controller, id) | 只读轮询 Probe；仅 Initializing 等待，Ready 成功，其余状态或查询错误终止；不自动激活或重试 |
| 重试失败 | RetryActivation | 清除已结束的失败再确保加载；无失败时等同 EnsureActivation，不替换正在加载或关闭的激活、不重投原业务请求 |
| 当前激活请求卸载 | ctx.Deactivate() | Handler 可以调用；框架关闸并收尾，不允许业务直接销毁邮箱或激活状态 |
| 外部请求及等待卸载 | 当前 Runtime 无对称公共入口 | 作为后续独立改动评估，不能等同于删除 GD、淘汰缓存或迁移 assignment |

必须保留以下边界：

- Handler 可以请求自身卸载，但不能同步等待自身 Handler 返回后才能完成的卸载，避免自等待。
- 调用方取消或超时只结束等待，不证明初始化、业务执行或清理已结束；资源与容量在实际完成后释放。
- 卸载不删除持久业务实体；普通消息以后仍可能重新激活它。业务永久关闭由业务存在性或状态校验表达。
- 清理完成前不能重建同一身份；外部卸载入口还须明确针对哪一次激活，避免延迟控制请求误卸载新激活。不得为了等待卸载而先激活一个不存在的 Actor。
- 当前卸载回调错误不否决卸载；必要持久化仍按现有缓存、WriteBack 与服务排水契约处理，不把新控制入口包装成更强的落库保证。

本次设计不改名现有四个入口，也不要求每个业务 Service 都添加 Load/Unload 空方法。公共 Runtime 组合 Messenger 和 Controller，三项控制操作保留原签名；WaitReady 是低频只读辅助函数，不增加传输协议。外部卸载及针对某次激活的完成等待留作独立需求，不以缺少代次标识的身份请求代替。

## 模块复用与资源归属

tx 不分类 Rank、Room、Bag，不维护业务模块容器。项目保留 modules/<业务>.go 与对应 internal 实现边界，模块通过项目进程内单例获取。同进程的其他子服务可显式复用公开模块，直接调用不等于投递 Actor 邮箱，业务负责选择正确的状态访问方式。

项目启动绑定 GD、Redis 与 Actor 调用资源，普通模块不传递只用于资源定位或普通 I/O 的 ctx。无状态 Handler 使用匿名内嵌自动提升方法，不逐个绑定 Access，也不保存请求或 Actor 状态。真正的 Actor 生命周期、事务与排水上下文保留；底层操作有期限但不以取消信号代替实际完成。

模块单例不持有玩家、Room 或 World 激活状态，也不改变缓存、Sender、WriteBack 的关闭责任。进程组合与独立部署都不把本地单例解释成分布式状态权威。生成绑定入口与显式上下文入口的区别见 [runtime-access.md](runtime-access.md)。

## Room 的默认创建链路与首次选址

```text
Game：FightroomAt(ctx, roomID).Start(request)
  → tx 解析 Type/ID、服务池和稳定槽位
  → 已有 owner 则沿用；没有 owner 才做首次选址与 CAS
  → owner 上激活 Actor，工厂创建 Room 业务对象
  → 初始化回调成功，Actor Ready
  → 邮箱调用 RoomHandler.Start，设置成员并开始战斗
```

At 只构造引用，Start 才发起 Call；调用方不是执行方。组合部署时可以走本地邮箱，仍由 Fightroom Actor 执行。Room 激活就绪与业务开战成功是两个不同阶段，不增加全局中转 Actor 来掩盖这一区别。

GlobalRoomActor 只在匹配、组队、配额等业务编排有实际需求时引入；单纯选择实例与激活 Actor 使用 tx 通用能力。

当前 Room 使用默认 1024 个稳定分区，首次 owner 由哈希起点、候选容量检查与 CAS 决定。后续选址策略扩展仅作用于未分配归属，不重算已有 owner；不能把随机选址理解为每条消息随机转发。

本轮不改变分区布局。若以后要求每个新 Room 独立选择实例，须另外确认放置粒度、归属记录数量及释放方式；这不是新增一个随机选择函数就能保持现有布局不变的改动。

## 后续实施顺序与验收门槛

各批次独立验证，不自动提交。

1. **子服务生命周期（已实现）**：tx 的 service_host、server 及 Bee/Ant 服务适配已分阶段；World started/AfterStart 已移除。验收初始化/启动顺序、指定 World 加载失败回滚、启动中业务拒绝但控制可用、组合部署和本地路径一致、排水超时不提前 Stopped。不能在“服务 Running 才能寻址”与“寻址完成才能 Running”之间形成循环等待。
2. **Actor 控制入口整理（已实现）**：Runtime 组合 Messenger/Controller，WaitReady 收敛 World 就绪轮询；保持激活与 Call/Tell 语义。World 等待完成后再次核对本机归属。外部卸载未纳入本批；现有 ctx.Deactivate 保持请求即返回，清理结束才释放激活名额，旧句柄不影响新激活。
3. **项目模块统一访问**：启动时绑定 Access，验证宿主隔离、无请求包装、订阅与 Actor 状态的生命周期；I/O 与事务沿用原调用上下文。
4. **完整本地回归（正式包生命周期已验收）**：两组示例分别使用正式打包和集群入口验证 Role、Room、World；真实依赖用例单独启用，未启用不能计为通过。常规上下行不得新增远程生命周期探测，路由缓存命中与消息投递微基准用于检查回退；不自动开展云端容量测试。

txgen 默认不增加新输入注解或按 Actor 生成整套生命周期管理器；只有上述实际接入证明生成 API 有缺口时才修改。客户端协议、Excel、WorldID 映射及已有哈希不属于本批改动。

## 本批实现与验证记录

- 必需 Init、可选 Start；启动钩子使用调用方取消信号，托管后台任务继续使用独立 lifecycle，防止取消等待提前释放资源。
- 全部服务初始化后才执行 Start，全部 Start 成功后统一开放准入；指定 World 不会和自己的 Running 准入形成循环等待。
- Init/Start 失败沿用回滚与 Actor 排水；排水超时保留 Draining 与资源，停止完成后保留 Failure 供诊断。
- txgame/txgameslg 已同步迁移；Sched 先装配协议表，再在 Start 登记任务并启动调度。
- 最终只读复核的两项启动闸门缺口已修复：Post/After 及 Sched HTTP 共用准入；After 只等待尚未入队消息，不重投已执行消息。
- 本地回归与真实依赖验证（Claims JSON 缺口已单独修复）见 IMPLEMENTATION_PLAN；本批不变更 txgen、协议、配置、Scope 或 Actor 公共控制 API。

### Actor 控制入口整理

- Controller 仅承载 Probe、EnsureActivation、RetryActivation；业务调用与生命周期控制可以分别依赖，不新增管理器或每 Actor 控制对象。
- WaitReady 只等 Initializing；遇到 Unloaded/Closing/Failed 或查询失败立即结束。保留原失败对象，超时返回调用方 Context 错误，不隐式重试加载或转移归属。
- Ready 是身份的瞬时观测，不锁定 owner 或激活代次。指定 World 的本机归属核对属于项目；框架不绑定 World。
- 不提供远程卸载及等待清理完成的新协议，也不改 Redis/Fusion 编解码。跨服务榜单读取实现见下。

### 模块统一访问

当前方案以本文“模块复用与资源归属”和两组工程 docs/plans/module-scope.md 为准。Rank、Bag、Role 等通过项目进程内单例获取，固定资源在启动时绑定；不再在共享 Handler/Service 中注入 Access。正式包生命周期的历史验收见 IMPLEMENTATION_PLAN；本次重构另外执行本地回归，不将历史验收冒充本轮测试。
