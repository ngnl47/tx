# 子服务、Actor 生命周期与业务模块复用

本页维护子服务、Actor 和业务模块的生命周期契约。模块采用进程内单例；GD 生成接口显式接收执行上下文，不再提供 Access 或可选 ctx 包装。

## 目标与工程边界

让子服务启动阶段、Actor 激活状态及业务模块资源归属各自清晰；保留 Room 直接寻址、按需激活的默认调用方式。合服目录及其生效方式由应用工程管理。

| 工程 | 职责 |
| --- | --- |
| tx | 通用子服务生命周期执行、Actor 激活与邮箱、状态查询、稳定归属、首次放置和排水 |
| txgen | 生成类型化身份、Ref 与协议绑定；不产生另一套生命周期状态机或业务初始化规则 |
| txgame / txgameslg | 选择启动哪些服务、指定哪些 Actor 为启动必需、加载哪些数据、模块如何复用及业务资源如何收尾 |

不新增 GlobalRoomActor 转发层、模块权限系统、通用业务模块注册容器、业务手动 MarkReady、自动 Actor 热迁移或运行中随机改派。生命周期整理不改变 Call/Tell、上行无重放及下行邮箱契约。

## 状态分层

| 层次 | 当前职责 |
| --- | --- |
| 进程实例 | Discovery 发布 Starting/Running/Draining，进程管理 RPC、控制入口和连接；Running 不等于所有业务 Actor Ready |
| 子服务 | Init 装配、可选 Start 完成必要加载、Drain 等待已接纳工作、Stop 释放资源 |
| Actor | Initializing/Ready/Closing/Failed 表达单次激活状态；控制操作与业务消息投递分开 |
| 业务模块 | 进程内单例复用能力；激活状态由 Actor 持有，GD 操作使用本次 Handler 或 Procedure 的上下文 |

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

当前顺序为：初始化所选服务 → 通信基础设施可用 → 执行各服务必要启动工作 → 服务 Running。进程发现状态和子服务状态是不同事实；不能把子服务状态机械复制为进程状态。

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

不要求每个业务模块添加 Load/Unload 空方法。公共 Runtime 组合 Messenger 和 Controller，三项控制操作保留原签名；WaitReady 是低频只读辅助函数，不增加传输协议。外部卸载及针对某次激活的完成等待留作独立需求，不以缺少代次标识的身份请求代替。

## 模块复用与资源归属

tx 不分类 Rank、Room、Bag，不维护业务模块容器。项目保留 modules/<业务>.go 与对应 internal 实现边界，模块通过项目进程内单例获取。同进程的其他子服务可显式复用公开模块，直接调用不等于投递 Actor 邮箱，业务负责选择正确的状态访问方式。

项目启动绑定 GD、Redis 与 Actor 调用资源；模块获取不使用 ctx，GD 查询、修改、事务及远端调用沿用显式操作上下文。无状态 Handler 使用匿名内嵌自动提升方法，不逐个绑定 Access，也不保存请求或 Actor 状态。真正的 Actor 生命周期、事务与排水上下文保留；底层操作有期限但不以取消信号代替实际完成。

模块单例不持有玩家、Room 或 World 激活状态，也不改变缓存、Sender、WriteBack 的关闭责任。进程组合与独立部署都不把本地单例解释成分布式状态权威。启动绑定和显式上下文的分工见 [runtime-access.md](runtime-access.md)。

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

## 验收与尚未提供的能力

- 启动验收覆盖 Init/Start 顺序、加载失败回滚、启动中业务拒绝与控制可用；组合部署和本地调用不得绕过准入。
- 排水超时保留 Draining 和未完成资源，不能提前报告 Stopped。Handler 内请求自身卸载不能同步等待自身结束。
- 正式包的 Role、Room、World 生命周期与存储验证见 [本地验收记录](local-package-rollout-acceptance.md)；历史报告仅证明指定版本、拓扑和场景。
- 外部卸载及针对某次激活的完成等待尚未提供；有实际需求时再定义代次和资源边界，不通过删除 GD 或缓存条目模拟。
- 按 Room 独立选择实例、自动 Actor 热迁移和云端容量均不是上述生命周期契约已经验证的能力。当前待办统一维护在 [实施计划](IMPLEMENTATION_PLAN.md)。
