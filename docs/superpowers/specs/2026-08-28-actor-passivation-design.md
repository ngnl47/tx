# Actor 空闲钝化与常驻缓存淘汰设计

## 目标

为长期运行的游戏服务补齐两条彼此独立的生命周期：

1. Actor 在一段时间没有收到业务消息后自动停止，释放邮箱和执行对象；下一条消息到达时仍可按原 identity 或 route key 惰性重建。
2. 有状态项目的常驻 GD 缓存只在持久化运行时确认对象已经干净后删除引用，避免 Actor 停止后业务对象永久驻留，也避免待刷状态被提前释放。

普通玩家心跳继续使用生成业务命令并经过 RoleActor，不增加专用网络帧。只要心跳间隔短于 RoleActor 空闲时间，在线玩家的 Actor 就会保持活跃。

## 固定术语

| 术语 | Go 对应结构 | 职责 |
| --- | --- | --- |
| 空闲钝化 | `actor.WithIdlePassivation` | Actor Kind 在连续无消息达到阈值后停止实体 Actor |
| 惰性重建 | Virtual Actor placement 或 `fixedHost` | 下一条消息到达时重新创建已经钝化的实体 Actor |
| 常驻缓存 | `txgameslg/services/internal/resident.Cache` | 在进程内复用同一个 GD 对象实例并合并并发冷加载 |
| 安全释放 | `cache.Manager.Release` / `cachemgr.Release` | 在同一实体锁下确认没有事务预留、待刷根或 Store 变更 |
| 缓存淘汰 | `resident.Cache` 后台扫描 | 安全释放成功且候选未被重新访问后删除 GD 引用 |

Actor 钝化不是 GD 淘汰，GD 淘汰也不负责停止 Actor。两者不能通过回调直接绑定。

## Actor 空闲钝化

空闲时间属于 Kind，而不是全局 Runtime：

```go
kind, err := actor.NewFixedKind(
	"role",
	newRoleActor,
	actor.WithIdlePassivation(5*time.Minute),
)
```

该选项同时适用于 Virtual Actor 和固定归属 Actor：

- 普通业务消息到达时重置 tx 独立维护的空闲计时器。
- tx 的通用生命周期包装器使用独立计时器和私有钝化消息，业务 Actor 自己的 `ReceiveTimeout` 语义不受影响。
- 钝化只停止当前实体 Actor，不修改 etcd 成员、固定槽位或业务持久化状态。
- Virtual Actor 由 placement 在后续请求时重新激活；固定 Actor 由 `fixedHost` 收到终止通知后删除旧 PID，并在下一次投递时重建。
- 未配置空闲时间时保持现有常驻行为；零值不隐式选择业务策略。

钝化不能携带刷盘回调。Actor Runtime 不认识 GD、Store、Procedure、CacheManager 或 WAL。
钝化也不提升基础投递的可靠级别：`Tell` 仍允许在旧 PID 终止窗口进入 dead letter；需要确认的调用使用
`TellAccepted` 或 `Call` 并由调用方处理错误。

## 常驻缓存安全淘汰

有状态项目缓存为每个条目维护值、最后访问时间和访问代次。每次 `Load` 命中都会更新访问时间和代次；后台仅使用一个定时扫描任务：

```text
扫描到空闲候选
  -> 复制 key/value/generation
  -> 不持有缓存锁，调用 cachemgr.Release(value)
       -> 同一实体锁下检查事务预留、pending 和 Store 变更
  -> Release 成功
  -> 重新取得缓存锁
  -> 仅当 value 与 generation 均未变化时删除
```

`cache.ErrPendingRoot` 表示对象尚未具备淘汰条件，缓存保留对象并在后续扫描重试。其他错误保留对象并记录结构化错误。扫描期间发生的新访问会推进 generation，使旧候选即使已经完成 `Release` 也不能删除新近使用的对象。

缓存淘汰不为每个对象创建 goroutine 或 timer，不扫描 CacheManager 内部状态，也不在锁外读取 `Store.HasChanges()` 猜测是否安全。

## 心跳与连接

心跳仍是普通生成命令：

```text
TCP/WebSocket Session
  -> System_Ping
  -> Agent
  -> RoleActor mailbox
  -> Handler
```

心跳只证明 RoleActor 最近收到过消息，不直接延长 GD 缓存生命周期。若在线连接只发送心跳而长期不访问角色数据，轻量 RoleActor 可以保持活跃，GD 仍可在安全释放后从常驻缓存淘汰；后续真正访问数据时再由缓存加载。

断开 TCP/WebSocket 连接不直接停止 Actor。Actor 可能仍有服务间消息、延迟任务或重连请求，统一以 Kind 空闲时间决定钝化。

## 配置边界

tx Core 只提供强类型 Kind 选项，不读取 YAML。模板工程在具体 L4 服务配置中声明：

- `runtime.actor.idlePassivation.<kind>`：对应 Actor Kind 的空闲钝化时间。
- `services.slggame.residentCache.idleTimeout`：有状态项目常驻角色对象的空闲淘汰时间。
- `services.slggame.residentCache.sweepInterval`：常驻缓存扫描间隔。

配置解析在进程启动前校验正数；不合法 duration 会 fast-fail。同一份项目配置可以声明多个可选服务的 Kind，Actor Runtime
只应用当前进程实际注册的 Kind。无状态 `txgame` 只需要 Actor 空闲时间，有状态 `txgameslg` 同时配置两条生命周期。

## 非目标

- 不新增专用心跳 FrameKind。
- 不把连接生命周期等同于 Actor 生命周期。
- 不深复制 GD，也不把 GD 放进 Actor Runtime。
- 不让 Actor 超时回调直接执行存储 I/O。
- 不实现跨进程缓存淘汰协调或分布式锁。
