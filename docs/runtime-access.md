# 启动绑定与显式执行上下文

CMod 仍由项目显式声明，GD 元数据只选择存储模式，不自动启用 CMod。一个正常应用进程运行一个 Bee/Ant Server，组合的子服务共享底层运行时和连接；各服务的 Context 是声明能力视图，并非独立运行时。

## 启动绑定

Bee/Ant 的 `Context.Runtime()` 返回可绑定的 `tx/runtime.Provider`。在 Setup 中只能访问本次启动已启用的能力；在 Service Factory 中仍检查该服务的 Requires。它复用既有资源，排水期间有效，最终关闭后拒绝访问。tx 不保存全局默认 Provider，也不管理业务模块单例。

项目在 Setup 把 Provider 绑定到 GD、Redis、Actor 调用等项目入口，清理回调在服务停止之后解除绑定。消息操作上下文在应用 Handler 外从当前消息派生，保留取消和期限；后台任务及加载沿用各自生命周期，不复用已结束请求。正常排水先等待邮箱与模块工作结束，再释放这些资源；超时不代表实际任务已经退出。

`txruntime.Procedure(ctx)` 与 `CModProcedure()` 均返回同一类受限执行视图，只提供 Call/Submit；其动态值不暴露事务运行时的 Drain、配置或提交参与者绑定能力。这些管理操作由框架拥有。

普通只读使用 `data.SnapshotGD*(ctx, id)`。启用共享缓存时，完整 `data.FindGD*(ctx, id)` 与 `db.Create/Remove` 必须在 Procedure 内执行，修改交由 WriteBack；未启用缓存或显式排除的独占对象使用 Find 与 `db.Save/Create/Remove` 直接存取。所有入口显式传 ctx，不提供 Access、对象式 db.Find 或可选上下文包装。Handler 外的 actorcmd 统一创建十秒操作期限并在同步响应处理后释放，继承消息取消、已有期限与事务关联。Module 沿用 ctx，不重建根范围；纯内存规则不要求 ctx。活跃 Procedure 中 `db.Save/SaveChange` 拒绝直接持久化；`db.Create` 通过唯一缓存占位、事务回滚与 WriteBack 首次写入登记新根对象，未绑定缓存或受管写回则报错。结束后的事务 ctx 不得用于存储写入。 Actor 通信保留 cmd.Actors 的独立操作便捷入口和显式 At(ctx,id)，不创建新连接或改变 Call/Tell。

模块可以保存资源与自己拥有的配置缓存、查询索引，不保存当前玩家、请求上下文或事务，不增加逐请求 Service 包装。单例获取和测试清理由项目管理，不能推导出跨进程状态唯一性。

## 必须显式保留的执行边界

`tx/runtime.Bind` 和现有显式 context API 继续用于 Actor 生命周期、事务、后台工作和独立工具。Actor 接收端绑定自身服务能力，不继承发送端 Provider；Provider 不跨 RPC 传输。

```go
reader := txruntime.FusionReader(ctx)
writer := txruntime.Writer(ctx)
executor := txruntime.Procedure(ctx)
caller := txruntime.Actor(ctx)
```

应用在 actorcmd 的 Receive 中派生期限并更新本次 MessageContext，模块内显式沿用 ctx；纯内存规则不需要无用途的 ctx。生成期启用 EntityCache 时，完整 Find 仅在有效 Procedure 内复用唯一对象并纳入记录锁与写回预留；普通读取使用 Snapshot，在相同记录锁内深复制，返回前释放使用权。

生成的 GD 查询统一显式 ctx；Actor Client/Ref 的显式 ctx 入口供需要继承当前范围的调用与独立契约测试使用。默认 Reader 注册仍只用于显式独立工具；正式项目采用启动绑定，不混用请求 Provider 与全局默认 Reader 回退。

调用取消只结束等待，不能推断远端未执行；普通调用不自动重投。保存完成也不等于 Fusion 已最终落盘，SLG 缓存、Procedure 与 WriteBack 的职责不变。

### 统一 GD 实体访问

`gen/config.yml` 的 `data.entityCache` 是生成期选项，缺省 false；txgame 为 false，txgameslg 为 true。少数独占根用 `@noEntityCache` 排除。运行期只有共享回收预算，不设置第二个缓存启停开关，也不登记逐 GD 的缓存。根 Store 文件内生成描述与 Snapshot，启用时增加 `entity_access.go`。缓存按描述与完整 `RecordKey` 区分身份；组合子服务共享同一应用的缓存，不同应用运行时独立。

```text
普通只读操作
  -> data.SnapshotGD*(ctx, 完整主键...)
  -> EntityCache 取得使用权 -> 等待同一记录锁
  -> 深复制当前内存字段 -> 释放锁和使用权 -> 返回独立对象

txruntime.Call(ctx, procedure)
  -> Locks 预声明已知键并排序取得锁 -> Process
  -> data.FindGD*(tx.Context(), id)
  -> 缓存使用权 -> Enlist：记录锁 / 写回预留 / undo
  -> Store 修改，或 db.Create / db.Remove
  -> 成功：发布创建或隐藏删除 -> WriteBack 待刷
  -> 失败：撤销修改、创建占位与删除
  -> 锁释放、提交/回滚回调结束 -> 释放使用权

WriteBack / 缓存回收
  -> 同一记录锁 -> 写入存储或检查干净状态
  -> 失败：保留 pending 与原对象 -> 后续 Flush / 排水重试
```

共享可变 GD 只能在有效 Procedure 中取得；无事务完整 Find 在加载前返回 `entitycache.ErrProcedureRequired`，失效事务返回 `txn.ErrInactive`。启用缓存的持久根必须显式声明 `@transaction`，生成期缺少声明直接报错；不会为 Handler 隐式开启事务。对象和子对象引用只可在当前事务内使用，不得长期保存在 Actor/后台任务中。缓存内部互斥锁只保护元数据，等待记录锁、复制字段或访问存储时不持有它。取消信号不能提前释放仍在执行事务的使用权。

`SnapshotGD*` 读取缓存中的最新值，包含尚未写回的提交；在有效事务内复用 Find 与现有锁，可读到本事务修改。复制不携带 Store、tracker、事务状态，调用方可自行修改副本但不能用它 Save/Remove 原实体。普通快照每条记录一致，不保证多条快照处于同一时刻。创建中身份仅对所属事务可见；其他读取返回 `ErrCreating`；提交删除后不再返回对象，持久化失败也不重新暴露旧对象。

完整批量 Find、FindObjects 与查询别名复用同一受控单条入口。字段投影、FindObject、显式 Reader 的 Load* 是独立的**存储快照**，看不到未落盘缓存修改，不是共享对象的第二个写入口；投影与 FindObject 在 Procedure 内拒绝。Mongo 原生入口同样不参与缓存与事务。底层存储接口用于适配器和明确独占路径，不得旁路修改正在受管的同一记录。

共享根禁止直接 `db.Save/SaveChange`；Create/Remove 必须在有效 Procedure 中，删除还校验原对象身份。创建允许先给临时对象赋初值，接纳后不得改变记录键和锁身份。失败首次写回保留 pending，结果未知不能自动认领已有记录；删除失败由 WriteBack 的 Flush/排水重试。已移除缓存普通 Save/Remove 的同步协调入口。关闭缓存或被排除的独占 GD 保留直接存取，不增加事务要求。

EntityCache 使用 Otter 选择软容量候选；使用中、创建中、删除未完成、dirty 或 pending 的对象不会为满足容量被强制丢弃。缓存回收与 Actor 卸载独立，持久化收尾归现有 WriteBack 排水。框架不提供跨进程唯一对象、跨记录数据库原子提交或崩溃 WAL 保证。

## Mongo 原生查询

`txruntime.MongoDatabase(ctx, route, routeValue)` 返回官方 `*mongo.Database`；`txruntime.MongoCollection(ctx, spec, routeValue)` 进一步从 GD 元数据选择集合。两者复用 CModMongoStorage 的连接与路由，不创建第二个客户端。Bee/Ant 的 `CModMongoStorage()` 同样提供 `MongoDatabase`，低层 `txmongo.Collection` 可按类型选择集合。

```text
应用 db.MongoCollection(ctx, GD 类型描述, 路由值)
  -> 当前服务声明与运行时生命周期校验
  -> Mongo 健康闸门（取得句柄时）
  -> 既有存储路由 -> 已连接 Database -> Collection
  -> 官方 Find / options / Cursor.Decode
```

高级查询直接使用官方 BSON、排序、投影、聚合与游标 API。取得句柄后，操作和错误由官方驱动处理，不再经过 tx 健康闸门，也不自动加入 Procedure、EntityCache 或 WriteBack。Mongo 原生读取看不到尚未落盘的缓存修改；原生写入不刷新缓存，不能用于旁路修改正在受管的同一实体。连接由宿主统一关闭，业务不得调用共享 `Client.Disconnect`。

GD 原生解码使用 BSON 标签，必须为每条文档创建新对象，再按需取得 `Store()`；不要复用已跟踪修改的 GD 作为 Decode 目标。该路径不附加生成 Load 的字段加载标记，也不初始化缺失 map；投影结果宜使用专用查询结构，不得当作完整 GD 保存。`uint64` map 键可以使用完整无符号范围，BSON 数值本身仍受官方驱动与 BSON int64 范围约束。

正常遍历结束检查 `cursor.Err()`；错误/取消路径也要关闭游标，关闭使用独立的有界 ctx，不能依赖已经取消的查询 ctx。原生能力保留这个职责给调用者，不新增查询 DSL 或游标代理。


## 服务持久化收尾

Bee/Ant 的可选 ServiceFlusher.Flush(ctx) 在全部 Actor 与已登记后台任务排空后、CMod 排水前执行。它只表达服务拥有的数据必须完成持久化，不规定 World、网格、保存周期或后端。已成功的服务 Flush 不因其他服务/传输失败重复执行。

```text
停止外部准入 -> 可靠任务排水 -> Actor 卸载 -> 服务后台任务结束
  -> ServiceFlusher.Flush -> CMod 排水 -> 传输确认
  -> PrepareHandoff 成功（保留连接/租约）或 Stop 释放资源
```

Actor 卸载回调的错误会记录，但不能单独保证保存完成；需要阻止正常交接的服务须保留失败对象，在 Flush 内重试并返回错误。正常 Flush 失败沿用 Stop/PrepareHandoff 可重试边界，保留依赖与绑定，不将失败视为完成。致命租约丢失仍按既有强制终止路径处理；内存数据不因此获得宕机恢复保证。

服务后台任务收尾先取消全部服务任务，再等待实际退出。等待期限取调用方剩余期限与 `runtime.lifecycle.drainTimeout` 的较小值；超时不执行后续 Flush 或释放服务依赖，Stop/PrepareHandoff 的重试复用同一个完成信号。重试不会重新开放准入，也不会重复创建等待协程。

### 本地性能与使用限制

受管 Find 包含记录锁、使用期和事务登记，不能用底层 Cache 的无锁命中数据代替。Snapshot 在锁内深复制，时间与分配随实体大小增长；若一次业务已处于 Procedure，可在当前事务内提取必要响应字段，避免多次整对象复制，但不能将共享 map、slice 或子指针带出事务。独占 World 继续在网格邮箱内读取和保存，不引入第二份共享快照。

Go 的公开字段无法阻止调用方私自保存裸指针并在事务外修改。当前保证基于所有共享访问都遵循生成入口及 Procedure 契约；快照和误用检查不是对任意业务代码的数据竞争沙箱。

本轮量化数据、命令与跳过范围见 [GD 访问本地验收](entity-access-acceptance.md)。

## EntityCache 管理入口

`txruntime.EntityCache(ctx)` 返回同一应用的缓存运行时。Stats/List 只观察框架元数据；SnapshotCached 只复制当前缓存对象，不回源；TryRelease 复用 WriteBack 安全判定，不刷盘或删除记录。EntryRef 关联驻留代次，旧引用不能操作重新加载的对象。管理快照/回收必须在 Procedure 外调用，并遵守独立的排水/关闭准入；业务查询仍用生成 Find/Snapshot。详见 [管理 API](entity-cache-management.md)。

## CMod 与初始化回滚的关闭边界

```text
正常 Stop
  -> 完成排水与持久化 -> Service.Stop（传输入口仍有效）
  -> 取消根任务 -> 按关闭期限等待实际退出
  -> 解除绑定、关闭连接 -> 逆序 CMod.Stop -> Stopped
启动失败
  -> 部分服务：Stop 唤醒资源等待 -> 限时等待服务任务
  -> 部分 CMod：限时等待任务 -> Stop 自身资源 -> 逆序关闭依赖
超时
  -> 保留尚未关闭的对象和依赖 -> 后续 Stop 继续原阶段
```

CMod 回滚与最终关闭使用现有 `lifecycle.drainTimeout`，不增加独立配置；直接使用 `component.Host` 时由 `Start(ctx)` 和 `Stop(ctx)` 提供期限。回调必须遵守传入期限，框架不通过遗留 goroutine 模拟回调已完成。开始最终释放后不再重新 Drain、Flush 或交接；已释放阶段不会重复执行。

组件实际工作未退出时，宿主自动保留依赖。自定义 CMod.Stop 若尚未完成，应返回包含 `component.ErrStopDeferred` 的错误；普通关闭错误（包括已完成关闭时返回的 DeadlineExceeded）作为最终结果记录，并继续释放依赖。不能仅按错误文本推断资源是否仍在使用。

Placement 和 CMod 只取得 `lifecycle.Tasks` 受限视图，其 Go 回调同样拿不到 Cancel、Wait 或计数所有权。部分启动失败的组件只执行清理，不再调用要求完整启动的 Drain。

启动阶段的框架错误应终止本次启动，不能开放业务准入或报告启动成功。失败清理用于释放已经取得的资源；有界等待和有限清理重试不代表恢复启动或继续运行部分子服务。运行期正常关闭仍须排空已接纳工作并完成持久化。

一个 Server 运行时只有一份 Procedure CMod，各子服务共享事务运行时。当前实现未发现内置 CMod 或两组应用在 CMod.Start 中提交异步 Procedure；不将这一可构造的扩展场景列为已确认缺陷，也不据此增加专门的回滚机制。

### 启动错误传播与消费准备

```text
CMod 准备连接、读取 ConsumerInfo、静态校验
  -> Setup / Service.Init -> RPC 与控制面监听 -> Service.Start
  -> 检查启动错误 -> 开放业务准入 -> 启动任务消费 -> 提交启动成功

启动期致命错误 / 传输入口非预期退出
  -> Server 登记错误并取消启动操作 -> 停止后续阶段
  -> 有界清理已取得资源 -> Start 返回原始错误及清理错误
```

启动取消与根生命周期分离，保留必要依赖用于失败清理；回调须响应取消，框架不强杀 goroutine。RPC Host 的失败回调由 Server 接收，独立使用 Host 时可传 nil；回调不能同步等待 Host 关闭。控制面同理。正常 Stop、根取消与 gRPC 正常停止不报告故障。

任务 Prepare 不领取消息，Start 不重新读取或校验 ConsumerInfo；静态错误在开放准入前暴露，实际消费仍在业务就绪后开始。Broker 后续变化仍可使启动失败，不自动重建消费者或重启服务。
