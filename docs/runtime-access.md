# 启动绑定与显式执行上下文

CMod 仍由项目显式声明，GD 元数据只选择存储模式，不自动启用 CMod。一个正常应用进程运行一个 Bee/Ant Server，组合的子服务共享底层运行时和连接；各服务的 Context 是声明能力视图，并非独立运行时。

## 启动绑定

Bee/Ant 的 `Context.Runtime()` 返回可绑定的 `tx/runtime.Provider`。在 Setup 中只能访问本次启动已启用的能力；在 Service Factory 中仍检查该服务的 Requires。它复用既有资源，排水期间有效，最终关闭后拒绝访问。tx 不保存全局默认 Provider，也不管理业务模块单例。

项目在 Setup 把 Provider 绑定到 GD、Redis、Actor 调用等项目入口，清理回调在服务停止之后解除绑定。消息操作上下文在应用 Handler 外从当前消息派生，保留取消和期限；后台任务及加载沿用各自生命周期，不复用已结束请求。正常排水先等待邮箱与模块工作结束，再释放这些资源；超时不代表实际任务已经退出。

普通 GD 查询使用 `data.FindGD*(ctx, id)`，保存使用 `db.Save(ctx, gd)`，创建使用 `db.Create(ctx, gd)`；所有入口显式传 ctx，不提供 Access、对象式 db.Find 或可选上下文包装。Handler 外的 actorcmd 统一创建十秒操作期限并在同步响应处理后释放，继承消息取消、已有期限与事务关联。Module 沿用 ctx，不重建根范围；纯内存规则不要求 ctx。活跃 Procedure 中直接存储写入返回 `db.ErrProcedureWrite`，SLG 通过唯一缓存、锁与 WriteBack 提交。 Actor 通信保留 cmd.Actors 的独立操作便捷入口和显式 At(ctx,id)，不创建新连接或改变 Call/Tell。

模块只保存固定资源，不保存当前玩家、请求上下文或事务，不增加逐请求 Service 包装。单例获取和测试清理由项目管理，不能推导出跨进程状态唯一性。

## 必须显式保留的执行边界

`tx/runtime.Bind` 和现有显式 context API 继续用于 Actor 生命周期、事务、后台工作和独立工具。Actor 接收端绑定自身服务能力，不继承发送端 Provider；Provider 不跨 RPC 传输。

```go
reader := txruntime.FusionReader(ctx)
writer := txruntime.Writer(ctx)
executor := txruntime.Procedure(ctx)
caller := txruntime.Actor(ctx)
```

应用在 actorcmd 的 Receive 中派生期限并更新本次 MessageContext，模块内显式沿用 ctx；纯内存规则不需要无用途的 ctx。事务关联不等于数据对象自动加入事务，SLG 数据仍由唯一缓存与显式锁管理。

生成的低层 Find/Load 与 Actor Client/Ref 接口保留显式 ctx，供特殊操作与独立契约测试使用。默认 Reader 注册仍只用于显式独立工具；正式项目采用启动绑定，不混用请求 Provider 与全局默认 Reader 回退。

调用取消只结束等待，不能推断远端未执行；普通调用不自动重投。保存完成也不等于 Fusion 已最终落盘，SLG 缓存、Procedure 与 WriteBack 的职责不变。
