# MsgQ 角色归属与多会话下行实施计划

本计划串行执行，不提交 Git。设计依据为本轮确认的角色单 MsgQ 归属、独立 Session 邮箱和零逐消息 Redis 寻址共识。

## 第一批：归属和缓存基础

- [x] 新增 `modules/msgq/role.go`：角色归属值、存储接口和有界 Otter 路由缓存；不依赖项目实体 CacheManager。
- [x] 新增 `modules/msgq/role_redis.go`：复用 RedisSessionStore 的连接路由和 TTL，提供原子首次归属、读取、完整版本条件续期与释放。
- [x] 新增关键行为测试：并发首次申请只产生一个归属；旧版本不能续期或删除新版本；热路由不回源、并发冷查询合并、旧失效通知不删除新缓存。
- [x] 运行真实 Redis 测试和 MsgQ race 回归；更新 README 与设计说明，明确本批尚未切换运行入口。

验证记录：`go test ./...` 通过；`TX_REDIS_ENDPOINT=127.0.0.1:6379 go test -race ./modules/msgq/... -count=1` 通过；
缓存并发测试重复 100 轮通过。`BenchmarkRoleRouterWarm` 在本机 darwin/amd64、Intel i9-9880H 上为 204.7 ns/op、
16 B/op、1 alloc/op；预热后不增加归属读取次数。该数据仅是进程内路由基准，不代表真实网络吞吐。

归属使用 `RoleID + InstanceID + Generation`。Generation 使用随机唯一令牌，不增加集中式计数器；同实例释放后重新申请也必须换代。Redis key 有 TTL，不扫描 key。缓存不把网络超时解释成未入队，也不自动重发业务载荷。

## 第二批：运行入口与本地分发

- [x] 先修复共享分发契约：真实 gRPC 部分失败保留入队统计；明确广播载荷在错误返回后仍可能被邮箱引用；验证慢邮箱不影响健康邮箱接收和确认。

- [x] Agent 新会话选择复用角色归属；目标 MsgQ 在 Init 内再次核对有效归属，串行协调同角色初始化与最后一个邮箱的归属释放。释放由既有维护循环完成，角色租期失效后拒绝旧邮箱活动。
- [x] MsgQ 本地维护角色订阅索引，共享不可变载荷，各邮箱独立 Ack、恢复和容量限制；本地正常分发不查 Redis。
- [x] 定向角色发布携带归属版本，保留部分成功统计，明确初始化失败、旧归属拒绝和宕机干净登录行为。真实 gRPC 验证错误实例/角色/版本拒绝、部分溢出及已入队但响应丢失不重发。
- [x] 将 `Sender.PubRole` 切换到 RoleRouter 与版本化 RPC，返回 BroadcastResult；两套示例与客户端独立生成分发统计契约，不返回共同 Seq。
- [x] 角色下行统一使用 RoleRouter，保留精确 Session 寻址；测试和说明仅覆盖当前接口。
- [x] 身份、Login SPI 和邮箱生命周期统一为独立 Session，不维护会话优先级或仅写入的索引。

本批验证：tx、txgame/server、txgameslg/server 的 `go test ./...` 通过；真实 Redis 下
`go test -race ./modules/msgq/... ./modules/agent/... ./modules/actor/... ./tests -count=1` 通过。
真实 Redis 用例覆盖同角色第二个 Session 登录后第一个仍可恢复、未确认消息保留、共享下行使用独立 Seq，
以及最后一个邮箱退出后角色归属释放与换代。

## 第三批：示例与集群验收

- [x] 分别对接 txgame/txclient、txgameslg/txclientslg；业务不感知 Borrow，两个工程不交叉引用。
- [x] 两套示例的命令适配层自动共享业务成功响应；携带 RequestSeq 的 System_Error 精确返回发起 Session，处理完成确认只返回原 Agent。业务 Handler 不增加分类标记。

本批验证：两套 server 的全量 Go 测试通过；命令适配层关键测试覆盖角色寻址、精确错误和无业务载荷的处理完成确认。
- [ ] 真实多 Agent、多 MsgQ 下验证同角色集中归属、不同角色分散、热缓存 Redis 查询计数、断线恢复及故障边界。

阶段记录：两套示例均已通过跨 Agent 同角色双 Session 响应共享、独立确认和断线恢复验收。
真实 Redis 生命周期用例增加客户端命令计数：角色冷寻址 1 次，100 次热寻址/角色双邮箱下行/Fetch/Rec/Confirm 额外命令 0 次，
Session 续期到期后 100 次活动只续期 1 次。`TX_REDIS_ENDPOINT=127.0.0.1:6379 go test -race ./modules/msgq/... -count=1` 通过。
该计数尚不是独立集群各进程的合并归因，不据此勾选整项完成。其他热点 I/O 检查与后续优先级见
[热点路径 I/O 检查](../../audits/2026-09-08-hot-path-io.md)。

续期锁拆分后的独立集群回归（2026-09-08）：使用 tx `d7e34ce`，依次运行两套完整 `cluster-e2e`，均返回 `status=passed`。

- 无状态示例 txgame `1d75084`：`/private/tmp/txgame-renewal-unlocked-e2e/20260908-095442.103996000`。
  同角色 Session `362503841943552`、`362503841943555` 分别连接两个 Agent，共享下行及恢复通过；Game 重启、MsgQ 排水重启、Login 停止期间既有连接和最终排水通过。
- 有状态示例 txgameslg `b61999f`：`/private/tmp/txgameslg-renewal-unlocked-e2e/20260908-095601.185435000`。
  同角色 Session `362505587466240`、`362505587466243` 分别连接两个 Agent，角色/世界响应共享及恢复通过；WAL 恢复、world/slggame generation 2 接管、checkpoint、MsgQ 故障重启和最终排水通过。

两套均使用各自已有 ownership 隔离范围，未修改生产配置。此回归不替代进程级 I/O 计数或慢 I/O 注入。

精确 Session 下行已增加有界邮箱归属缓存：`ResolveMailbox` 只缓存 MsgQ InstanceID，Topic 的 `ResolveSession` 仍读取当前完整绑定。
两套示例独立接入显式 65536 容量，真实 Redis 热邮箱基准为 0 命令/次。tx 与两套 server 全量测试通过；
新增缓存的独立集群回归已通过（tx `f8141dd`）：

- txgame `9630604`：`/private/tmp/txgame-mailbox-cache-e2e/20260908-101308.507973000`，完整套件 `status=passed`。
- txgameslg `b1cf87d`：`/private/tmp/txgameslg-mailbox-cache-e2e/20260908-101418.653127000`，完整套件 `status=passed`，WAL 恢复与 checkpoint 通过。

两套均验证跨 Agent 多 Session 共享下行、独立确认、断线恢复、节点故障或排水重启与最终优雅关闭。
进程级 I/O 归因仍待完成：接入点是 `storage/runtime/runtime.go` 创建真实存储客户端的位置，不能用 MsgQ 方法调用数替代实际命令数。
后续采样需按进程区分冷启动、稳定请求、后台续期/健康探测，不使用 Redis 全局 MONITOR、KEYS、SCAN 或重置全局统计。

本计划不引入全 MsgQ 广播、不实现可靠业务消息队列、不附带实施其余审计项。
