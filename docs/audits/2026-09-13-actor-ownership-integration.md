# I1：事务写回与实例租约组合验证

日期：2026-09-13。基线 tx=`8de6005`；实施前六工程干净。仅修改 tx 测试及文档，未提交 Git。实施范围对应 [归属接线计划 I1](../superpowers/plans/2026-09-13-actor-ownership-integration.md)。

## 结论与边界

已有 SlotHost/SlotAllocator 能与实际 `txn.Runtime`、`writeback.Manager.Shutdown`、Redis 变更适配器及 `Discovery.Runtime` 组合，未发现本节点必须修改生产实现的缺陷。

新增组合根位于测试文件中，负责启动身份、Watch、双入口及租约丢失后的关闸。它证明组件能组合，**不表示正式 framework 已经接入新的 Actor 运行时**。当前正式 `CModActor`、PrepareHandoff、权威 reader 的公共装配入口仍按 I3 处理。

持久化使用一个可回滚字段的 Store 契约夹具，经过真实事务提交、待刷登记、Lua 写入和重新加载。没有通过自定义 Redis.Set 快照代替管道；也没有将该夹具称为 txgen 生成 GD。生成 GD、Mongo/Fusion、业务子协程完成边界及两个独立示例仍在 I4 验收。

## 两个组合场景

`TestOwnershipIntegrationWritebackHandoff`：

- Discovery 申请真实启动身份，Watch 就绪前槽位缓存拒绝读取；starting 实例不进入可路由目录，入口注册后才标记 running。
- Agent 直连请求加 3，NATS 请求加 4，两者进入 A 的同一实体邮箱。前者未完成时不返回 ACK；后者已排队，排水不能丢弃。
- B 新上线不改变 world/51 的已有归属。A 关闸后两条旧入口拒绝新业务，已接受请求继续执行，最终值为 7。
- 首次最终写回注入错误，Manager.Shutdown 到达本次期限返回失败；assignment 不晋升，脏根和 writer 保留，Redis 仍为初始值 0。
- 第二次在真实写入前暂停，交接方取消等待后再释放写入：源方可以成功写回，但调用方未获成功结果，不能继续 CAS。
- 显式重试复用源方成功收尾，再晋升到 B。B 从 Redis 加载 7，两条缓存路由均指向 B；待刷集合清空，卸载回调只执行一次，成功写入不重复。

`TestOwnershipIntegrationLeaseLoss`：

- 撤销测试实例登记使用的真实 etcd lease，等待 Discovery.Runtime 报告身份丢失；测试组合根据此关闭本地实体准入。
- 保留旧路由快照，直接访问旧 gRPC/NATS Receiver，确认拒绝来自实体运行时，而不只是目录摘除。
- B 就绪后 Ensure 仍返回原 owner 不可用，持久 assignment 不改变，B 不加载实体。
- 将旧身份路由到 B 的接收地址，验证接收方在加载前拒绝旧启动身份。这是受控的端点复用语义模拟，不是操作系统重复绑定同一端口的实测。

## 验证结果

环境：Go 工具链 1.27.0，工程语言版本 1.26；现有 NATS `2.14.6-alpine`、etcd `3.5.15`、Redis `8.8` 容器。依赖保持 go.mod 中 NATS Go `1.53.1`、go-redis `9.17.3`、etcd client `3.7.1` 和 gRPC `1.82.1`，本轮未升级。

| 本轮检查 | 结果 |
| --- | --- |
| 修改前 actortrial/framework/discovery/component/tests 基线 | 5 个包通过 |
| 新增两个真实依赖组合场景 | 2 pass，无 skip/fail |
| 定向真实依赖 race，含两个新场景及既有滚动交接、双入口交接，各运行 3 次 | 12 pass，无 skip/fail，无 race 报告 |
| tx 全量 `go test ./... -count=1 -json` | 1147 pass、63 skip、0 fail |
| 显式启用 NATS/etcd/Redis 后完整 actortrial 包 | 71 pass、1 skip、0 fail；含子测试 |
| 隔离反向验证：Go overlay 跳过 SlotHost 的 Flush | 预期失败，命中“写回失败却交接成功” |
| 成功的 race 与 actortrial 批次清理复核 | 22 个 etcd 前缀、12 个 Redis key 均无残留 |

各批包含重叠测试，不合计为独立用例数。全量默认 skip 包含未启用外部依赖的用例；真实 actortrial 包唯一 skip 是 `TestRPCComparisonProcess`，需要独立性能驱动的 `TX_RPC_COMPARISON` 开关，不是可靠性场景遗漏。

命令和原始日志保存在 `/private/tmp/tx-ownership-i1-20260913/`：`baseline.log`、`integration.log`、`race.jsonl`、`full.jsonl`、`actortrial.jsonl`、`mutation.log`、`cleanup-scopes.json` 与 `cleanup.log`。反向构建的 overlay 位于 `/private/tmp/tx-i1-mutation/`，没有改动实际 slots.go 或 Go 模块缓存。

首轮沙箱限制本机监听，基线在获得执行许可后重新运行通过。开发中修正了新夹具的 starting 查询断言及 RPC Host/WaitGroup 清理顺序；失败范围、堆栈与说明保留在上述目录，不计入最终成功清理数。反向验证故意失败的专用数据也依约保留。没有停止、创建或升级用户现有容器。

## 改动规模与后续

Go 变更为新增 427 行组合测试，以及已有 slots_test.go 的 +8/-2：净增 433 行，全部在 `_test.go`。复用既有 etcd ownership 创建/清理、SlotHost、直连 Sender 和等待助手；没有新增生产接口、依赖或采集路径。本节点是可靠性接线验收，未执行 benchmark，不作性能提升或退化结论。

后置 ponytail-review：Lean already. Ship. 本轮差异没有需删除的公共抽象或重复生产机制；Store 契约夹具、故障注入和资源清理用于上述两个组合场景，保留关键可靠性断言。

下一项是 I2：先确定代表 Procedure/GD 的负载和完整上行对照方式，再测固定到达率、吞吐/P99、资源与网络字节。云部署位置尚未确定，计费流量使用情景预算；T4 原性能筛选线继续有效，正式迁移没有自动放行。
