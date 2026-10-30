# Procedure 整合后的本地正式包滚动验收

日期：2026-10-30。本轮复用既有打包与集群入口，无生产代码修改，不提交，不跑云压测。

## 结果

三轮持续流量均达到 **99.99%**。保留交接期间的全部失败，不重投，不放宽超时或通过门槛。

| 场景 | 连接数 | 窗口 | 尝试数 | 完成数 | 失败数 | 完成率 | 成功请求 P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| txgame Game 发布/回滚 | 50 | 180s | 45,000 | 44,997 | 3 | 99.993333% | 1.295ms |
| SLG Role 发布/回滚 | 4 | 180s | 343,782 | 343,774 | 8 | 99.997673% | 3.058ms |
| SLG World 发布/回滚 | 4 | 180s | 1,122,651 | 1,122,643 | 8 | 99.999287% | 1.173ms |

txgame 为每连接 5 次/s 的计划负载，未发送为 0；SLG 为既有闭环客户端，按实际尝试数统计。分位数只统计成功请求，不能与失败分离后声称无长尾；三者不是相同负载，也不能作为生产容量或优化收益对比。

本机 Intel Mac，Go 1.26.5，正式服务 `GOMAXPROCS=2`、驱动上限 4。程序经过 package.py 构建、解包后在源码目录之外运行。每轮仅切换外部配置 A→B→A，程序摘要始终不变；SLG 两轮复核得到同一程序摘要。

## 验证的生命周期与存储边界

```text
正式客户端持续请求
  → 发布新配置、启动目标实例
  → 旧实例关闭准入并排水、完成必要写回
  → 槽位交接、旧实例退出
  → 新归属继续处理请求
  → 回滚配置，重复同样的交接流程

测量结束
  ├─ Role：核对最终名称的 Redis/Fusion 与 Mongo 回读，再正常关闭
  └─ World：先排水保存并关闭，再核对网格 Mongo 坐标与版本
```

- txgame 四次 Game 更新/回滚后均核对全部 50 个角色的目标归属；最终业务角色背包为道具 1×6、道具 2×3。缺 JSON 与错误结构版本两个实例拒绝启动，旧实例仍可领奖。这里是业务存储连续性证据，不将 Fusion 响应解释为同步完成 Mongo 落盘。
- SLG Role 主服放置槽位代次 1→2→3，源与目标 InstanceID 不同；每次交接后都观察到新增完成请求。4 个最终角色名称在 Redis/Fusion 与 Mongo 均核对一致。
- SLG World 的 490001、490002 两个槽位均推进到代次 3。最终排水后核对 4 个测试单位、4 个分区，Mongo 坐标一致，文档版本合计 17。
- 额外显式启用真实 Mongo 执行 `TestOwnedBucketsMongoRoundTrip`：保存后重新加载，Monster 坐标恢复通过。`TestFinalSaveFailureRetainsOwnedWorldAndRetry` 通过：保存失败保留旧运行时并允许重试；其故障来自可控 Persistence 实现，不冒充真实数据库宕机。
- 25 次正常业务进程启动全部退出 0；两次故意错误配置退出非零。核对全部 27 个记录 PID 均已退出。

## 与异步 Submit 的关系

当前默认已是 64 执行并发、8192 总接纳、10s 独立期限；SLG 集群模板未覆盖 Submit 额度，沿用框架默认。**本轮正式 Role 改名仍走同步 `txruntime.Call`；World 仍是独占网格保存，不是 Procedure。** 没有为测试临时改成异步业务，也没有在集群插入后台 Submit 发生器。

因此，本轮证明当前共享框架与应用装配的正式发布路径没有破坏排水和持久化；不声称测到了集群滚动期间排队的 8192 个 Submit。异步队列接纳、期限和排水由框架契约测试覆盖；真实业务 Submit 的请求结束、缓存、事务提交与最终 Mongo 写回，见[有限批次验收](https://github.com/ngnl47/txgameslg/blob/main/docs/procedure-submit-acceptance.md)。这些证据互补，不能互相替代。

## 环境与收尾

- 启动此前专用的 `tx-local-acceptance-mongo-20261029`（仅绑定本机 27029），未修改日常开发 Mongo 27017。
- 复用 Redis DB12/13 与既定 etcd 验收前缀，由入口检查 ownership。三轮顺序运行，成功后清理自身范围。
- A/B 为此前完整生成配置的验收副本，复制到本轮目录，经过正式打包与快照校验；本轮未修改策划 Excel、工程 JSON 或生成代码，不包含 Luban 导出回测。
- 收尾只读核对 DB12/13 均为 0 键、etcd `/tx/e2e/` 为空；隔离 Mongo 恢复为停止状态并保留容器数据，其他容器不变。
- 本轮未发现需要修改实现的问题，只更新报告和当前计划。后续可按工程归拢未提交的 Procedure 变更，准备提交。

## 复现与证据

根目录：`/private/tmp/tx-procedure-rollout-20261030`；保留三个完整包、客户端结果、进程日志、配置版本、退出码与摘要。原始临时目录可能被系统清理，主要证据保存在[机器可读记录](validation/procedure-rollout-20261030.json)。

沿用[上一轮复现命令](local-package-rollout-acceptance.md#复现入口)，配置副本根改为 `/private/tmp/tx-procedure-rollout-20261030/configs`，附加 `GOCACHE=/private/tmp/tx-go-build`；txgame 指定 `TXGAME_CONFIG_ARTIFACT_ROOT`，SLG 用 `--artifact-root` 分别保存在本轮 `txgame`、`slg-role`、`slg-world` 目录。三个持续流量命令顺序运行，不并行占用端口。

World 恢复补测在 txgameslg/server 执行：

```sh
GOTOOLCHAIN=go1.26.5 GOCACHE=/private/tmp/tx-go-build GOMAXPROCS=4 \
TX_MONGO_URI=mongodb://127.0.0.1:27029 \
go test ./services/slgworld/modules/internal/world \
  -run '^TestOwnedBucketsMongoRoundTrip$|^TestFinalSaveFailureRetainsOwnedWorldAndRetry$' \
  -count=1 -v
```

本轮没有重复上一轮完整 Go/race 回归，因为没有实现修改；只运行上述正式包真实验收及恢复补测。没有云端容量、长时间稳态、意外杀进程、全服务滚动或跨物理机容灾验收；本轮 Room 生命周期也未重复运行。
