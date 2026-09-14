# 世界数量配额与正常交接集群验收

## 本轮范围

起点六工程工作区干净：tx `bbdd202`、txgame `6ab2f20`、txgameslg `c3c76db`；txgen `8bc6c97`、txclient `5514f2a`、txclientslg `9f2d571`。本轮只修改 tx 控制面容量错误分类与 txgameslg 真实集群编排，不提交 Git。

此前框架集成测试已覆盖数量准入，但示例集群仍由控制面预建首个世界，且主要验证世界崩溃后的运维接管。本轮保留故障场景，在同一套件增加正常流程：

1. 独立世界进程使用 YAML `maximum: 1, scope: assignment, initializations: 1`；角色进程和共享压测配置不变。
2. 启动前后不注入 worldId，不预建世界归属；真实客户端首次业务请求自动建立第一代归属。
3. 检查真实成员发布的数量上限；向满载实例创建第二个世界必须返回 `ResourceExhausted`，权威存储不能留下该世界归属。
4. 保留原有世界崩溃、MsgQ 崩溃/重启、角色排水和第二代世界恢复。
5. 新增空闲世界实例后，已有归属仍保持第二代；通过正式 `PrepareHandoff` 等待源收尾。确认源成员仍存在、归属未提前改变，再执行单次 CAS 晋升第三代，随后停止源实例。
6. 第三代复用真实客户端验证 TCP/WebSocket、世界直连、跨 Actor 调用与直接下行；最终 Mongo 世界位置为 `(49,85)`，并再次检查目标名额已占用。

这是运维编排对已有控制原语的验收，不是自动部署控制器，也不是单实体热迁移。正常交接新增调用遇到错误直接失败保留现场，不重复未知写入。

## 修复

`modules/control/slot.go` 原先把容量不足映射为 `Internal`，改为 `ResourceExhausted`，覆盖创建和晋升，并保持错误包装可识别。先运行新增回归复现旧分类，再改实现；真实控制面满载请求也验证该分类。没有更改业务消息热路径。

## 验证记录

SLG 真实集群最终通过，摘要 `worldCapacityVerified=true`、`worldHandoffGeneration=3`、`worldDocumentVersion=2`。日志位于 `/Users/a47/workspace/tx-artifacts/world-capacity-bbdd202/20260914-095025.108123000`。

首轮失败保留在同根目录 `20260914-094759.826713000`：第三代新增角色只有 Login 角色投影，遗漏 Mongo 角色夹具，导致改名响应等待超时；补齐既有 Prepare 的角色列表后重跑通过，没有扩大业务超时或改变消息重试。

本轮没有 benchmark：唯一生产行为变化为控制面失败分类，其余是隔离验收编排。冻结 I2 基线和性能夹具负载未修改，不声明吞吐改善或性能无回退。没有重跑 `--shutdown-failure` 分支或客户端测试/build；客户端源码、协议和生成物均未改变。

补充回归：txgame 既有真实 cluster-e2e 通过；txgameslg/server `go test ./... -count=1` 通过；tx `go test ./modules/control -count=1` 与同包 `-race` 通过。txgame 集群 artifacts：`/Users/a47/workspace/tx-artifacts/world-capacity-txgame-bbdd202/20260914-095206.340572000`。本轮未重复 tx 全量测试或 txgame/server 全量测试；相关生产包与两个真实集群均已覆盖。

后置 ponytail-review：Lean already. Ship. 新增代码为隔离验收及两行错误映射，未引入公共工厂、重复归属表或生产采集逻辑；保留必要错误分类与真实交接检查。`git diff --check` 通过。

## 下一项

集中核对 I3/I4 尚未关闭的交接可靠性项，尤其 PrepareHandoff/Stop 并发、成功回复丢失与多个槽位部分晋升，已有证据足够的项归档，确有缺口再补测试。暂不把单世界集群验收等同于多世界规模验收，也不进入自动扩 Pod 或 JetStream。
