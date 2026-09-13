# 默认 Actor 宿主切换与公开 Kind 收敛

本轮不提交。起点为 tx `29c5f63`、txgame `c92d7fe`、txgameslg `0ae5b0f`，均在 `feature/nats-actor-runtime`。累计包含上一轮初始化期限调整；txgen、txclient、txclientslg 无源码改动。

## 结果与边界

- 默认 CModActor 已接入唯一 tx 实体宿主、共享 Discovery、gRPC 与 Core NATS。业务工厂统一 NewKind/MustKind(BehaviorFactory)，WithSlotMapping 保留业务分服、战区和独立世界 SPI。
- 两示例 Agent 使用 CModActorDirect，Kind/Key 经 tx 查找 owner 后 gRPC 直达；跨 Actor 调用使用 CModActor 的 NATS 视图。下行仍直接发布 MsgQ，客户端协议与生成 GD 未修改。
- 普通 Kind 默认 1024 稳定分区；SLG 示例 role 继续映射 slggame-1，world 继续映射独立世界槽位。未配置逐世界内存估算，不宣称自动内存均衡已完成。
- 工厂及初始化、激活事件无统一 deadline；Receive、卸载与权威查询默认 30 秒，可通过 callbackTimeout 配置。Tell 冷寻址由 tx 设置有限控制查询期限，不传播给业务初始化。
- 旧 ProtoActor 源码与关键测试保留在 internal/actortrial/testdata/protoactor；旧 Agent 测试在相邻 agent 夹具。Bee/Ant 正式依赖闭包已无 ProtoActor，go.mod 因历史测试与对照基线仍保留依赖，不能把移动统计为仓库真实删行。

简单使用：

```go
kind := actor.MustKind("world", worldFactory, actor.WithSlotMapping(worldSlot))
result, err := runtime.CModActor().Call(ctx, actor.Identity{Kind: "world", Key: "i:490001"}, delivery)
```

业务无须创建 SlotResolver、拼 PID 或管理 NATS 订阅。普通 Agent 命令使用同一 Identity，由框架提供的 CModActorDirect 执行。

## 联调发现与处理

1. 旧 Actor YAML 与独立监听端口已迁出，改为 natsURL、namespace、callbackTimeout；Discovery 租约仍独立配置。模板、集群编排和配置参考同步更新。
2. 首次 Tell 使用 Background 时，冷路径控制查询要求 deadline，导致已支持的调用形态失败；在 tx 公开调用适配设置有限寻址期限，真实加载和卸载测试复验通过。
3. txgame 旧验收只停止 Game，不交接持久分区。后续角色哈希到旧分区时被正确拒绝；验收编排现先 PrepareHandoff 确认，再逐分区 CAS，最后停止旧进程。失败 artifact 保留在 `/private/tmp/txgame-kind-cutover-e2e/20260913-232608.402448000`，没有增加自动故障改派。
4. 公开结果适配保留远端拒绝的 errors.Is 分类和 CallError 错误码，避免内部错误类别在 API 边界丢失。

## 本轮验证

- tx、txgame/server、txgameslg/server：`go test ./... -count=1` 均通过。tx 与 txgame 启用本机 NATS/etcd；SLG 启用 TXGAMESLG_SHUTDOWN_TEST=1，包含角色关闭刷盘与真实世界 Mongo 写入回归。
- actorhost、actorresult、actorrpc、framework、modules/actor、modules/agent：最终 `-race -count=1` 通过，设置真实 NATS/etcd。旧 ProtoActor Provider 的历史 race 不在此结论范围内。
- 两客户端：79/101 项测试及各自 build 通过，零 skip。
- 两示例真实 cluster-e2e 通过：多 Session、TCP/WS、MsgQ 重启、跨世界 NATS、直接上下行、写回与交接。证据分别位于 `/private/tmp/txgame-kind-cutover-e2e-fixed/20260913-232842.913401000` 和 `/private/tmp/txgameslg-kind-cutover-e2e/20260913-232714.266868000`。
- 历史 ProtoActor 与 Agent 夹具显式编译通过；uplink/build.py 的隔离 overlay 构建通过。构建成功不等同于本轮已重跑冻结 I2。
- 日志：`/private/tmp/{tx,txgame,txgameslg}-kind-final-full.log`、`/private/tmp/tx-kind-final-race.log`。

## 完整上下行性能对照

从 HEAD 只读导出旧 tx/txgame 至 `/private/tmp/tx-kind-before`，与当前工作区分别运行现有 cluster-perf。Go 1.27.0、darwin/amd64、16 CPU；每档预热 2 秒、正式 10 秒、请求超时 5 秒；负载为完整 TCP System_Ping/Pong，含 Agent、Game、MsgQ 与下行确认。按档关闭旧集群后运行新集群，没有全 NATS 上行组，也没有生产采集钩子。

| 并发 | 旧完成吞吐/s | 新完成吞吐/s | 变化 | P99 ms（旧→新） |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1040.4 | 1534.5 | +47.5% | 1.789 → 1.317 |
| 8 | 3132.1 | 5352.2 | +70.9% | 5.261 → 3.293 |
| 32 | 5865.2 | 7450.1 | +27.0% | 12.717 → 10.752 |

六档均 attempted=completed、failed=0；本轮未观察到该负载性能退化。每个配置只有一轮，不给统计显著性或生产容量结论，实例放置也可能影响样本。原始数据和路径保存在 [本轮对照](baselines/2026-09-13-default-actor-cutover.json)。冻结的 [I2 基线](baselines/2026-09-13-uplink.json) 未修改：其 1000/s、1024 字节、GD 回读及计划到达 P99 口径仍须在正式入口接入后单独完成，不能拿本次 Ping/Pong 数字替代。

## 行数与后置审查

累计 Go 净变化（含未跟踪文件、测试及旧基线）：tx +956、txgame -84、txgameslg -394，总计 +478。排除 `_test.go` 和 testdata 后分别 -1284、-36、-165；这里包含旧运行时代码移出生产路径，不能描述为实际删除 1485 行。

ponytail-review 已执行：删去新排水分支中无意义的 `errors.Join(nil, err)`，清理复制遗留的无调用错误变量及重复旧配置注释；保留 Kind 策略 SPI、错误契约和历史可靠性证据。相关回归通过，最终无待处理的过度封装项。

## 下一项

继续 I2 正式入口的固定到达率和真实 GD 回读验收，保持冻结基线与 proto/hybrid 两组、原载荷和错误阈值；不得用 Ping/Pong 单轮取代。之后评估逐世界容量声明与未收敛的旧 ActorEndpoint 发现字段，保留必要高级查询能力。JetStream、etcd 替代及单实体热迁移不在本轮范围内，I3/I4 总体尚未全量验收关闭。
