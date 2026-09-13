# 正式 Actor 入口 I2 上下行验收

日期：2026-09-14。正式入口满足此前接受的 I2 临时预算，允许继续接线；不代表生产 SLA，也不宣称饱和吞吐没有退化。

## 原因与改动范围

默认后端切换已提交：tx `eef61a5`、txgame `c305eab`、txgameslg `2b99d1f`。另外三仓为 txgen `8bc6c97`、txclient `5514f2a`、txclientslg `9f2d571`，本轮均未修改。

此前冻结 hybrid 使用手工装配宿主与静态归属快照，不能替代正式 CMod 验收。本轮只在 `internal/actortrial/testdata/uplink` 增加 `Formal: true` 装配，通过真实 Bee、应用 Kind 目录、Discovery 租约/Watch、默认 1024 分区、首次 CAS 和加载前权威校验运行同一业务负载。生产源码和生产依赖未修改，开关只在测试配置内。

例如一次上行经过 Agent → CModActorDirect/gRPC → game，约 10% 请求再通过 CModActor/Core NATS 调用 world；下行由业务直接写 MsgQ，再经 Agent 返回。完成要求 ACK 与下行内容均匹配。没有重测上下行经 NATS 的路径。

## 口径

复用冻结夹具：1024 Session、512 game 实体、32 world、64 worker；每窗预热 1024 次，每次测量 4 秒。五轮交替执行 ProtoActor 与正式 hybrid，分别覆盖 128 B 闭环、1024 B 闭环、1024 B 1000/s、1024 B 3000/s，共 40 窗。

生成 GD 沿用 txgame Redis ResRankItem：16 个初始子项，每请求更新根字段和四个子项，100ms 写回。每窗最终 Flush 和生成 Load 回读独立校验，不计入稳态窗口。Redis AOF/RDB 未启用，本验收不验证 Mongo/Fusion 或磁盘持久化保证。

Linux/amd64 Docker、各进程 GOMAXPROCS=4；五个应用进程及独立 NATS、etcd、Redis。依赖容器以本轮随机 label 标记，按归属检查后清理，没有操作共享开发 Redis。镜像、端点、版本与原始资源样本见归档 manifest。

## 五轮结果

以下为五轮中位数；吞吐按实际完成耗时计算，1000/s 档即使全部完成，其观测吞吐也可能略低于 1000/s。

| 档位 | 正式 hybrid 完成/s | 同轮 ProtoActor 完成/s | 正式 hybrid P99 |
| --- | ---: | ---: | ---: |
| 128 B 闭环 | 5048.39 | 6745.78 | 36.38ms |
| 1024 B 闭环 | 4153.21 | 6240.77 | 41.30ms |
| 1024 B，1000/s | 998.42 | 见机器记录 | 计划到达 33.84ms |
| 1024 B，3000/s | 2964.03 | 见机器记录 | 计划到达 50.10ms |

正式入口 1000/s 档五窗各完成 4000 次，共 **20,000 次，零错误、零未发送**；计划到达 P99 最差 **38.591409ms**，低于 100ms。40 个主验收窗口的 GD 回读均一致。独立脚本从压缩原始延迟样本重算 P99、核对计数、窗口矩阵与回读记录，结果通过。

3000/s 档正式 hybrid 未发送数中位数 123、范围 66～371；ProtoActor 为 23、范围 0～264。这里的未发送是压测发起端没有按窗口计划发出的请求，不能算服务已完成，也不能据此接受 3000/s 容量。

1024 B 闭环正式吞吐相对冻结 hybrid 4642.28/s **下降 10.53%**；相对本轮 ProtoActor 中位数 **低约 33.45%**。同期 ProtoActor 相对冻结值反而上升约 10.24%，因此不能把所有差异归因于整体环境变慢。冻结基线 `2026-09-13-uplink.json` 保持不变。

## 同二进制补充诊断

因超过 10% 的历史偏移，额外运行三组 trial hybrid / formal hybrid 对照，使用同一二进制、相同 1024 B 闭环负载，交替先后顺序；仅切换装配，未引入另一种传输。

| 轮次 | trial 完成/s | formal 完成/s | formal 相对变化 |
| --- | ---: | ---: | ---: |
| 1 | 7076.90 | 5900.27 | -16.63% |
| 2 | 4401.09 | 4485.71 | +1.92% |
| 3 | 4369.44 | 4389.08 | +0.45% |

六窗零错误、零未发送，GD 回读均通过。差异方向不一致，未稳定复现正式装配额外导致超过 10% 的损失；这不是等价性证明。正式装配每完成请求分摊的分配量增量中位数约 282 B、5.67 次，相比约 80KB 的整条链路较小。该指标包含采样区间内多个 Go 进程与后台活动，不是单函数的精确 allocs/op。本轮保留职责边界，不为这点增量再做微优化。

## 验证与审查

- 隔离 overlay 二进制构建、完成判定与生成事务测试、修复后 smoke 通过。
- 初次 smoke 因夹具显式发布 `127.0.0.1:0` 导致 EOF；删除显式 InstanceEndpoint，让 RPC Host 发布实际监听地址后通过。失败日志保留，没有修改生产代码规避失败。
- 本轮 `go test ./... -count=1` 通过，设置了真实本机 NATS/etcd 环境变量。
- 本轮 `go test -race ./internal/framework ./internal/actorhost ./internal/actorrpc -count=1` 通过；不扩大为全仓 race 通过。
- 后置 ponytail-review：Lean already. Ship. 新增代码局限于必须的公开契约适配与测试装配，没有新增生产抽象。

## 证据与复现

机器汇总：[正式入口记录](baselines/2026-09-14-formal-actor-uplink.json)。原始证据位于 `/Users/a47/workspace/tx-artifacts/nats-migration-eef61a5/formal-uplink-20260914/evidence.tar.gz`，SHA256、大小和二进制 hash 记录在机器汇总及同目录 manifest.json。

归档包含主验收与诊断的 runner、镜像 manifest、压缩原始样本、结果/回读 JSONL、完整日志、独立核验脚本、构建二进制以及本次夹具源码快照。主验收统计必须筛选 `phase == final`，不能混入 smoke。

复现时在上述提交上恢复 source 中的三个夹具文件，用 build.py 重建 linux/amd64 二进制；将归档 runner 的 ROOT/BINARY 路径改为新的独立目录和二进制位置后运行主 run.py final。诊断使用诊断目录自己的 run.py。先阅读 runner，确认仅使用它创建的随机标签依赖。verify.py 与各自 summarize.py 用于复核原始结果；不执行 compare.py 的历史主程序或其他历史矩阵。

## 下一项

正式 I2 临时预算验收完成。I3/I4 不整体勾完：下一项评估并接入正式 Kind 的容量声明、逐世界估算及生命周期状态探测 SPI；区分未加载、加载中、拒绝加载和不存在。world 子协程工作如引入，仍需明确实际完成与排水边界。本轮未进入 JetStream，没有提交 Git。
