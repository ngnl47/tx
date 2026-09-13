# I3a：正式交接准备与迁移前基线

日期：2026-09-13。起点 tx=`fe8656f`，六工程工作区均干净；I1/I2 已提交。本轮只改 tx，未执行 Git commit。用户接受 I2 临时预算后开始 I3；本节点完成正式框架交接准备，尚未替换 Actor Runtime 或迁移两个示例。

## 性能约束与固定基线

用户确认按既定分工接线：Agent 上行直连 gRPC，Actor 间 RPC 使用 NATS。上行也经 NATS 已退出候选，后续不再运行。允许处理迁移中发现的直接相关冗余，不为删行数或细小波动继续微优化。

已接受下一阶段临时准入：1 KiB padding，同口径按 1000 请求/s 投递且全部完成、计划到达 P99 ≤100ms、零错误与未发送、生成 GD 回读一致。它不是生产 SLA，也没有把此前相对 ProtoActor 的 18% 吞吐差抹平。重点比较去掉 ProtoActor 后的完整上下行完成链路，不能只比较 Actor 回显。

[可机读基线](baselines/2026-09-13-uplink.json) 保存 ProtoActor/hybrid 的八个负载点、五轮原始汇总、测量源码与二进制 hash、归档校验值及临时预算。以下是 I2 的历史中位数，不是本轮重新压测：

| 1 KiB 完整链路 | 完成/s | 调用 P99 ms | 1000/s 到达的计划 P99 ms |
| --- | ---: | ---: | ---: |
| ProtoActor | 5661 | 29.42 | 34.24 |
| hybrid | 4642 | 38.24 | 31.41 |

原始材料已完整归档到 `/Users/a47/workspace/tx-artifacts/nats-migration-fe8656f/i2-baseline.tar.gz`，451 个文件、108789600 字节，SHA-256：`8aff00fe55f962561c417bfa6cb5cb859426bb5a96aad030aa7c7d1b94c42bcc`。归档包括已退出的全 NATS 组及失败探索材料；后续比较只取 `phase=final` 的 proto/hybrid。压测二进制和大样本留在独立 artifact 目录，不进入生产构建或 Git。

后续保持固定基线，同时在同环境复跑保留的 ProtoActor 基线，区分硬件波动与迁移变化。超过 10% 的完成吞吐/P99 偏移仍需定位说明；已接受预算允许继续接线，不允许静默重置基线、放大超时或删去 ACK/下行核验。网络计数保持独立构建。没有改变高频消息路径，本节点不重复吞吐矩阵，也不宣称性能提升或零回退。

## 正式实现

- `Server.PrepareHandoff(ctx, expectedInstanceID)` 检查本次启动身份，复用原关闭前半段：封闭准入、等待已接受工作、停止服务后台任务、逆序 CMod Drain。准备与 Stop 共用串行边界；组件成功进度仍由现有 `component.Host` 维护。
- 准备成功保留共享 RPC、控制面、存储绑定和租约；失败保留资源，重试从未完成阶段继续。取消等待或准备期间身份失效不返回成功许可。它不做槽位 CAS、自动接管或业务重投。
- 移除 `rpcCMod.Drain` 对共享 Host 的提前关闭。首次回归暴露根任务等待与 gRPC Serve 互等，堆栈证明原 Host 不响应根取消；沿用控制 Host 的方式，在 Serve 外绑定一次 `context.AfterFunc`。该逻辑按 Host 启停执行，不进入逐消息路径。
- 既有控制端口增加可选 `HandoffService`；保留 `LifecycleController` SPI，不要求其他独立控制宿主补实现。Bee/Ant Server 暴露同一能力，`txctl prepare-handoff --instance` 校验成功响应中的源身份。Proto 文件经 `go generate ./modules/control` 更新生成物。
- `StateDrained` 仍仅表示入口已接受工作排空，不能等同于最终写回成功。Handoff 是独立操作，以其成功响应作为源方准备结果；后续仍需显式晋升完整旧 assignment，并在所有计划槽位处理完后 Stop。

示例：源实例 A 承载多个槽位，控制端先对 A 调用一次准备；首次写回失败不执行晋升。恢复后重新准备成功，逐槽 CAS 到 B/C 时复用 A 的收尾结果，最后 Stop A。卸载回调与新实体宿主的接线尚在后续 I3 中，不宣称当前已经完成 NATS 生命周期迁移。

```bash
go run ./cmd/txctl --endpoint 127.0.0.1:19001 --timeout 30s prepare-handoff --instance txgame-1-12345
```

上例 ID 必须换成当前真实源启动身份；输出 `prepared=true runtimeInstance=...` 后才可进入现有逐槽晋升流程。

## 本轮验证

材料位于 `/private/tmp/tx-handoff-i3a-20260913`：

| 验证 | pass | skip | fail |
| --- | ---: | ---: | ---: |
| tx 全量，显式启用 etcd 与 actortrial NATS/Redis | 1221 | 11 | 0 |
| 框架交接、Stop、fatal 定向 race，三次 | 33 | 0 | 0 |
| txgame/server 全量 | 236 | 3 | 0 |
| txgameslg/server 全量 | 331 | 5 | 0 |

计数包含子测试。两个示例串行运行普通全量测试；未运行其 cluster-e2e，依赖未启用的测试列为 skip，不冒充真实示例验收。客户端、生成器和业务示例代码未变。

定向验证包含：旧身份在收尾前拒绝、后台任务先于写回退出、写回失败保留资源、重复准备和最终 Stop 不重复冲刷、准备与 Stop 并发、调用方取消但写回已完成、准备期间 fatal 身份丢失、真实 etcd 原租约保留至 Stop、控制响应身份检查。四次正式租约测试的专属前缀成功后按 ownership 条件删除，并在测试内读回确认无残留；不清理其他历史失败范围。

`red.log`、`control-red.log`、`cli-red-v2.log` 保存缺少能力时的失败；`green.log` 保存首次关闭阻塞的 SIGQUIT 堆栈，随后修复通过。初次沙箱禁止监听/缓存访问的失败单独记录，不当作业务失败。最终结果见 `full.jsonl`、`race.jsonl`、`txgame.jsonl`、`txgameslg.jsonl` 与 `test-summary.json`。

累计 Go 差异：运行代码 +231/-47（净增184），测试 +298，控制协议生成物 +234/-33（净增201），总净增683行。主要是新增交接能力及验证，不属于瘦身交付；基线 JSON 单独保存证据。

必要验证后完成一次 ponytail-review：**Lean already. Ship.** 保留跨层控制 SPI、两个独立门面和关键失败验证；未增加第二份完成表或写回管理器，未进行全库 audit。

## 下一项

继续 I3 的权威槽位访问、实体宿主和 API/SPI 迁入，再衔接 NATS 与直连入口。核对现有 PID/Props/Context/ActorSystem 的实际使用，保留必要高级能力；业务只提供身份和 factory。正式收发替换后按固定基线复测完整上下行，再进入独立示例的真实生成 GD、Redis/Mongo/Fusion 与多 Session 验收。无需重新确认已接受的临时预算。
