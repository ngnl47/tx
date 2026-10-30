# Actor 放置本地验收记录

本页保留实现批次的历史证据，不作为施工计划或当前构建的容量结论。现行契约见 [放置能力](actor-placement.md)。

## 初次接入验收

- 已完成命名策略声明、初始化、框架查询缓存、加载前校验接线与 SLG 专用适配移除。
- 已完成本地六进程 64 连接短测，111,396 次全部完成，零失败/超时，持久化与排水通过；未跑云测试。
- 缓存命中基准：2,000/10,000 身份、P2/P4，串行中位数 102～110ns，并行 126～171ns，全部零分配。
- ponytail-review 独立审查发现两个超时缺口，已补 RED→GREEN：冷查询等待必须观察共享 3 秒期限；关闭等待遵守调用方/配置期限，超时返回 errStopDeferred，保留实际工作及资源供重试。另删除 Agent 的无效返回包装。
- 同身份首调用取消终止共享查询是已记录取舍，不自动重投；后加入者可独立取消。框架可限时退出等待，不能强制终止不响应取消的业务 I/O。
- tx、txgameslg/server 全量回归通过；txgame/server 兼容回归通过。tx 的 framework/actorplacement/actorhost/ant/bee 与 SLG 归属、目录、建角、装配相关包的 race 回归通过，本地 Mongo/etcd 依赖已启用。
- 文件完整性检查恢复了被同名新测试覆盖的既有 Actor 放置与退役测试；新增策略测试单独存放，actorhost 普通及 race 回归再次通过。
- 多进程短测与旧数据未处于严格一致的负载条件，不能据此量化吞吐增减；当前证据支持缓存命中维持百纳秒、零分配开销及完整上下行链路正常。

新增 `TestPlacementGroupHandoffKeepsIdentityCache` 使用真实本地 NATS/etcd，验证三个类型共享一个槽位、本地与跨实例 Call，以及交接后复用 Identity→SlotID 缓存访问新 owner。

2026-10-27 本地验证：新同槽交接测试及既有归属写回/身份丢失测试通过，放置查询、缓存与交接 race 通过。SLG 真实依赖下 Agent→Role→MsgQ 往返及 Legion/ServerActor 调用通过。10,000 身份、P2 缓存命中约 118～120 ns/op（串行）、148 ns/op（并行），全部零分配；未跑新的集群吞吐测试。
