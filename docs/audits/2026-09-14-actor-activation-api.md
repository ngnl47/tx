# Actor 数量配额与正式状态入口

累计未提交变更基于 tx `679e9c4`、txgame `c305eab`、txgameslg `2b99d1f`。txgen、txclient、txclientslg 无本轮修改。本文以最终数量方案替代前一轮未提交的字节估算方案。

## 原因与职责

按 Actor/GD 估算字节无法可靠代表宿主资源，且 Actor 卸载不等于 GD 缓存释放。改为部署配置数量上限，tx 负责并发准入与持久归属占位，业务负责估算合理数量和独立实体映射。没有增加资源调度器、第二份权威计数表或 K8s controller。

`actor.Config.Capacity{Maximum, Scope, Initializations}` 对实例全部本地 Kind 生效，零上限不限制：

- `activation`：初始化、运行、清理都占名额，清理完成释放。适合临时房间；默认分区只共享归属，不等于实体数量，已归属分区满载不会自动迁出其中一个实体。
- `assignment`：独立世界归属占名额，Actor 卸载、初始化失败和 GD 淘汰都不释放；同世界重载复用名额，成功交接迁出释放源名额。要求一实体一槽位的显式业务映射；启动校验映射存在，映射唯一性仍是业务契约。
- `Initializations` 单独限制并发加载数，不给初始化添加统一 deadline。

world 与 seasonWorld 共用实例上限，不预绑定 worldId，不恢复 logical-slot。GD 缓存与写回生命周期独立，大世界继续直接 Mongo，不引入 Fusion。数量配额不保证内存/CPU 不超载。

## 使用示例

独立 slgworld 部署的 txgameslg 配置：

```yaml
runtime:
  cmods:
    actor:
      capacity:
        maximum: 1
        scope: assignment
        initializations: 1
```

含义是本实例接纳任意首个世界；需要下一个世界时选择有空位的实例。临时房间可以配置 `maximum: 1000, scope: activation`，1000 只是业务需要压测的示例。txgame 对应路径为 `runtime.actor.capacity`。现有合并调试配置不默认启用大世界配额。

公开 `actor.Runtime` 保留 Messenger，并接入 Probe / EnsureActivation / RetryActivation；业务仅发消息仍可依赖 Caller 或 Messenger。Bee、Ant 同步接线，没有暴露连接关闭权限。

```go
state, err := runtime.CModActor().EnsureActivation(ctx,
    actor.Identity{Kind: "world", Key: "i:490001"})
// 先处理 err；Initializing 可提示场景准备中，再用 Probe 查询。
```

Probe 只读权威归属，不分配、不激活，也不排队等待业务初始化；Unloaded 不等于业务世界不存在。Failed 保留失败分类，显式 Retry 才重新执行已失败的加载；失败缓存有界，不承担永久存在性登记。容量不足和网络错误均不能解释为世界不存在。

## 原子边界

本地数量检查与激活登记共用宿主锁，不增加消息热路径采集。默认 EtcdSlotStore 依据已发布的实例上限与既有 assignment 计数：有限归属的创建和晋升读取目录快照，以范围 ModRevision 比较加目标键 CAS 原子写入。交接在源排水前预检目标，最终晋升再次复核。

范围比较允许删除：删除只会减少占位，不会造成计数超额；不能将这个底层原语解释为任意用途的完整快照相等检查。明确比较失败才重读，网络写入结果未知时直接返回，不自动改派或重发业务。自定义 SlotStore 需要自行满足相同原子准入契约。

有限归属冷分配扫描目录，成本随 namespace 规模增长；普通路由不扫描。当前先复用已有权威数据，是否增加事务索引需规模证据。没有实现世界注销、自动扩 Pod、运行中单实体热迁移或 CPU/内存采集。

## 本轮验证

- tx：真实 NATS/etcd 环境下 `go test ./... -count=1` 通过。
- tx：`go test -race ./internal/framework ./internal/actorhost ./internal/actorrpc ./modules/discovery ./etcd -count=1` 通过。
- txgame/server、txgameslg/server：各自全量 `go test ./... -count=1` 通过。
- 正式宿主集成覆盖两种配额、初始化期间探测、重复 Ensure、失败分类与显式 Retry；稳定归属在卸载后继续占位、新实例承载新世界、原世界复用归属重载。
- 真实 etcd 下 16 个世界并发争抢一个名额恰好一个成功；满载目标不触发源排水，晋升不能绕过配额，迁出释放源名额。
- 保留初始化无统一超时、排水超时不提前释放和并发激活等可靠性回归；两个示例验证 YAML 配额读取。txgameslg 同时清理两处已无调用的 Messenger 测试桩覆盖，保留实际使用的直连桩。
- 本轮未运行完整 cluster-e2e、客户端构建或 I2 全链路压测，不沿用历史结果冒充当前验证。
- 后置 ponytail-review：Lean already. Ship. 保留必要 SPI 与并发可靠性测试，无额外删除建议。

## 局部性能

Darwin/amd64、Intel i9-9880H、GOMAXPROCS=4，每项 300ms × 5，中位数。先以 HEAD `679e9c4` 的全部已跟踪 actorhost Go 文件构建 overlay 跑修改前，再串行跑工作区。旧 limited 是字节限制，新 limited 是数量限制，二者均仅允许一个激活；它们不是完全相同的配置语义。

| 宿主基准 | 修改前 ns/op | 修改后 ns/op | 耗时变化 | 分配 |
| --- | ---: | ---: | ---: | --- |
| Probe | 51.22 | 41.51 | -18.96% | 均为 0B / 0次 |
| 激活—处理—卸载，无上限 | 5973 | 6141 | +2.81% | 均为 1376B / 18次 |
| 激活—处理—卸载，有上限 | 6132 | 6361 | +3.73% | 均为 1376B / 18次 |

周期耗时中位数小幅增加，没有统计显著性结论，不宣称整体性能提升。此局部基准不覆盖公开控制入口网络开销和新 etcd 快照路径，不替代完整链路测量；冻结的 I2 基线不变。

原始日志、旧源码 overlay、汇总与 SHA256 位于 `/Users/a47/workspace/tx-artifacts/actor-count-679e9c4`。overlay 使用本机归档绝对路径，异机需调整。

## 下一项

对接两个示例的真实部署编排，验收配置驱动的空闲世界实例分配、满载拒绝与正常滚动交接；结合 I3/I4 清单评估剩余缺口。自动扩容和世界注销尚未纳入实施范围，JetStream 不在本轮范围。
