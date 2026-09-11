# NATS 与最小本地 Actor 运行时试验实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 串行执行，不使用子代理，不自动提交。

**Goal:** 在 feature 分支验证 NATS Core + 最小本地实体运行时能否替代 ProtoActor，并以 JetStream 衔接可靠任务；以正确性、维护复杂度和性能决定是否推广。

**Architecture:** 先隔离原型，再逐步接入两个独立 game 示例。本地实体管理负责身份、激活、邮箱及生命周期；NATS 负责跨进程通信，JetStream 负责任务存储与消费进度；etcd 暂留实例身份、租约和槽位交接。原型可被否决，不预设必须迁移成功。

**Tech Stack:** 当前仓库锁定的 Go 工具链、NATS 官方 Go 客户端、nats-server、现有 etcd/Redis/Mongo；执行前固定版本和镜像摘要，不用浮动 latest 做结果依据。

**Spec:** [生命周期与可靠任务草案](../specs/2026-09-11-actor-lifecycle-reliable-task-design.md)。本计划补充“试验优先且不预设保留 ProtoActor”的最新方向；不代表已有运行时已变更。

## 当前状态与分支

2026-09-12 检查：六工程均在主分支。tx/main=638d04a4525cfcec15327ac07bfdbaba4a2d59da；txgame/master=b611ebb80af3908d62660d9dd551f434265a5c12；txgameslg/main=0b465210ef878cbc39039a36a228bd7b1dfff085。tx 已有未跟踪的生命周期草案，本轮另更新总计划并新增本文；其余五工程干净。实施起点必须重新核对，以上仅为计划快照。

统一分支名：`feature/nats-actor-runtime`。

| 工程 | 是否先建分支 | 工作边界 |
|---|---|---|
| tx | 是，从当前 main | 原型、本地运行时、通信、JetStream、框架装配、文档及测试 |
| txgame | 是，从当前 master | 无状态角色示例、服务装配、真实集群回归 |
| txgameslg | 是，从当前 main | 世界固定归属、跨世界调用、写回及生命周期示例 |
| txgen | 否，按需 | 仅实际生成契约或模板依赖改变时加入；不优化生成器性能 |
| txclient | 否，按需 | 首先作为现有外部协议验收工具；协议或测试驱动必须变更时加入 |
| txclientslg | 否，按需 | 同上；生成链仍由 txgameslg/gen 管理 |

分支尚未创建。三个同名分支是独立 Git 仓库分支，不会自动固定跨仓库版本。开始时记录所有 HEAD、工作区差异、Go replace/模块解析路径和客户端版本；测试报告必须说明实际使用哪份 tx。已有未提交文档带入 tx 分支，不 reset、clean 或自动提交。只在分支不存在且工作区仍符合记录时执行：

```bash
git -C /Users/a47/workspace/tx switch -c feature/nats-actor-runtime
git -C /Users/a47/workspace/txgame switch -c feature/nats-actor-runtime
git -C /Users/a47/workspace/txgameslg switch -c feature/nats-actor-runtime
```

## 全局边界

- 本轮授权是更新计划；执行从分支准备开始，当前不安装依赖、不启动原型、不替换生产代码。
- 不保留历史 API 兼容负担，但保留实际需要的高级 SPI；不能把底层组装和空值检查重新推给 game。
- 五个事件名保持 OnInitAfter、OnActivateBefore、OnActivateAfter、OnDeactivateBefore、OnDeactivateAfter。卸载前 RPC 尽力执行，错误记 error 后继续卸载；否决在发起卸载前判断。回调有界且协作取消，不并发释放仍被回调使用的对象。
- RPC 超时不代表未执行，不自动重发；区分投递接受、目标取出与业务结果。保留排水的真实工作计数及必要持久化安全收尾。
- 业务明确“无法处理并结束”可以 ACK；永久加载拒绝进入死信，临时加载失败重试。ACK 不等待 GD 刷盘；成功后结果/ACK 丢失只记录可观察错误，不建完成表。
- 默认并发消费不保证业务顺序；可保留产品有序入口，声明保证时必须限定范围和失败边界。
- etcd、GD、Procedure、Redis/Mongo/Fusion 和外部 BON/Frame/Seq/Ack 不随第一阶段替换。消除 etcd 是独立后续决策。
- 不预建通用 Actor 平台、复杂监督树、透明迁移、第二套归属控制面或永久双后端兼容层。
- 所有采集、故障注入与临时对照二进制隔离；失败证据保留。每批验证后执行一次 ponytail-review，仅明确大节点才执行 audit。

## T0：分支、事实清单与共同基线

涉及：三个仓库根、tx/modules/actor、tx/modules/discovery、tx/internal/framework、两示例服务及集群入口；新增证据记录 `tx/docs/audits/2026-09-12-nats-actor-runtime-trial.md`（开始采集时创建）。

- [ ] 创建上述分支并保留未提交材料；记录模块解析实际路径，避免新示例误连主线 tx。
- [ ] 从现行代码提取必须保持的行为：虚拟/固定寻址、并发激活、Tell/TellAccepted/Call、错误码、排水、实例身份和世界归属；标记目前尚不存在的生命周期能力。
- [ ] 固定原实现源码快照或只读 checkout，先编译基线；原型与基线使用相同负载，不直接比较不同机器的历史报告。
- [ ] 记录版本、硬件、GOMAXPROCS、消息大小、并发、目标分布、网络/TLS、采样时长与失败计数。NATS 数据目录使用独立临时目录，结束只清理本次拥有的进程和资源。

交付：可复现的对照起点及行为矩阵，不产生性能改善结论。

## T1：仅在 tx 内验证最小本地实体运行时

拟新增：`tx/internal/actortrial/runtime.go`、`runtime_test.go`；这些是试验文件，不是最终公共 API。参考 `modules/actor/{kind,mailbox,lifecycle,fixed_host,passivation}.go`；不复制第三方实现。

- [ ] 先写关键状态测试：同身份并发首请求只创建一次、不同实体不互相串行阻塞、准入永久/暂时拒绝可区分、激活失败清理。
- [ ] 实现限定的实体映射、有界队列与串行调度；明确锁与回调不互相等待，同步回调不得自调用等待同一邮箱。
- [ ] 加入五个生命周期事件、关闸后拒绝、已准入请求排空、重复卸载合并、旧定时器隔离。卸载前错误不得取消卸载。
- [ ] 用可控时间完成 60s 战斗结束、65s 卸载与迟到 Buff 的场景；不用真实睡眠替代时序控制。
- [ ] 实施最小 panic 清理和等待者错误返回，不直接复制通用监督树；验证没有半激活实例、孤立等待者或明显 goroutine 泄漏。

验证入口：`go test ./internal/actortrial -count=1`，随后单独 `go test -race ./internal/actortrial -count=1`。预期相关状态断言全部通过；原 ProtoActor 的已知 race 不得拿来解释新运行时失败。

退出门槛：若为覆盖现行必需行为必须重建复杂 Actor 框架，则记录原因，转评保留本地 ProtoActor 的备选，不强行推进自建。

## T2：NATS Core RPC 与归属集成

拟新增：`tx/internal/actortrial/nats.go`、`nats_test.go`；修改 tx/go.mod、go.sum 仅用于固定依赖。参考现有 `modules/actor/{delivery,future}.go` 和 discovery 接口。

- [ ] 先验证真实 nats-server 下的双进程调用、目标不在线、连接中断、超时后业务仍可能完成、迟到响应清理。
- [ ] 每运行实例设置入口 Subject，载荷携带稳定实体身份和命令。固定世界沿用 SlotResolver；角色虚拟实体的 owner 选择与重建先明确沿用的规则，不能以普通 Queue Group 随机分配冒充唯一归属。
- [ ] 把取出确认与业务完成分开；本地调用走相同实体队列但不强制绕 NATS。NATS handler 入队后返回不代表排水已经完成。
- [ ] 处理永久准入拒绝、邮箱满和 draining 的显式错误；业务结果编码沿用现行载荷契约，普通业务不直接操作 NATS 连接。
- [ ] 验证关闸、在途工作、卸载前 RPC、最终写回与连接关闭次序。复用现行工作计数，不增加第二套全局屏障。

验证：T1 命令加双进程集成入口；真实依赖缺失必须显式失败。测试启动器和配置仅在试验目录，不能依靠开发机已有 NATS 掩盖前置条件。

## T3：共同负载性能及是否进入示例的决策

拟新增：`tx/internal/actortrial/benchmark_test.go`；采集脚本、旧源码和二进制放 `/private/tmp/tx-nats-actor-trial-*`，报告放 T0 证据文档。

- [ ] 同进程、同机跨进程及独立机器分别标记；暂时无多机环境就不声称跨主机容量。
- [ ] 请求/响应各采用 128B、1KiB、8KiB；并发 1/16/64/256，单实体热点与 1000 实体分散负载分别测试。先预热，再顺序交替运行至少五轮；按饱和结果收窄后续采样，不无目的扩矩阵。
- [ ] 同时统计成功完成 RPC/s、P50/P99、超时/拒绝、CPU、RSS、Go 分配及残留 goroutine；累计所有相关进程资源。不能用 NATS 单向发布吞吐对比完整 Actor RPC。
- [ ] 用固定到达率补测排队和过载；吞吐按完成而非发送计。性能原型不包含持久化时必须明确，JetStream 单独测试。
- [ ] 列出最终可删除代码、必须新增代码、保留依赖与高级 SPI 映射。试验夹具不算生产瘦身；API 简洁度不能仅按行数判断。

门槛：正确性零未解释失败；共同负载中 P99 或完成吞吐稳定退化超过 10% 时先定位并停止默认推广（这是试验筛选线，不是生产 SLA）；较小变化报告波动，不宣称零回退。无明显性能损失仍需确认维护复杂度收益。备选是本地 ProtoActor + NATS，或保留原架构；在评审结论前不删除正式 Runtime。

## T4：接入两个独立示例并评估正式 API

修改范围：tx/modules/actor、tx/internal/framework/cmod_actor.go 及相关能力装配；txgame/server/services/game/service.go、services/internal/actorcmd；txgameslg/server/services/{slggame,slgworld}/service.go、services/internal/actorcmd、server/internal/placement；两个 server/go.mod 与运行配置、README、集群验收入口按实际依赖更新。

- [ ] T3 通过后才整理正式包位置，收敛试验 API；必要高级能力给 game 使用，不把通用运行时细节暴露到示例。
- [ ] 先接 txgame 的角色请求和多 Session，再接 txgameslg 的固定世界与跨世界侦查；保留独立示例，不互相 import。
- [ ] 在现有 cmd/cluster-e2e 中验证真实网络、实例变化、关闸、槽位代次、重建、GD 写回及多会话下行。沿用工程固定 DB/database/prefix 和 ownership 清理约束，两个套件串行运行。
- [ ] 外部协议不变时直接使用原客户端；若必须修改生成输入，先为相关生成器/客户端建同名分支，再经原生成入口生成，禁止手改产物。
- [ ] 在 tx 与两个 server 模块运行 `go test ./... -count=1`；两个客户端运行 `npm test`、`npm run build` 并驱动真实集群。集成命令参数从各工程当时 README 核对，不盲用历史环境。

交付：三仓版本矩阵、实际配置入口、功能与性能报告；失败不伪装为既有行为等价。不需要永久双后端开关，原实现由基线快照提供对照。

## T5：JetStream 可靠任务闭环

位置：先在 `tx/internal/actortrial/tasks.go`、`tasks_test.go` 验证，采用后再收敛正式适配目录；示例以现有命令或试验内部消息构造任务，不为演示改变客户端协议。

- [ ] 文件流、共享 durable pull consumer、显式确认；业务 Handler 与 RPC 复用分派，投递和确认归 MQ 适配。
- [ ] 测试执行后确认、业务明确结束、永久加载拒绝转死信、临时错误恢复、进程重启接管和排水停止领取。
- [ ] 死信先持久转存确认再结束原任务；转存失败保留原消息。捕获 MaxDeliver 通知并持久保存，验证处理端离线与残留源消息恢复。不把 Term 或重试耗尽当成已自动转存。
- [ ] 不建成功完成表；结果或 ACK 丢失仅记录可观察 error，接受重复窗口。慢任务续期与背压按真实版本验证。
- [ ] 用确定的磁盘同步与副本配置单独测任务链路；单机多容器不宣称高可用。对同一次激活的重复、旧计时器、过期战斗作明确分类断言。

交付：符合既定边界的任务演示与失败恢复证据。JetStream 不通过不应迫使采用 Core RPC；通信和 MQ 可分别决定。

## T6：五项整合收益逐项结算，不扩大本轮范围

- [ ] 汇总通信配套、请求等待逻辑、发现拓扑和非 Actor 接入的实际收益；没有删除或验证的收益标为未实现。
- [ ] msgq 仅在前述阶段有收益后单独立项：对等比较现有 PubSession/PubRole→邮箱→Fetch/Ack，不将 Core pub/sub 当完整邮箱。Seq/Ack、连接隔离、Topic owner 范围必须保持；无收益就不替换传输。
- [ ] etcd 替代只给独立评估输入：WorkSlot/SessionID、租约失效、持久槽位 CAS、快照 Watch、旧代次隔离。不在本试验中自动实施 KV 迁移。
- [ ] 确认采用后才删除被替代 ProtoActor 代码和依赖；重新核验高级 SPI、两示例和客户端。未采用则保留报告，不合并原型，旧分支和未提交工作不得自动删除。

最终交付为“采用 / 局部采用 / 不采用”及证据，不以完成代码量作为成功标准。每轮按累计未提交变更给中文 Angular Commit Message，提交由用户操作。
