# Actor 放置本地验收记录

本页保留实现批次的历史证据，不作为施工计划或当前构建的容量结论。现行契约见 [放置能力](actor-placement.md)。

## 初次接入验收

- 已完成命名策略声明、初始化、框架查询缓存、加载前校验接线与 SLG 专用适配移除。
- 已完成本地六进程 64 连接短测，111,396 次全部完成，零失败/超时，持久化与排水通过；未跑云测试。
- 缓存命中基准：2,000/10,000 身份、P2/P4，串行中位数 102～110ns，并行 126～171ns，全部零分配。
- ponytail-review 独立审查发现两个超时缺口，已补 RED→GREEN：冷查询等待必须观察共享 3 秒期限；关闭等待遵守调用方/配置期限，超时返回 errStopDeferred，保留实际工作及资源供重试。另删除 Agent 的无效返回包装。
- 当时采用同身份首调用取消终止共享查询、后加入者独立取消；2026-10-30 已改为所有等待者独立取消，现行契约见 [放置能力](actor-placement.md)。框架可限时退出等待，不能强制终止不响应取消的业务 I/O。
- tx、txgameslg/server 全量回归通过；txgame/server 兼容回归通过。tx 的 framework/actorplacement/actorhost/ant/bee 与 SLG 归属、目录、建角、装配相关包的 race 回归通过，本地 Mongo/etcd 依赖已启用。
- 文件完整性检查恢复了被同名新测试覆盖的既有 Actor 放置与退役测试；新增策略测试单独存放，actorhost 普通及 race 回归再次通过。
- 多进程短测与旧数据未处于严格一致的负载条件，不能据此量化吞吐增减；当前证据支持缓存命中维持百纳秒、零分配开销及完整上下行链路正常。

新增 `TestPlacementGroupHandoffKeepsIdentityCache` 使用真实本地 NATS/etcd，验证三个类型共享一个槽位、本地与跨实例 Call，以及交接后复用 Identity→SlotID 缓存访问新 owner。

2026-10-27 本地验证：新同槽交接测试及既有归属写回/身份丢失测试通过，放置查询、缓存与交接 race 通过。SLG 真实依赖下 Agent→Role→MsgQ 往返及 Legion/ServerActor 调用通过。10,000 身份、P2 缓存命中约 118～120 ns/op（串行）、148 ns/op（并行），全部零分配；未跑新的集群吞吐测试。

## 2026-10-30 生命周期与冷查询边界修复

本轮基于 tx `9280ef6` 的工作区修复，未提交；txgen、txgame、txgameslg 不修改代码。

- 共享查询由框架生命周期和独立三秒期限管理，首个请求取消只结束自身等待；后续等待者可继续共享结果，成功后缓存。
- 不同 Identity 的实际在途冷查询超过 1024 时继续接纳，每个缓存最多每分钟 Warn 一次。测试同时挂起 1026 个不同身份，全部正常完成且只告警一次；期间热缓存仍可访问。它是观察阈值，不限制排行榜数量、发奖数量或在线人数，也不是内存保护上限。
- 查询超时仍登记实际工作，解析器真正退出后才释放；不响应取消的解析器仍可能累积工作。没有新增容量拒绝错误、排队层或自动重投。
- 服务后台任务先全部取消，再按调用方/配置期限等待；Stop 与 PrepareHandoff 超时均保留资源，实际退出后重试完成。回归覆盖调用方期限、配置期限、重复关闭和资源释放次数；本项不表示任意业务 Stop 回调都能被强制中断。
- `txruntime.Procedure(ctx)` 使用与 CModProcedure 相同的受限执行视图；Call/Submit 保持可用，业务无法通过该视图取得事务运行时的 Drain 所有权。

本地回归：tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 通过；tx 的 `internal/framework`、`internal/actorplacement`、`lifecycle`、`modules/txn`、`bee`、`ant` 的 `-race` 回归通过。未设置外部依赖验收环境变量，需这些变量的用例按原规则跳过；没有运行真实多进程集群或云压测。

缓存命中对照在本机 macOS amd64、Go 1.27.0、P4 下执行，前后各五次、每次 300ms；下表为中位数，全部为 **0 B/op、0 allocs/op**。基准单独运行，不与回归并行。

| 身份数 | 调用方式 | 修复前 ns/op | 修复后 ns/op | 变化 |
| --- | --- | ---: | ---: | ---: |
| 2,000 | 串行 | 98.26 | 95.92 | -2.4% |
| 2,000 | 并行 | 139.6 | 138.4 | -0.9% |
| 10,000 | 串行 | 98.05 | 99.06 | +1.0% |
| 10,000 | 并行 | 145.2 | 148.1 | +2.0% |

```sh
go test ./internal/framework -run '^$' -bench '^BenchmarkActorPlacementHit$' \
  -benchmem -benchtime=300ms -count=5 -cpu=4
```

本组结果未观察到明显缓存命中回退，不证明冷查询容量或完整上下行吞吐。收尾差异审查未发现新增阻塞问题；实际解析器耗时及生产内存容量仍需对应负载证据。

## 2026-10-31 条件路由修正与共享冷刷新

本批基于 tx `6dedf9a`，分支 `feature/actor-route-refresh`；只修改 tx，保留既有文档改动，不提交。两组应用只运行接入回归。

### 已实现与验证边界

- 发送保留观察到的 SlotID/InstanceID/Generation，失败只使同一份旧归属失效；迟到拒绝不撤销新地址。正常命中仍不回源，未知投递结果不自动重发。
- 每个服务池/槽位共享独立三秒查询，等待者独立取消；实际查询退出前保留登记。Watch 与主动读按 etcd revision 合并，目录不可用仍拒绝服务，单键查询不能重新开放目录。
- gRPC 单向与 Core NATS 单向的框架拒绝通过既有 NATS 实例控制入口通知，连接和订阅数量不增加，没有逐消息成功 ACK；业务正文继续走既定传输。内部信封增加来源和归属字段，不改变业务 Delivery。
- EntityRouter 依赖最小 ActivationControl，具体 NATS 发送由 Messenger 绑定。正常关闭释放、失联接管、Watch 增量更新不在本批。

新增失败回归覆盖旧错误/旧快照、取消后仍执行的权威读、同槽查询共享、目录关闭，以及单向投递不重发。真实 etcd 的 `TestEtcdRouteRefreshRepairsWithoutWatchEcho` 验证：权威已晋升、Watch 尚未回显时，主动读能够修正缓存，旧快照与迟到失败不能撤销新归属。

差异审查发现并先复现、后修复两项问题：

1. 两个不可用槽位的查询互相通知，原实现绕过轮询间隔，在 60ms 内执行 20,968 次读取。现在通知只检查本地缓存，权威回源保留每槽 100ms 间隔；同一回归窗口仅两次读取。
2. A 的 Handler 返回 B 的框架拒绝时，原实现会误失效 A 的地址。现在将当前 Actor 的准入/归属校验与业务执行结果分开；业务返回仍保留 `errors.Is` 和错误码，但不把下游拒绝传播为当前 Actor 的路由拒绝。

取消后的后端迟到读也已通过失败回归修正：查询观察到取消时不再写入路由缓存。独立复审确认上述修复，未发现新增阻塞项。

### 回归入口

以下使用现有本机 NATS、etcd、Redis，测试沿用独立前缀和清理规则：

```sh
export TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222
export TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379
go test ./...
go test -race ./internal/actorrpc ./internal/actorhost ./internal/actorplacement \
  ./internal/framework ./modules/discovery ./modules/rpc ./tests
```

tx 全量与上述七包 race 均通过。实际启用归属写回交接、失联不改派和真实 etcd 目录回归；没有设置 Mongo 后端等其余可选验收变量，相关用例按原规则跳过。txgame/server、txgameslg/server 各自 `go test ./...` 通过，未额外启用应用外部验收环境变量。本批未运行正式包多进程持续流量、云压测或第三方项目测试，不将包级回归解释为达到 99.99% 流量目标。

### 缓存命中微基准

本机 macOS amd64、Go 1.27.0、Intel i9-9880H。先执行同机前后五轮 P16 原定基准，观察到未改动的分组缓存也有波动；随后将基线提交 `6dedf9a` 导出到临时目录，分别编译测试程序，在 P4 下交替运行前/后版本五轮，每项 300ms，奇偶轮交换先后顺序。基准期间没有并行运行回归或其他本轮负载。下表为交替对照中位数。

完整路由基准在新实现中启用共享刷新绑定，持久端留空以保证命中不能回源。原定 P16 完整路由中位数由 222.9ns 到 264.5ns；下表的短窗口交替结果仍显示同方向回退，不能全部归因于机器波动。

| 基准 | 改动前 ns/op | 改动后 ns/op | 变化 | 改动前后分配 |
| --- | ---: | ---: | ---: | --- |
| 完整缓存路由到传输调用边界 | 217.7 | 245.9 | +13.0% | 8 B/op、1 allocs/op |
| 2,000 身份分组缓存，串行 | 92.16 | 93.78 | +1.8% | 0 B/op、0 allocs/op |
| 2,000 身份分组缓存，并行 | 143.7 | 143.8 | +0.1% | 0 B/op、0 allocs/op |
| 10,000 身份分组缓存，串行 | 97.06 | 99.4 | +2.4% | 0 B/op、0 allocs/op |
| 10,000 身份分组缓存，并行 | 143.7 | 145.7 | +1.4% | 0 B/op、0 allocs/op |
| 槽位缓存，串行 | 148.2 | 155.3 | +4.8% | 0 B/op、0 allocs/op |
| 槽位缓存，并行 | 90.83 | 91.04 | +0.2% | 0 B/op、0 allocs/op |

完整缓存路由的样本范围为改动前 203.3～227.1ns、改动后 241.4～265.5ns，中位数增加 **28.2ns（13.0%）**。这是可重复的小幅绝对成本，不能写成“性能完全不变”。该路径增加了失效状态检查、归属字段传递和正式刷新装配；本组测量未逐项隔离其占比。分组缓存中位数变化在 2.4% 内，槽位串行缓存增加 7.1ns，并行基本持平；所有路径均未增加分配。

保留条件失效所需的状态与归属标识，后续正式包验收继续观察端到端影响。本批证据只覆盖缓存到传输入口，未测新增内部信封字段、反馈回调对完整上下行吞吐的影响，不据此宣称吞吐提高或完全无回退。

基准入口：

```sh
go test ./internal/actorrpc ./internal/framework ./tests -run '^$' \
  -bench 'Benchmark(SharedCachedRoute|ActorPlacementHit|SlotRouterCachedResolve)' \
  -benchmem -count=5
# 交替对照使用相同三个包的测试二进制，每次：
# -test.run='^$' -test.bench='Benchmark(SharedCachedRoute|ActorPlacementHit|SlotRouterCachedResolve)'
# -test.benchmem -test.benchtime=300ms -test.cpu=4
```

## 2026-11-02 正常关闭释放与重新分配

第二批基于 tx `6dedf9a` 及尚未提交的第一批改动；应用基线为 txgame `0dd23d8`、txgameslg `d472123`。本机 macOS amd64、Go 1.27.0、Intel i9-9880H，复用本地 etcd/NATS/Redis/Mongo。本轮不提交、不跑云测试。

### 当前行为与失败边界

```text
旧 owner / g1
  → 正常排水、必要写回、Service.Stop 的私有队列完成
  → CAS：空 owner / g2
  → 普通寻址预检容量及成员状态 → CAS：新 owner / g3
```

空闲记录不删除、不占容量，迟到释放不能覆盖新归属。指定 World 仍由应用 `--world-id` 显式认领并加载；普通消息不会自动选择 World 宿主。启动回滚、写回失败、已接纳工作未退出、致命身份丢失不进入正常释放；`PrepareHandoff` 保留旧代次用于显式 Promote。释放成功之后的框架连接关闭失败不撤回已经安全释放的归属。自定义 SlotStore 只有实现可选 `SlotReleaser` 才参与正常释放，否则保留其既有显式管理方式。

真实 etcd 回归覆盖并发认领唯一胜者、单调代次、容量回收、部分释放后的重试及迟到旧释放。重复调用模拟“服务端已释放、调用方尚未确认”的状态，未注入真实网络丢包。Server 回归覆盖业务 Flush 失败后的重试、阻塞邮箱、启动失败、重复 Stop、PrepareHandoff 和 Service.Stop 自有队列的完成顺序。

差异审查发现三项缺口，均先由定向测试复现后修复：控制面使用缓存包装绕过成员复核；前缀快照 CAS 未阻止目标成员键删除；应用 Rank 私有队列直到 Service.Stop 才结束，不能在此前释放。现在正式写入口使用权威 Store，CAS 额外比较目标成员修订号，槽位释放晚于 Service.Stop。没有增加应用 Flush 适配层或逐消息网络查询。

### 本地回归

```sh
# tx：启用本机依赖
export TX_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222
export TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379
go test ./...
go test -race ./modules/discovery ./internal/framework ./modules/control \
  ./internal/actorrpc ./internal/actorhost ./internal/actorplacement
go test -race ./tests -run 'Test(Slot|CachedSlot|EtcdSlot)' -count=1
# 审查修复后的整包复验，启用 etcd/NATS
go test -race ./internal/framework ./etcd ./modules/discovery -count=1
# txgame/server 与 txgameslg/server 分别执行
go test ./...
```

上述检查通过。最终 tx 全量日志 `/tmp/tx-release-final-go.log`，六核心包 race 见 `/tmp/tx-release-race.log` 前六项，公开槽位 race 见 `/tmp/tx-release-public-race.log`，审查修复后三包 race 见 `/tmp/tx-release-review-green.log`；两组应用见 `/tmp/txgame-release-go.log`、`/tmp/txgameslg-release-go.log`。未设置其他可选后端验收变量的包级测试按原条件跳过，真实应用路径由下列独立集群覆盖。

**未通过项：** 扩大执行整个 `./tests -race` 时，`TestSessionClientRouterConcurrentFreshSessionsShareRoleOwner` 暴露既有 `tests/msgq_client_test.go:260` 的 `sessionClientDialerFake.instances` 并发 append；该测试同时假设并发建连只拨号一次。本轮未修改 MsgQ 路径或该夹具，失败完整保留在 `/tmp/tx-release-race.log`，不将定向通过表述为全套 race 通过。

### 正式包多进程验证

复用既有入口，无额外部署工具：

```sh
# txgame
sh cmd/cluster-e2e.sh --timeout 5m --artifact-root /tmp/tx-slot-release-game-final
# txgameslg
sh cmd/cluster-e2e.sh --world-startup --timeout 6m --artifact-root /tmp/tx-slot-release-slg-final
sh cmd/cluster-e2e.sh --timeout 6m --artifact-root /tmp/tx-slot-release-slg-full
```

| 场景 | 结果 | 独立核对 |
| --- | --- | --- |
| txgame 默认集群 | passed；普通 Game Stop 不再指定下一宿主 | 13 个进程退出码均为 0，登录、连接续接和存储探针完成 |
| SLG World 指定启动 | passed；旧 g1 → 空闲 g2 → 新 g3 | 两角色、直达/Role 转发、重启前后共 8 条成功通知；GDWorld 保留；10 次正常退出，重复 owner 的 1 次启动按预期非零退出 |
| SLG 完整集群 | passed；Game g3、World g4 | World 最终文档版本 2，持久化回读和容量检查均通过；13 次正常退出、2 次原夹具主动 kill，未算正常关闭 |

txgame 与 World 指定启动在最后三项审查修复后重新打包复验；SLG 完整场景在该修复前通过，修复后由核心 race 和 World 指定启动复验覆盖，未重复完整场景。工具成功退出完成自身 ownership 范围清理，最终进程列表未残留上述 artifact 路径的子进程；未删除其他数据库、容器或共享数据。

原始证据分别在：

- `/tmp/tx-slot-release-game-final/20261102-103736.177895000/summary.json`
- `/tmp/tx-slot-release-slg-final/20261102-103742.762610000/world-startup-summary.json`
- `/tmp/tx-slot-release-slg-full/20261102-103105.941970000/summary.json`

### 缓存热路径检查与未验证项

最终修复后，在其他本轮测试已结束时运行 P4、每项 200ms、三轮微基准。完整缓存路由中位数 **240.3ns/op，8 B/op、1 alloc/op**；分组缓存串行中位数为 2,000 身份 89.82ns、10,000 身份 97.06ns；槽位缓存串行 147.5ns、并行 84.52ns，后两类均 **0 B/op、0 alloc/op**。日志 `/tmp/tx-release-final-bench.log`。

```sh
go test ./internal/actorrpc ./internal/framework ./tests -run '^$' \
  -bench 'Benchmark(SharedCachedRoute|ActorPlacementHit|SlotRouterCachedResolve)' \
  -benchmem -benchtime=200ms -count=3 -cpu=4
```

本次没有重新采集第二批改动前的交替对照，不依据历史数值宣称变快或性能完全不变。新增成员快照/CAS 属于冷分配写入，目录规模成本留给第三批测量。有限请求集群不证明持续流量 99.99%、万连 20/50 或异常自动接管；本轮未运行云压测、长时稳态和持续流量发布回滚。

## 2026-11-02 目录更新测量与增量发布

第三批在前两批工作区基础上继续，本轮不提交。先测量现有全量重建，确认开销随总槽位数增加后，才改为正常 Watch 增量发布。仅修改 tx；两组应用和生成器契约不变，不跑云测试。

### 测量口径

- 机器：macOS amd64、Go 1.27.0、Intel i9-9880H。P4；每项 200ms，更新前后测试二进制交替运行三轮，第二轮倒换顺序；基准期间不并行运行本轮回归。
- 规模：1,024 / 8,192 / 65,536 条槽位；每批 1 / 32 / 256 条变化，轮转修改目标。普通 Type 每种默认 1,024 分区，玩家数不等于槽位数；65,536 是扩展规模检查，不代表当前示例已有这么多槽位。
- 更新基准覆盖已解码变更到可查询目录：旧实现更新 Watch map、展开全量 slice 并重建目录；新实现创建当批变化 slice 并更新目录。包括每批通知，不包括 etcd 网络、JSON 解码和 Runtime 发布屏障；也不模拟并发业务读取。
- 启动/重连另测新目录的完整快照发布，不混入正常 Watch 成本。峰值 RSS 使用独立测试进程执行单条变更 100 批，含 Go 运行时、输入夹具、初始化及 GC，不是正式服务的纯目录内存。

### 更新成本：三轮中位数

| 槽位数 | 每批变化 | 全量重建 | 增量更新 | 原分配/批 | 新分配/批 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 159.85µs | 0.228µs | 172,272 B | 176 B |
| 1,024 | 32 | 161.66µs | 2.894µs | 172,272 B | 2,160 B |
| 1,024 | 256 | 177.80µs | 20.907µs | 172,272 B | 16,496 B |
| 8,192 | 1 | 1,489.44µs | 0.229µs | 1,377,056 B | 176 B |
| 8,192 | 32 | 1,575.01µs | 3.282µs | 1,377,056 B | 2,160 B |
| 8,192 | 256 | 1,544.40µs | 24.235µs | 1,377,056 B | 16,496 B |
| 65,536 | 1 | 20,024.38µs | 0.280µs | 11,015,456 B | 176 B |
| 65,536 | 32 | 19,660.04µs | 5.084µs | 11,015,456 B | 2,160 B |
| 65,536 | 256 | 17,254.26µs | 32.672µs | 11,015,456 B | 16,496 B |

旧实现每批分配次数随目录规模为 8 / 36 / 260，新实现各档均为 2。增量成本主要随变化数增加；大目录仍有 CPU 缓存局部性影响，不能解释为各规模耗时完全相同。65,536 单变化旧实现范围 18.95～23.40ms，新实现 0.251～0.308µs；变化批量 256 的新实现为 31.65～45.41µs，完整样本另存原始日志。

| 槽位数 | 初始完整快照：前 → 后 | 快照分配/次（约） | 单独进程峰值 RSS：前 → 后 |
| ---: | ---: | ---: | ---: |
| 1,024 | 88.56 → 91.80µs | 129KiB | 14.62 → 11.32MiB |
| 8,192 | 805.25 → 818.52µs | 1.00MiB | 19.40 → 12.55MiB |
| 65,536 | 7,341.22 → 8,259.50µs | 8.01MiB | 72.86 → 23.20MiB |

初始完整快照算法没有优化，仍保留 O(N) 初始加载和断流恢复成本。65,536 档三轮中位数从 7.34ms 升到 8.26ms，因此在全部回归结束后另做每项 1 秒、交替五轮复测：前后中位数为 **6.387 / 6.454ms**，范围分别为 6.214～7.306ms、6.260～6.693ms，没有复现稳定的明显回退，也不宣称初始加载提速。复测文件为 `directory-snapshot-final-{before,after}-{1..5}.txt` 和 `batch3-snapshot-final-summary.json`。

峰值为每档单次进程观测，不外推生产 RSS。第一次 `/usr/bin/time -l` 受沙箱 sysctl 限制失败，取得本地测量权限后重新执行，失败样本不参与表格。

### 决策与边界

采用增量更新：旧实现即使只改一条，也在每个订阅进程上遍历并分配整份目录；8,192 条已达到毫秒级和每批约 1.31MiB。最小改动是在 `SlotDirectory` 增加私有变化发布入口，复用现有锁、版本表与单轮 Watch 发布屏障，移除 Watch 长期保存的重复 map 和全量展开函数；不增加配置、依赖或公共 SPI。

```text
首次启动 / Watch 重连 → 完整快照 → 目录可用
正常 Watch 事件批次 → 完整解码校验 → 同一锁内应用当批新增/修改/删除
  → 发布一次变化通知 → 后续请求继续查同一目录
Watch 中断 → 本轮作废 + 目录不可用 → 下一轮完整快照
```

迟到批次不能覆盖新 Watch；主动权威读的新版本优先于较早事件。同一批可能包含同一槽位的多次变更，因此用各事件的 ModRevision 保护主动读，用批次最大版本推进目录水位，不能用最大版本代替所有事件版本。每键主动读版本在可接受的对应变更或全量重建时清理，不为清理旧版本重新扫描全表；目录失效会全部清理。相同归属事件不清除已经收到的框架拒绝，释放的空 owner 保持有效记录，不能混同删除。新增入口不恢复已失效目录，旧 Watch 轮次不能向新目录发布。本批不改成员目录和冷分配容量快照的更新策略。

### 回归与复现

正式入口位于 `modules/discovery/slot_directory_benchmark_test.go`：

```sh
go test ./modules/discovery -run '^$' -bench '^BenchmarkSlotDirectory' \
  -benchmem -benchtime=200ms -count=3 -cpu=4
# 同机交替对照分别编译更新前后模块，保留两个二进制后交替运行：
go test -c -o /tmp/directory.test ./modules/discovery
/tmp/directory.test -test.run='^$' -test.bench='^BenchmarkSlotDirectory' \
  -test.benchmem -test.benchtime=200ms -test.count=1 -test.cpu=4
# 每个规模单独测量进程峰值，例如：
/usr/bin/time -l /tmp/directory.test -test.run='^$' \
  -test.bench='^BenchmarkSlotDirectoryWatchUpdate$/^slots=8192$/^batch=1$' \
  -test.benchtime=100x -test.cpu=4
```

原始样本及改动前后二进制保存在本地 `.superpowers/sdd/actor-routing-lifecycle-plan/`：最终数据使用 `directory-final-{before,after}-paired-{1,2,3}.txt`、`directory-final-{before,after}-rss-*.txt`、`batch3-final-bench-summary.json`。`batch3-before/` 保存原生产代码及全量发布基准，`final-before-overlay.json` 用于不改工作区地编译旧实现；`batch3-after/` 保存最终基准源码。第一轮没有单独保留旧基准源码，审查后重新保存源码并完整重测；无 `final` 的初测日志不作为最终结果。这里只保存观测证据，不加入另一套运行期开关或对照实现。

定向失败回归已观察到更新前一条变化触发第二次完整快照；更新后通过，见 `update-red.txt` / `update-green.txt`。版本合并、增量不能恢复失效目录和旧轮次发布拒绝已纳入测试。

扩大真实依赖启用范围时，`TX_ACTORTRIAL_ETCD_ENDPOINT` 打开了此前未执行的 Actor 选址用例，发现夹具只登记本地目录、没有登记 etcd 成员，因此被第二批的权威成员校验拒绝。使用改动前生产代码 overlay 复现相同错误后，补齐三处夹具的权威成员登记，不绕过正式校验。先前仅启用 `TX_ETCD_ENDPOINT` 的通过记录不能覆盖这些用例。失败记录为 `fixture-before-failure.txt`、`batch3-full.txt`、`batch3-race.txt`；修复后记录如下。

```sh
export TX_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222
export TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379
go test ./...
go test -race ./modules/discovery ./internal/framework ./internal/actorrpc \
  ./internal/actorhost ./internal/actorplacement ./modules/control ./modules/rpc -count=1
go test -race ./tests -run 'Test(EtcdSlot|EtcdRouteRefresh|Slot|CachedSlot)' -count=1
```

三组命令均退出 0，分别记录在 `batch3-full-final.txt`、`batch3-race-final.txt`、`batch3-public-race-final.txt`。真实依赖用例使用独立 ownership 范围；成功后按现有规则清理，失败范围保留，不清理其他数据。本批没有重跑全量 `./tests -race`，第二批已记录的 MsgQ 测试夹具竞争不因此消失，也不计为全量 race 通过。

缓存热路径在其他本轮负载结束后单独运行三轮，P4、每项 200ms：完整路由中位数 247.8ns/op、8 B/op、1 alloc/op；10,000 身份的分组缓存串行 104.9ns、并行 160.4ns；槽位缓存串行 151.1ns、并行 93.28ns，后两类均 0 B/op、0 alloc/op，见 `batch3-hot.txt`。本次只有目录更新作了严格前后交替对照，不以热路径与旧日期结果的差别断言性能完全不变或提升。

两组示例分别在 `txgame/server`、`txgameslg/server` 执行 `go test ./...` 通过，未改变应用装配或生成契约。见 `batch3-txgame-server-final.txt`、`batch3-txgameslg-server-final.txt`。

独立审查发现一项 P2：批次里较早事件可能清除主动读最终归属的失效标记。`TestSlotChangeBatchPreservesInvalidatedRead` 先失败，再通过逐事件版本判断修复，见 `batch-version-red.txt` / `batch-version-green.txt`；修复没有新增归并 map、公共接口或运行期开关。性能表使用修复后的重新测量，不沿用中间实现数值。

本批不跑正式包集群、持续流量、云压测和故障接管；不外推业务吞吐、P99 或 99.99% 完成率。

后续三批联合正式包与持续流量已于 2026-11-02 单独完成，结果见[联合验收](actor-routing-rollout-acceptance.md)。该报告区分 txgame 在线滚动与 SLG 停服重启，不改变本节微基准的测量范围。
