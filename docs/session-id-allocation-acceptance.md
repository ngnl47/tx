# SessionID 独立发号本地验收（2026-11-02）

本轮完成 [实施计划](session-id-allocation-plan.md)：新 Session 使用独立 etcd Put 的成功响应 revision，InstanceID 使用 `AppID-UUID`，删除 work slot 与旧时间拼号器。Actor 逻辑槽位、容量、排水、条件释放和显式交接保持原契约。业务继续调用 `CModSessionID().AllocateSessionID(ctx)`，两组客户端和 txgen 无需修改。

## 责任和生命周期

```text
框架创建共享 etcd 客户端
  → Discovery 借用客户端登记成员、维护目录
  → CModSessionID 借用同一客户端、发布尚未启用的发号句柄
  → 服务启动成功后开放发号

Login 新 Session → 核对准入/身份 → 独立 Put → 检查取消/范围/身份 → 返回编号

Server.Drain → 关闭服务准入、取消在途发号 → 等待已接纳请求退出
  → 其余业务排水和必要写回 → 条件释放 Actor 槽位
  → 注销成员/撤销租约 → 框架关闭共享连接
```

发号键为规范化 Discovery 根 `R` 的 `R + "-session-ids/issuer"`，不在 `R + "/"` 的目录 Watch/容量 CAS 范围内，不挂租约。普通上下行和同 Session 重连不发新号。

审查复现并修复了一个关闭顺序问题：原取消发生在最终 CMod Drain，但 Server 会先等 Login 请求退出；etcd 写入等待可能反向阻塞排水。新增经真实 Admission、Server.Stop 的测试先复现 `pending 1: context deadline exceeded`，再验证提前取消仅作用于发号，其他已接纳业务继续原排水规则。调用方在分配前或返回前取消的测试也先复现旧行为，再验证返回 `0/error`。

## 本地回归

环境为 Intel i9-9880H Mac、32 GiB、Docker 单节点 etcd 3.5.15，使用既有本地 NATS/Redis/Mongo；未修改共享依赖配置，未跑云测试。

| 验证 | 结果和边界 |
| --- | --- |
| tx 最终 `go test ./... -count=1` | 通过；未启用的外部依赖用例依旧 Skip，不作为真实依赖通过 |
| 设置 `TX_ETCD_ENDPOINT`、`TX_ACTORTRIAL_NATS_URL` 后测试 `./etcd ./sessionid ./modules/discovery ./internal/framework ./tests` | 通过，覆盖实际发号、成员身份和 Actor 归属；未设置 Redis/Mongo 专用开关的用例不计入此行 |
| 设置 etcd 后 `go test -race ./etcd ./sessionid ./modules/discovery ./internal/framework -count=1` | 通过 |
| txgame、txgameslg 的 server `go test ./...` | 两组通过；定向清理/配置/Login 包额外以 `-count=1` 通过 |
| 两组客户端 `npm test`、`npm run check` | txclient 116 个、txclientslg 127 个测试通过，无 Skip；类型检查通过 |
| 两组 `cmd/package.py program --env debug` | 构建通过，清单 `diagnostics=false`，未对这两份无诊断包重复运行整套集群 |

真实新增集成测试覆盖：多分配器并发唯一、重启不复用、旧句柄失效、53 位范围、启动失败清理、借用连接关闭边界，以及发号与逻辑槽位创建/释放并行。另用确定性的“根快照 → 发号 → 槽位 CAS”断言键范围隔离，避免只靠并行偶然通过。

## 既有多进程入口

依次执行两组 `cmd/cluster-e2e.sh`，重新构建验收二进制并使用隔离资产目录。此入口使用 `txbench` 标签；这是功能与生命周期验收，不是无诊断构建的业务吞吐比较。

| 工程 | 实测 |
| --- | --- |
| txgame | `passed`，13 个进程；双 Login、新/旧账号角色投影、Login 重启；Login 全停前后两个 TCP/WS 连接及 SessionID 不变；Agent 重启恢复原 Session；Game、MsgQ 关闭/重启和槽位重新分配通过 |
| txgameslg | `passed`，15 个进程（含两个原验收设计的预期故障退出）；202 次不同账号、20 次同账号跨双 Login 并发发号无碰撞；两个 Login 重启后发新号；Login 全停后原连接继续通信、原 Session 重连；World 容量、指定归属、Generation 及 Mongo 写回通过 |

两组结束后只读复查：本轮进程无残留，`/tx/e2e/` 无键（包含派生 issuer），Redis DB12/13 均为 0，两组专用 Mongo 库无集合。清理工具有 ownership 门禁，并以目录尾 `/` 和精确 issuer 键删除；相邻键、其他环境及无/错误 owner 的行为由测试覆盖。

## 发号性能

同机单 etcd，公开 Bee `CModSessionID` 入口与 `PutRevision` 对照，每档 3 秒 × 3 轮，附加 8 并发固定 3000 次的 AB/BA 交错对照。下表为完整入口三轮指标的中位数，不是合并样本分位数。

| 并发 | 成功次数/秒 | P50 | P99 |
| --- | ---: | ---: | ---: |
| 1 | 666 | 1.402ms | 3.295ms |
| 8 | 3,632 | 2.081ms | 4.496ms |
| 32 | 9,528 | 3.226ms | 6.271ms |

22 组正式测量共 311,195 次，错误、组内重复和范围越界均为 0。各组均单独校验唯一性；不把此统计写成跨全部样本的额外集合校验。

原生 Put 在固定 32 并发下也出现 9,425 → 7,664 → 5,995 次/秒的波动，交错对照同样波动，因此不能将两组时延相减归因为框架成本。完整入口各档比原语多约 6 次分配，可作为包装成本观察；结果支持本机每秒数千次发号的可行性，不等于生产三节点 SLA 或完整登录 QPS。

主测量 etcd DB 增长 66,387,968 字节，配对增长 1,925,120 字节，包含 Go 校准、预热和清理产生的历史。隔离键已删除，但没有压缩共享 MVCC 历史。生产需按 [现行 API](api-reference.md#服务发现) 配置容量告警、历史压缩和安全恢复策略。首次新旧算法切换仍须统一版本、使旧票据/会话失效；恢复旧 etcd 快照不能直接沿用可能回退的 revision 发号。

## etcd 历史压缩恢复补验（2026-11-02）

本次新增 `modules/discovery/compaction_integration_test.go`，不修改生产代码。使用本机已有 etcd 3.5.15 镜像创建独立容器，仅绑定 loopback 随机端口，不挂载共享数据。测试开关为 `TX_ETCD_COMPACTION_ENDPOINT`，未设置则跳过，绝不回退到普通 `TX_ETCD_ENDPOINT`。**Compact 影响整个集群历史，此开关只能指向专用验收实例，随机键前缀不能隔离压缩操作。**

测试包装器只延迟真实客户端调用，不合成响应；分别在成员和槽位 Watch 上控制以下流程：

```text
首次快照发布 → 暂停目标 Watch 的建立
  → 修改成员和槽位、继续发号 → 压缩到更新的 revision
  → 恢复目标 Watch → 核对真实 ErrCompacted
  → 两份目录失效 → 重新读取快照 → 恢复 Ready
  → 新成员/槽位增量继续生效，SessionID 继续递增
```

恢复前成员查询不得复用旧目录，槽位查询返回 `ErrSlotDirectoryUnavailable`；恢复后读到 Draining 成员和已释放的 generation 2，再产生 Running/generation 3 增量并确认缓存更新。压缩前后发号均满足正数、JavaScript 安全整数及递增要求，issuer 不挂租约；不要求编号连续。单份新快照不能提前 Ready、迟到旧轮次写入拒绝的细节继续由既有单元测试覆盖。

最终代码在 Go 1.27.0、独立 etcd 3.5.15 下运行：

```sh
# 必须先准备独立 etcd，并替换为本轮端口；不要填共享服务的地址。
TX_ETCD_COMPACTION_ENDPOINT=http://127.0.0.1:<隔离端口> \
  go test -race ./modules/discovery \
  -run '^(TestEtcdRuntimeRecoversAfterCompaction|TestRuntimeWatchRejectsLateSnapshotAndRequiresCurrentPair)$' \
  -count=3 -v -timeout=120s

# 不启用压缩开关，复用同一独立实例运行三个包的现有集成测试。
TX_ETCD_ENDPOINT=http://127.0.0.1:<隔离端口> \
  go test ./etcd ./sessionid ./modules/discovery -count=1 -timeout=90s
```

结果：两类真实压缩场景共六次和既有轮次屏障三次均通过 race；三包回归通过。日志中的 `mvcc: required revision has been compacted` 是刻意触发的恢复输入，不是未解决的测试失败。用例取消并等待 Watch 返回后再关闭 Runtime/客户端，独立核对测试前缀无残留；本轮容器已删除，共享 etcd 未重启或修改配置。

证据目录：`/Users/a47/workspace/tx-artifacts/etcd-compaction-20261102/`。最终版结果见 `compaction-race.log`、`packages.log`，源码指纹见 `test-source.sha256`；`compaction.log` 保留精简测试屏障前的首轮记录，`cleanup.json` 和 `container.log` 记录隔离环境清理与服务日志。定向 `ponytail-review` 未发现需修改的问题。

本项只证明压缩后的恢复正确性，不证明自动压缩调度、长期容量、defrag 停顿或生产三节点吞吐；这些仍按 [etcd 使用与维护](api-reference.md#etcd-使用与维护) 在实际部署中核验。不跑云测试、不提交。

## 证据与审查

本机证据根：`/Users/a47/workspace/tx-artifacts/sessionid-implementation-20261102/`。

- `tx-full-final.log`、`tx-integration.log`、`tx-race.log`：最终框架回归。
- `txgame-full.log`、`txgameslg-full.log`、`txclient-*.log`、`txclientslg-*.log`：应用/客户端回归。
- `txgame-e2e/20261102-140441.100022000/summary.json`、`txgameslg-e2e/20261102-140550.844130000/summary.json`：集群结果。
- `session-id-framework-benchmark.log`、`session-id-framework-benchmark-paired-abba.log`、`session-id-framework-benchmark-summary.json`：原始性能样本及汇总。
- `session-id-stop-red.log`：关闭时机问题的失败基线；`cleanup-verification.json`：清理核验。
- `baseline/`：本轮开始时的工作区快照，用于区分此前未提交变更。

按 `ponytail-review` 对本轮增量及关联调用做过独立审查，关闭时机问题修复并复核后，无未解决发现。三工程 `git diff --check` 通过；未提交 Git。
