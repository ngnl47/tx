# Bee 与 Ant 迁移基线

本文记录 Core、Bee、Ant 框架边界迁移开始前的测试和性能基线。后续阶段只在相同工具链、相同命令和相同机器上比较，
避免把环境差异误判为架构收益或退化。

## 环境

- 日期：2026-08-27
- 系统：macOS darwin/amd64
- CPU：Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
- Go：go1.27.0
- module 最低版本：Go 1.26
- Go 缓存：`GOCACHE=/private/tmp/tx-go-build`
- 临时目录：`GOTMPDIR=/private/tmp`

## 正确性基线

以下四组命令均通过。涉及本地 TCP、WebSocket、gRPC、Redis、Mongo 和 etcd 的测试在允许绑定本地端口的环境中执行。

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgen && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgame/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgameslg/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

## 性能基线

表中数据是五次运行的范围；`B/op` 和 `allocs/op` 在五次运行中保持一致时只写一个值。

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Fixed Actor 已激活，串行 | 10,602-11,095 | 735-743 | 12 |
| Fixed Actor 已激活，并行 | 1,206-1,478 | 636-640 | 10 |
| Fixed Actor 冷激活，串行 | 16,344-16,925 | 2,629-2,719 | 34 |
| Fixed Actor 冷激活，并行 | 4,441-5,273 | 2,485-2,606 | 33 |
| Fixed Actor 创建/销毁 | 31,721-34,626 | 2,362-2,369 | 39 |
| 缓存路由命中，串行 | 135.2-149.4 | 0 | 0 |
| 缓存路由命中，并行 | 68.38-75.12 | 0 | 0 |
| Fusion ApplyChanges | 1,439-1,513 | 1,624 | 8 |
| Procedure 根过程 | 997.1-1,017 | 1,160 | 9 |
| Procedure 子过程复用根锁 | 1,191-1,235 | 1,192 | 10 |
| Procedure 子过程动态加锁 | 1,826-1,901 | 1,481 | 14 |
| 单 Key 锁获取/释放 | 235.3-254.9 | 0 | 0 |
| 等待图无环登记/移除 | 50.69-52.65 | 0 | 0 |
| 等待图检测双事务环 | 131.7-135.3 | 216 | 3 |
| 事务提交到 Cache Manager | 3,590-3,796 | 2,234 | 25 |
| WAL NewRecord，1 KiB | 2,827-2,915 | 0 | 0 |
| WAL checkpoint 合并提交 | 79.84-87.70 | 0 | 0 |
| 生成 Store，单标量 | 2,932-3,000 | 5,216 | 46 |
| 生成 Store，100 项 map | 7,630-7,798 | 12,848 | 157 |
| Snapshot Diff，单标量 | 3,319-3,793 | 5,488 | 48 |
| Snapshot Diff，100 项 map | 56,763-58,535 | 26,877-26,919 | 320 |

WAL `NewRecord` 的对应吞吐为 351.25-362.21 MB/s。

基线命令：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'StoreView|Fusion|Transaction|Cache|WAL|Actor' -benchmem -count=5
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./modules/txn ./wal ./wal/checkpoint -run '^$' -bench '.' -benchmem -count=5
cd /Users/a47/workspace/txgen && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./tests -run '^$' -bench 'BenchmarkGeneratedPersistenceModes' -benchmem -count=5
```

## 迁移比较规则

1. Core 组件图和生命周期宿主不应改变 Store、Fusion、Procedure、WAL 或 Actor 热路径。
2. Bee/Ant 的门面构建和启动不进入业务请求热循环，优先审计启动正确性和生命周期，不用零分配目标掩盖 API 复杂度。
3. 迁移后若热点 `allocs/op` 增加，必须定位新增分配来源；没有明确收益时回退该实现。
4. Actor benchmark 会受 ProtoActor 启动和调度抖动影响，比较时同时观察耗时范围、分配和功能测试，不能只取单次最小值。

## Core 运行时提取复测

完成 Component Host、Storage Runtime、Discovery Runtime 和 control.Server 提取后，使用同一台机器与工具链执行：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'Fusion|Actor|Directory' -benchmem -count=5
```

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Fixed Actor 已激活，串行 | 9,830-10,988 | 730-736 | 12 |
| Fixed Actor 已激活，并行 | 1,184-1,266 | 636-638 | 10 |
| Fixed Actor 冷激活，串行 | 15,091-15,954 | 2,626-2,653 | 34 |
| Fixed Actor 冷激活，并行 | 3,966-4,513 | 2,484-2,557 | 33 |
| Fixed Actor 创建/销毁 | 20,698-22,098 | 2,287-2,295 | 39 |
| Fusion ApplyChanges | 1,011-1,220 | 1,624 | 8 |

核心路径的 `allocs/op` 均未增加。Actor 调度耗时容易受机器状态影响，本轮结果只用于确认没有性能退化，不把耗时下降归因于启动期架构迁移。

## Ant 与 CModProcedure 复测

完成 Ant 门面、CModProcedure 和 txgameslg 迁移后执行：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'Transaction|Cache|WAL' -benchmem -count=5
```

| 场景 | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 事务提交到 Cache Manager | 2,742-3,059 | 2,234 | 25 |
| 缓存路由命中，串行 | 112.6-134.9 | 0 | 0 |
| 缓存路由命中，并行 | 49.75-61.50 | 0 | 0 |

事务提交链的分配与基线完全一致；门面迁移没有把适配分配带入业务热路径。耗时结果受机器状态影响，只作为无退化证据，
不把本轮下降归因于框架命名空间调整。
