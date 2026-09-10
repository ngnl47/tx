# 本轮 Ponytail 累计收益复核（2026-09-11）

## 口径

范围为 2026-09-10 官方 Ponytail 补充审计的基线至 S99 完成提交（S01–S99），不包含更早的框架瘦身，不将各轮净行数或微基准百分比相加。S100 尚未提交的收尾文档与本报告不计入固定提交范围。用 Git diff --numstat --no-renames 统计；删除行包含被替换的旧行，因此实际代码规模变化应看净减少。

| 工程 | 起点 | 终点 | 增加代码行 | 删除代码行 | 净减少代码行 |
|---|---|---|---:|---:|---:|
| tx | a4e20ea | 2cf50a1 | 38 | 60 | 22 |
| txgen | 9c2ea1e | 52ac2ac | 1,160 | 2,229 | 1,069 |
| txgame | 01233b1 | c44d9bb | 11,628 | 28,962 | 17,334 |
| txgameslg | e8c15df | db8a6a0 | 448 | 1,101 | 653 |
| txclient | 5514f2a | 5514f2a | 0 | 0 | 0 |
| txclientslg | 5fd5641 | 9f2d571 | 58 | 54 | -4 |
| 合计 | | | 13,332 | 32,406 | 19,074 |

代码口径为 Go、TS/JS、Vue、模板及对应测试。生成代码按 tests/output、server/gen 目录或生成标记识别，模板和测试优先单列；本提交区间没有其他类型文件差异。独立工程生成物分别计数，反映仓库实际维护/编译体积，不代表这些副本同时加载进同一个进程。

| 分类 | 增加 | 删除 | 净减少 |
|---|---:|---:|---:|
| 生成代码 | 12,697 | 31,732 | 19,035 |
| 手写代码 | 213 | 311 | 98 |
| 模板 | 227 | 280 | 53 |
| 测试 | 195 | 83 | -112 |
| 文档 | 2,993 | 13 | -2,980 |

文档单列，不作为瘦身收益；负数表示净增加。测试净增加是保留和补充关键行为证据，并不抵消生成物维护面的减少。

## 累计运行时对照

通过 git archive 在 /private/tmp/ponytail-overall-20260911/before 与 after 分别展开 tx、txgen，模块 replace 使用同一侧的 tx。前后均使用当前同一组 22 个测试/基准文件，SHA256 核对一致；保留各自提交的生成物。fixture 输入在本区间没有变化。没有改写当前工作区代码，没有使用旧版本测量结果。

Go 1.27.0 darwin/amd64，Intel i9-9880H，GOMAXPROCS=1。先编译两份测试二进制，再交替顺序执行六轮，每项 300ms，采集中不并行执行编译或其他 benchmark。每轮先执行 TestGenerated.*(Load|RecordKey) 相关测试。下表为六轮中位数，耗时变化负数为更快。

| 场景 | ns/op 前→后 | 耗时变化 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| GeneratedLoad/Single | 2416.5→2306.5 | -4.6% | 736→576 | 17→16 |
| GeneratedLoad/BatchByID | 2644→2517.5 | -4.8% | 904→744 | 22→21 |
| ProjectionAllocation/RoleMissing | 1156.5→970.4 | -16.1% | 672→432 | 10→7 |
| ProjectionAllocation/RoleIncluded | 1141.5→983.4 | -13.9% | 640→432 | 9→7 |
| ProjectionAllocation/ShardMissing | 1094→889.4 | -18.7% | 496→304 | 10→6 |
| GeneratedPersistenceModes/SnapshotSlice32 | 13394→10605.5 | -20.8% | 8680→7608 | 128→62 |
| GeneratedPersistenceModes/SnapshotScalar | 4649→4739 | +1.9% | 5488→5488 | 48→48 |
| GeneratedPersistenceModes/StoreScalar | 4241→4370.5 | +3.1% | 5216→5216 | 46→46 |
| GeneratedStoreDirtyRuntime/FirstScalarWrite | 160.6→164.55 | +2.5% | 296→296 | 3→3 |
| GeneratedStoreDirtyRuntime/RepeatedMapKey | 62.345→64.25 | +3.1% | 0→0 | 0→0 |
| GeneratedStoreDirtyRuntime/DistinctMapKeys1024 | 137834→137228 | -0.4% | 123936→123936 | 1050→1050 |
| GeneratedStoreDirtyRuntime/CaptureMergeAndBuildFrameDocument | 887400→792487 | -10.7% | 439259→439243 | 3614→3614 |
| GeneratedStoreDirtyRuntime/CaptureFrameOnly | 518242→472260 | -8.9% | 282289→282289 | 2273→2273 |
| GeneratedStoreDirtyRuntime/BuildCapturedLeafFrameDocument | 775171→619186 | -20.1% | 263066→246648 | 3765→3765 |
| GeneratedStoreDirtyRuntime/SuccessfulFlush1024 | 317389→313597 | -1.2% | 194664→194662 | 2194→2194 |

负载来自当前实际生成结构：首次/重复标量写入、1024 个不同集合键、切片快照、16 个视图各含 64 项的帧处理、内存写回和加载。投影用空文档 loader，完整加载用已有记录样本；因此不同加载行之间不能直接比较成本。存储为测试 applier/loader，不包含真实 Mongo、Redis、网络或集群吞吐。返回对象和必要隔离副本均保留。

不把这些不同工作量的场景加权成一个总体百分比；没有生产流量占比，无法推算整服吞吐、延迟或节约服务器数量。没有重跑完整六工程测试、真实集群或 race；本轮工作是统计和对照测量，后续是否修正任何回退另按收益判断。

## 小幅上浮场景的延长复测

初测四项耗时上浮约 1.9%–3.1%，因此只对这些疑点延长至每项 1s，重新交替六轮，不重复其他已稳定收益项，也没有修改代码。原始初测与复测均保留。

| 场景 | ns/op 前→后 | 耗时变化 | B/op | allocs/op |
|---|---:|---:|---:|---:|
| GeneratedPersistenceModes/SnapshotScalar | 4526.5→4511.5 | -0.3% | 5488（不变） | 48（不变） |
| GeneratedPersistenceModes/StoreScalar | 4126→4111.5 | -0.4% | 5216（不变） | 46（不变） |
| GeneratedStoreDirtyRuntime/FirstScalarWrite | 159.1→158.65 | -0.3% | 296（不变） | 3（不变） |
| GeneratedStoreDirtyRuntime/RepeatedMapKey | 61.99→63.765 | +2.9% | 0（不变） | 0（不变） |

三个标量场景复测约 −0.3%，未确认持续退化。重复集合键写入仍约 +2.9%，即 61.99→63.765ns，绝对差 1.775ns，分配仍为零；该项保留为累计取舍，不据此自动启动新一轮微优化。当前测量未做统计显著性推断，不能证明所有硬件、数据分布或真实服务均无退化。

## 评估结论

本轮主要收益是生成代码净减 19035 行，以及部分生产运行路径的减少分配和耗时降低：32 项切片快照约快 20.8%，分配次数 128→62；投影加载约快 13.8%–18.7%，减少 2–4 次分配；捕获并合并构建帧文档约快 10.7%，叶子帧文档构建约快 20.1%，后者字节数约少 6.2%。完整单条/批量加载约快 4.6%–4.8%，各少一个对象分配。逐项原始数据为判断依据，不叠加百分比。

标量、普通集合写入和写回的收益有限，重复键微路径存在上述小幅上浮。因此结论是“生成物规模明显缩小，多条重路径有收益”，不是“全路径提升”。在缺少生产瓶颈证据时停止边际微优化是合理的，本报告不更改阶段收敛决定。

所有选定基准及随运行执行的加载/主键测试均通过；两版基准负载一致性校验通过。本轮仅补文档，不重新宣称历史全量测试或真实集群结果。

## 复现材料

原始证据目录：/private/tmp/ponytail-overall-20260911。line-counts.json 含逐文件分类及增加/删除数，results.json 含全部样本，medians.json 含中位数，before-N.txt/after-N.txt 为原始日志，run.py 为交替采样脚本，test-sha256.json 为共同负载校验。

基准脚本选择 GeneratedStoreDirtyRuntime、GeneratedPersistenceModes、GeneratedLoad 与 ProjectionAllocation 的 15 个子场景；完整筛选式及命令保留在 run.py，不在生产路径增加采集或优化代码。

延长复测使用 recheck.py，原始日志 recheck-before-N.txt/recheck-after-N.txt、recheck-results.json 与 recheck-medians.json 单独保存。

后置 ponytail-review：Lean already. Ship. 本轮无生产代码改动，未新增复杂度；六工程 diff 检查通过。报告及此前 S100 计划变更均未提交。
