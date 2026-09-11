# GD 标脏、视图合并与叶子变更回测（2026-09-11）

## 结论与口径

本次重新测量，不引用历史耗时作为结果。对照官方 Ponytail 优化前 tx a4e20ea / txgen 9c2ea1e 与当前 tx f4f3d38 / txgen 54e9164；两版通过 git archive 展开到临时目录，使用同侧 tx replace，各自保留生成物。当前工作区的未提交文档不影响二进制。两版复制同一组当前测试文件并核对 SHA256，输入模型在该区间无差异。

Go 1.27.0、darwin/amd64、Intel i9-9880H、GOMAXPROCS=1。先编译二进制，再交替顺序执行六轮，每项 400ms；两版不并行采样。耗时与分配列为各侧中位数；配对变化先计算每轮后/前比值，再取六轮中位数，避免时钟/机器负载漂移时只比较两侧中位数。负数表示耗时下降。没有统计显著性检验或真实业务流量加权，不推导整服收益。

| 场景 | 优化前 ns/op | 当前 ns/op | 配对耗时变化 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|---:|
| SnapshotSlice32 | 16649.00 | 12124.00 | -21.8% | 8680→7608 | 128→62 |
| SnapshotScalar | 5458.50 | 5048.50 | +0.2% | 5488→5488 | 48→48 |
| StoreScalar | 5108.50 | 4654.00 | -0.3% | 5216→5216 | 46→46 |
| FirstScalarWrite | 190.80 | 175.20 | +4.3% | 296→296 | 3→3 |
| RepeatedMapKey | 77.18 | 75.16 | +3.7% | 0→0 | 0→0 |
| DistinctMapKeys1024 | 171031.50 | 156610.00 | +1.5% | 123936→123936 | 1050→1050 |
| VisitChanges1024 | 108.50 | 48.77 | -53.0% | 0→0 | 0→0 |
| BuildDocument1024 | 606.75 | 508.10 | -13.9% | 480→464 | 5→5 |
| MergePreparedViews16x64 | 383521.00 | 266547.50 | -22.3% | 122016→122016 | 1058→1058 |
| CaptureMergeAndBuildFrameDocument | 1148464.50 | 919351.50 | -9.8% | 439258→439242 | 3614→3614 |
| CaptureFrameOnly | 650213.00 | 541060.50 | -5.2% | 282289→282289 | 2273→2273 |
| CaptureLeafFrameOnly | 1359331.00 | 1132658.50 | -11.7% | 491806→491806 | 5391→5391 |
| BuildCapturedFrameDocument | 748.60 | 635.30 | -12.6% | 480→464 | 5→5 |
| BuildCapturedLeafFrameDocument | 975714.50 | 800136.00 | -16.6% | 263060→246664 | 3765→3765 |
| LeafChanges1024/Visit | 289900.00 | 158198.50 | -44.7% | 0→0 | 0→0 |
| LeafChanges1024/Document | 998027.00 | 876086.50 | -11.9% | 263053→246652 | 3765→3765 |

## 短路径延长复测

首批发现短路径的逐轮配对变化与两侧中位数之比不一致，因此仅对以下五项延长到每项 1s，交替六轮。前表保留初始结果，短路径判断以下表为准。

| 场景 | 优化前 ns/op 中位数 | 当前 ns/op 中位数 | 配对耗时变化 | 六轮配对范围 | 分配 |
|---|---:|---:|---:|---|---|
| SnapshotScalar | 4997.50 | 4747.50 | -1.4% | -8.4%～+0.1% | 不变 |
| StoreScalar | 4701.00 | 4282.50 | -1.0% | -16.5%～+1.4% | 不变 |
| FirstScalarWrite | 180.05 | 165.10 | -0.9% | -15.8%～+2.0% | 不变 |
| RepeatedMapKey | 75.37 | 70.56 | +1.3% | -12.7%～+5.7% | 不变 |
| DistinctMapKeys1024 | 165279.00 | 150748.00 | -0.7% | -14.8%～+2.7% | 不变 |

首次标脏、重复键以及标量短路径未确认稳定提速或退化；例如重复键配对中位数 +1.3%，但正负波动均存在，不能从两侧中位数之比 −6.4% 宣称提速。没有消除全部机器噪声，不作统计显著性结论，也不为此继续第三轮测量或微优化。独立合并、切片快照、真实叶子遍历及叶子文档构建在原六轮中均为下降，按配对结果报告收益。

run-recheck.py、recheck-results.json、recheck-medians.json 和 recheck-before/after-N.txt 保存延长复测；paired.json 保存首批和叶子补测的配对中位数与范围。

## 负载边界

- FirstScalarWrite 包含创建 GD、首次创建 Store 和标脏；RepeatedMapKey 是同键反复写入，不代表新键分配。DistinctMapKeys1024 包含对象及集合建立。
- 现有 VisitChanges1024 / BuildDocument1024 对新建集合标脏，实际收敛为整字段替换，不能把其耗时当作逐条处理 1024 个叶子。Go 字段文档也未包含后续网络编码或持久化。
- 新增 LeafChanges1024 使用已存在的 1024 个 GDItem，只修改 Quantity；benchmark 检查 visitor 次数和文档 Set 条数均为 1024。真实叶子遍历配对耗时下降约 44.7%，仍为零分配；叶子文档构建配对耗时下降约 11.9%，分配次数不变，字节数减少约 16 KiB。
- 新增 MergePreparedViews16x64 在计时前捕获 16 批、每批 64 项的视图，计时包含空目标 View 创建和 16 次 Merge，不含源对象建立、捕获或 Document。检查最终 1024 项及末项值。该场景首批含整集合替换，后续追加不同键，不代表所有合并冲突分布。配对耗时下降约 22.3%，分配次数不变。
- CaptureMergeAndBuildFrameDocument 保留建立对象、标脏、捕获、合并、构建文档的组合成本。SnapshotSlice32 的分配由 128 降至 62，其余采样路径没有分配次数增加。
- 没有用本机较小差异宣称所有路径提速，也不根据这些结果继续微优化。没有真实 Redis/Mongo I/O、集群吞吐、并发事务或大于 64 字段模型测量。

## 验证与复现

两版各 68 项 Generated Store/Snapshot 行为测试通过。首次旧版验证失败于 TestGeneratedStoreReusesStaticTypeSpecInternally：它断言当前模板 helper 的源码字符串，不适用于优化前产物；随后在两侧同时用 -test.skip 排除该文本断言，未屏蔽行为失败，首次日志保留。当前 txgen 全量 go test ./... -count=1 -timeout=180s -json 为 506 pass、0 skip/fail，包括该当前模板断言。全部 16 个 benchmark 场景两侧各六轮成功，未运行 race。

材料目录：`/private/tmp/tx-gd-dirty-merge-20260911`。inputs.json 固定提交和首批共同测试 SHA256；prepare.py 为展开/编译脚本；run.py、run-leaf.py 为筛选及交替采样命令；results.json / medians.json 与 leaf-results.json / leaf-medians.json 保存全部样本和中位数；leaf-benchmark-sha256.txt 记录补测时两侧同一文件；before/after-tests.txt、before-initial-tests.txt 与 current-full-tests.jsonl 保存验证日志。首批和叶子补测分开采集，不覆盖初始结果。

本次生产和生成物修改 0 行。只在 txgen 现有 benchmark 文件增加独立合并及真实叶子路径负载；采集、对照源码和二进制都在临时目录，不进入生产路径。

后置 ponytail-review：Lean already. Ship. 复用现有基准夹具，只补独立合并和真实叶子路径，未新增公共抽象。六工程 diff 检查通过；当前 txgen benchmark 新增 64 行，生产和生成物修改 0 行。
