# 生成物运行时性能与行数对照（2026-09-10）

## S29：GD 快照复用字段复制

本轮重点是实际运行的生成物，未计时 txgen 自身。生成的 `snapshotCopy` 原先新建 GD 并逐项复制标量字段；现在复用已有 `Copy()`，批量复制字段并清除内部 state，然后对原有非直接复制字段逐项深拷贝。集合、嵌套对象、JSON 兜底与错误返回分支保持不变。所有 snapshotCopy 调用均先排除 nil：公开入口 BeginChange 检查接收者，递归包装与集合循环检查元素。

例如 GDRole 的 RoleId、Name、Power 等字段由 Copy 一次复制，Items 和 DailyTask 仍递归隔离；快照不会携带原对象的 Store 状态。未新增公共抽象、反射或生产采集代码，也未改变 GD.Copy 本身的浅拷贝契约。

### 生成文件行数

| 工程 | 发生变化的生成文件 | 净行数 |
|---|---:|---:|
| txgen fixture | 12 | -25 |
| txgame server | 163 | -987 |
| txgameslg server | 7 | -27 |
| 合计 | 182 | -1039 |

上述只统计生成文件，不含模板、测试、文档及未提交 S25–S27。模板净减 1 行，删除旧逐项赋值形式的文本断言 1 行；标量变更及嵌套对象隔离仍由现有生成代码行为测试覆盖。TypeScript 产物没有变化。

### 运行时对照

以修改前真实 fixture 产物和重新生成后的产物分别预编译测试二进制，运行现有 `BenchmarkGeneratedPersistenceModes`。Go 1.27.0 darwin/amd64，GOMAXPROCS=1，各版本交错 6 轮、200ms；计时不含编译或生成。下表为中位数：

| 场景 | ns/op 前 → 后 | B/op 前 → 后 | allocs/op 前 → 后 |
|---|---:|---:|---:|
| SnapshotSlice32 | 13148.5 → 13052.5 | 8680 → 8680 | 128 → 128 |
| SnapshotScalar | 4368.5 → 4364.5 | 5488 → 5488 | 48 → 48 |
| SnapshotMap100 | 65109.5 → 62377 | 26882 → 26869 | 320 → 320 |
| StoreScalar（未修改路径对照） | 4001.5 → 3981.5 | 5216 → 5216 | 46 → 46 |
| StoreMap100（未修改路径对照） | 9311 → 9478.5 | 12848 → 12848 | 157 → 157 |

标量与切片快照基本持平，Map100 快照中位数下降约 4.2%，所有场景分配次数相同。map 分配字节的小幅差别及未修改 StoreMap100 对照的 +1.8% 体现测量波动，不视为固定内存收益或由本项引起的退化。本轮没有发现快照路径明确退化，但不承诺稳定提速或外推到未测输入；主要确认收益是生产生成代码的重复赋值减少。

### 验证与证据

- 重新生成 fixture、txgame 和 txgameslg，逐项检查生成 diff：仅包含 New 初始化改为 Copy 及删除标量赋值。没有手改产物。
- txgen 全量测试、两个 gen 工程全量测试、两个 server 全量测试通过。首次 txgen 回归发现旧文本断言要求 `value.Name = gd.Name`，删除该实现细节断言后全量重跑通过；保留标量、map、slice、嵌套指针、加载状态及模式隔离行为测试。
- 首次 txgame server 测试被默认沙箱的本地端口限制阻断；结束该轮后允许本地端口，两个 server 全量命令重跑通过。不将普通全量测试结果作为专门真实存储或多进程集群验收证据。本轮未另跑真实集群，客户端无产物变化，未重跑 npm 测试。
- 临时证据目录 `/private/tmp/tx-gd-snapshot-20260910` 保存前后二进制、`bench-before-*.txt`、`bench-after-*.txt`、生成 diff 和测试日志；所有采集与生产路径隔离。后置 ponytail-review 无新增简化项，diff 检查通过。

保留未提交 S25–S27 和计划调整，本轮不提交。下一项继续评估 StoreView 选择、捕获与集合子 Store 的生产重复实现；具体修改尚需调用与运行时证据，不重开已因性能成本否决的抽象。

## S30：叶子 GD 不生成空路径分派

保留未提交 S25–S29。本轮复用模型已有 HasViewNestedFields 标记：无生成对象指针、可跟踪 map 或 slice 的 GD，其 ResolveValue 在顶层字段解析后直接返回 `(nil, false)`，不再输出仅含 default 的 switch。该标记与原分派 case 的生成条件一致，未新增模型状态或公共 helper；所有路径校验和嵌套类型输出不变。

例如 GDItem 的普通字段仍按 FieldID 返回值，对无可解析子结构的深层路径仍返回失败。StoreView 与主 Store 继续共用同一解析器，未削弱非法路径、集合键范围或 nil 处理。

本轮重新生成后，fixture 4 个文件净减 12 行，txgame 78 个文件净减 234 行，txgameslg 3 个文件净减 9 行，共净减 255 行。累计 S29–S30 的三套生成物分别净减 37、1221、36 行，共 1294 行；不计生成器源码或文档。TypeScript 产物无变化。

复用现有真实生成结构的 StoreView 叶子路径基准，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，各版本交错 6 轮、100ms，基线是未提交 S29 产物。中位数如下：

| 场景 | ns/op 前 → 后 | allocs/op 前 → 后 |
|---|---:|---:|
| CaptureLeafAndBuildFrameDocument | 1854099.5 → 1798696.5 | 9156 → 9156 |
| CaptureLeafFrameOnly | 1037405 → 1044913.5 | 5391 → 5391 |
| BuildCapturedLeafFrameDocument | 763092.5 → 759480 | 3765 → 3765 |

分配次数不变，耗时变化约 -3.0%、+0.7%、-0.5%；分配字节中位数分别为 754852.5→754857.5、491807.5→491807、263052→263061.5。小幅 map 分配与耗时波动不作为固定成本变化，本项不宣称提速，收益是删除生产生成代码中的空分派。基准采集与源码快照仅在 `/private/tmp/tx-leaf-resolver-20260910`，不进入生产路径。

txgen 与两个 server 全量测试通过，server 验证允许本地回环端口。最终输出排除了嵌套类型的无关空行变化，并确认该格式调整与已测版本的所有非空代码行一致；随后重新运行 txgen 全量测试。未另跑真实集群或客户端测试。后置 ponytail-review 无新增简化项，diff 检查通过。下一项仍需评估具体生产候选，不因集合代码相似就引入有运行成本的新抽象。

## S31：字段路径 helper 复用候选未采用

基线为未提交 S29–S30 生成物。评估将 PathFieldName 的直接指针、map 元素和 slice 元素三个分支中的子路径构造替换为已有 tailStoreViewPath。入口已检查深度和索引，递归时深度大于 1；helper 在该范围下结果等价，但运行成本必须另测。

首轮现有真实文档基准交错 6 轮、200ms：Go 字段文档中位数 2094→2112ns（+0.9%），Msgpack 字段文档 2089.5→2147ns（+2.8%），已捕获叶子视图文档 928480→908805.5ns（-2.1%）；分配次数分别保持 15、15、3765。方向不一致，追加直接路径测量，不用整体文档耗时掩盖局部成本。

直接调用实际生成的 GDRolePathFieldResolver，路径深度 2、index=1，覆盖 Items、DailyTask、Dress。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，各版交错 10 轮、300ms，中位数如下：

| 分支 | 原直接构造 ns/op | helper 候选 ns/op | 变化 | 分配 |
|---|---:|---:|---:|---:|
| Items map 元素 | 27.345 | 47.145 | +72.4% | 均 0 B / 0 次 |
| DailyTask 指针 | 29.330 | 52.395 | +78.6% | 均 0 B / 0 次 |
| Dress slice 元素 | 30.170 | 51.310 | +70.1% | 均 0 B / 0 次 |

因此未采用候选，保留生成的直接子路径构造；不以减少三行代码换取该热路径的额外成本。上述仅定位到候选引入的局部退化，没有进一步 profile 来归因编译器、内联或值复制细节，也不外推为整体业务吞吐同比退化。其他现有 helper 调用没有在本项重新评估，不连带改写。

候选模板已恢复到本轮前快照，通过生成器重新生成 fixture，三套产物逐文件与本轮前快照完全一致。恢复后 txgen 全量测试通过；本轮没有修改最终生产代码，未重跑 server、客户端或真实集群。后置 ponytail-review 无新增简化项，diff 检查通过。生成物本轮净变化 0 行，累计 S29–S30 仍净减 1294 行。

证据在 `/private/tmp/tx-path-tail-20260910`：有效文档基准 `bench-*.txt`/`raw.json`，直接路径基准 `direct-*.txt`/`direct-raw.json`，旧源码及输入快照、overlay、脚本与恢复验证日志。`control-only` 是候选尚未应用时的同版本采样，明确不作为候选性能证据。测量逻辑不进入生产路径。

下一项需评估其他生成物候选；本项作为性能驱动的重复实现保留，不再次机械建议 helper 复用。

## S32：集合状态复制使用 maps.Clone

基线为未提交 S29–S30 产物（S31 未采用候选）。生成包基础层 storeCollectionOps.clone 保留空表返回 nil，非空表改用标准库 maps.Clone，删除手工分配和逐项复制。key 到 change.Op 的状态表不含需要深拷贝的业务对象；选择器和撤销记录仍使用独立表，不改变对象快照语义。

临时 overlay 在实际生成包中直接测 int 键状态表，检查空结果为 nil，修改副本不影响原表。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，各版交错 6 轮、200ms，预编译后计时，中位数如下：

| 项数 | 原实现 ns/op | maps.Clone ns/op | B/op（两版相同） | allocs/op（两版相同） |
|---|---:|---:|---:|---:|
| 0 | 1.2375 | 2.881 | 0 | 0 |
| 1 | 163.1 | 106.9 | 192 | 2 |
| 64 | 2692 | 661.65 | 2392 | 4 |
| 1024 | 40616 | 7326 | 36992 | 6 |

非空表耗时中位数分别下降约 34.5%、75.4%、82.0%；空表分支绝对增加约 1.64ns，保留并明确记录这个取舍，不宣称全路径提速。结果限于本机、当前 Go 版本与 int 键样本，不外推为完整业务吞吐提升，也不推断其他键型的精确百分比。

另运行既有 CaptureMergeAndBuildFrameDocument、CaptureFrameOnly、CaptureLeafFrameOnly 六轮对照：耗时中位数分别 +0.1%、-1.7%、-2.3%，分配字节与次数相同。这些基准使用 nil configure，不触发选择器复制，仅作为未修改路径对照，不能用于证明 maps.Clone 的收益。

从模板重新生成三套产物，仅各包 type_registry.go 增加 maps import 并替换复制循环。本轮各净减 3 行，共 9 行；累计生成物 fixture -40、txgame -1224、txgameslg -39，共净减 1303 行。模板另净减 3 行，不并入产物统计。

证据在 `/private/tmp/tx-ops-clone-20260910`：direct-*.txt/direct-raw.json 是有效复制基准，bench-*.txt/raw.json 是捕获对照；临时校验、overlay、二进制及日志均不进入生产路径。txgen 与两个 server 全量测试通过，保留现有视图隔离及事务撤销行为测试。未另跑客户端或真实集群；TypeScript 产物不变。后置 ponytail-review 无新增简化项，diff 检查通过。全部变更未提交，下一项仍需评估具体生产候选。

## S33：集合解绑 nil 判断评估后保留

六工程起始工作区均干净，S25–S32 已提交。当前 txgen 基线为 4ae32de，txgame 为 1b7d26a，txgameslg 为 1b13ead。本轮候选删除 map/slice 子 Store 的 detachStore 在 delete 前对 stores 和 dirtyChildren 的 nil 判断；Go 的 delete 对 nil map 是安全空操作，但语义冗余不等于运行成本冗余。

临时 overlay 直接测实际生成的 GDRoleItemsMapStore，覆盖 nil 表解绑，以及重新放入已有子项和脏标记后解绑。临时行为校验确认解绑后两个表不再包含该项、子 Store 的 dirty hook 被清除，重复解绑安全。两版预编译，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；先交错 6 轮、200ms，再交错 10 轮、300ms 复测：

| 场景 | 首轮 ns/op 前 → 后 | 加长复测 ns/op 前 → 后 | 分配 |
|---|---:|---:|---:|
| nil 表 | 4.4055 → 5.833 | 4.4845 → 5.8855 | 两版均 0 B / 0 次 |
| 已绑定子项 | 68.33 → 66.34 | 67.925 → 66.5 | 两版均 0 B / 0 次 |

空表路径稳定增加约 1.4ns（31%），已有子项路径减少约 1.4ns（2.1%）。没有真实调用占比证据，不能据此确认整体收益，候选未采用。结果限于该生成 map 子 Store 的两个样本，不外推到所有集合或业务吞吐。保留现有 nil 判断，不以标准库语义允许省略为唯一依据删代码。

已恢复本轮前模板，通过生成器重新生成 fixture；txgen 工作区恢复干净，txgame/txgameslg 未生成候选且保持干净。恢复后 txgen 全量测试通过。最终生产代码与产物无变更，本轮只记录取舍；未重跑 server、客户端或真实集群。后置 ponytail-review 无新增简化项，diff 检查通过。原始基准、overlay、校验、前后二进制与恢复日志仅位于 `/private/tmp/tx-detach-20260910`，生产路径无采集逻辑。

本轮净行数变化 0，已提交的 1303 行生成物删减不再计作本轮收益。下一项需评估其他生成物候选；对这类极小热路径优先判断实际成本，不自动去掉 nil 快路径。

## S34：无子 GD 的字段路径解析只生成一次返回

保留未提交 S33 文档，代码基线仍为 txgen 4ae32de、txgame 1b7d26a、txgameslg 1b13ead。对没有直接子 GD、map 子 GD 或 slice 子 GD 的类型，PathFieldName 原有 index==0 分支和空 switch 的 default 返回完全相同。模板按原三个 case 条件确定是否需要子解析器；不需要时保留深度/索引校验，仅生成一次字段名解析返回。没有扩大 HasViewNestedFields 的含义，也没有增加公共 API 或运行时状态。

例如 GDItem 的顶层节点和合法索引下的回退解析均调用 GDItemResolveFieldName；包含子 GD 的 GDRole 仍保留原直接子路径构造，不引入 S31 否决的 helper。非法深度、索引、FieldID 和命名方式仍由原校验与字段名解析处理。

临时 overlay 使用真实生成 GDItem，对深度 0/1/2/4/5、索引 -1/0/1/3/4 和 Go/JSON/BSON/Msgpack 名称进行前后行为校验；两版结果一致。直接解析基准为深度 2、JSON 字段名、index 0/1，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错 6 轮、200ms，中位数如下：

| index | 原实现 ns/op | 收敛后 ns/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| 0 | 5.710 | 4.173 | 两版 0 | 两版 0 |
| 1 | 5.7515 | 4.1345 | 两版 0 | 两版 0 |

该类型两个直接场景约快 27%–28%，不外推完整文档构建或业务吞吐；本项同时有生成代码减少的明确收益。

| 生成工程 | 改动文件数 | 本轮净行数 |
|---|---:|---:|
| txgen fixture | 5 | -30 |
| txgame server | 116 | -696 |
| txgameslg server | 4 | -24 |
| 合计 | 125 | -750 |

模板净增 2 行，不计入产物删减；已提交的 1303 行不再作为本轮收益。所有生成文件相对 HEAD 均精确匹配上述一次分支替换，无额外格式变化；有子 GD 类型保持不变，TypeScript 产物不变。

重新生成后 txgen 和两个 server 全量测试通过，server 测试允许本地回环端口。未另跑客户端或真实集群。后置 ponytail-review 无新增简化项，diff 检查通过。临时校验、overlay、原始基准和测试日志在 `/private/tmp/tx-leaf-field-20260910`，采集不进入生产路径。全部变更未提交；下一项需继续评估具体生成物候选。
