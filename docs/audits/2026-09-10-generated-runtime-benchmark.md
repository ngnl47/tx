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

## S35：合并相同的序列化字段名分支

六工程起始干净，S33–S34 已提交。本轮基线为 txgen ec27ed3、txgame 0da825e、txgameslg 5a3590a。模板复用 HasBSONNameOverrides/HasMsgpackNameOverrides 标记，将无覆盖的命名方式并入 JSON case；有覆盖时保留原独立分支、覆盖字段匹配和 JSON 名回退。没有增加运行时状态或间接调用。

例如 GDItem 的 JSON/BSON/Msgpack 名称均为 itemId，只生成一次数组读取；Go 名称仍为 ItemId。显式 BSON/Msgpack 名称不同的类型不合并其覆盖分支。

临时 overlay 对照 12 个发生变化的 fixture 解析器旧实现：FieldID 0–127，Go/JSON/BSON/Msgpack、空值与未知命名方式，输出完全一致。实际 GDItem 的直接解析基准为字段 1，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错 6 轮、200ms，中位数如下，分配均为 0 B / 0 次：

| 命名方式 | 原实现 ns/op | 收敛后 ns/op |
|---|---:|---:|
| Go | 1.5485 | 1.324 |
| JSON | 2.3105 | 1.833 |
| BSON | 1.7865 | 1.8605 |
| Msgpack | 1.796 | 2.085 |

Go/JSON 降低，BSON/Msgpack 分别增加约 0.074ns/0.289ns（约 4%/16%）。明确保留这个局部取舍，不宣称全路径提速；主要收益为静态生成分支收敛且无新增分配。结果限于当前类型和机器，未测完整业务吞吐，不外推所有名称覆盖组合的性能。

| 生成工程 | 改动文件数 | 本轮净行数 |
|---|---:|---:|
| txgen fixture | 12 | -46 |
| txgame server | 160 | -634 |
| txgameslg server | 7 | -24 |
| 合计 | 179 | -704 |

模板净行数不变，四条要求独立 case 形式的旧文本断言及随之相邻的两条重复返回断言已移除；字段名、序列化标签、覆盖规则、未知输入和协议文档的行为测试保留。重新生成后逐文件检查，差异仅限 ResolveFieldName，其他生成内容和 TypeScript 产物不变。已提交历史删减不计入本轮。

首次全量回归因旧 case 形式断言失败，修正后重新运行 txgen 和两个 server 全量测试通过；server 验证允许本地回环端口。未另跑客户端或真实集群。后置 ponytail-review 无新增简化项，diff 检查通过。证据在 `/private/tmp/tx-field-kinds-20260910`，包括旧函数对照、overlay、原始基准及测试日志；采集不进入生产路径。全部变更未提交，下一项尚需评估具体生成物候选。

## S36：完整加载位图按字置位

保留未提交 S35。本轮修改各生成包共用的 markAllStoreFieldsLoaded：完整 64 位字一次置满，末尾不足 64 位时只 OR 有效位，替代逐字段移位和 OR。字段数为生成类型的非负字段数量，位图按该数量分配；额外位图字和尾字已有高位保持原值，部分字段加载不受影响。

实际生成包的临时旧实现对照覆盖字段数 0/1/10/63/64/65/127/128/129/1024，并以零值和交错位预填充、附加一个哨兵字，结果一致。基准使用 Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错 6 轮、100ms，分配均为 0 B / 0 次：

| 字段数 | 原逐字段 ns/op | 按字置位 ns/op |
|---|---:|---:|
| 0 | 1.292 | 1.796 |
| 1 | 1.773 | 2.290 |
| 10 | 12.000 | 2.275 |
| 63 | 74.430 | 2.277 |
| 64 | 76.750 | 2.355 |
| 65 | 76.125 | 2.791 |
| 128 | 145.700 | 3.083 |
| 1024 | 1131.500 | 6.6915 |

按字段规模显著减少位图操作；0/1 字段分支约增加 0.5ns，保留并记录取舍，不宣称所有字段规模提速。

再对现有真实生成 GDRole 的完整加载入口交错 6 轮、200ms：Single 中位数 2374.5→2403.5ns（+1.2%），BatchByID 2719→2657ns（-2.3%），BatchByFilter 2670→2690.5ns（+0.8%）；分配分别保持 736 B/17 次、904 B/22 次、904 B/22 次。loader 为测试替身，该结果包含解码与生成加载流程，不含真实网络或存储 I/O。未测出明确整体提速，不将局部位图百分比外推到加载或业务吞吐。

本轮模板与三套生成基础层各增加 3 行；生成物合计净增 9 行，主要收益为运行时计算减少。累计未提交 S35–S36 生成物净减 fixture 43、txgame 631、txgameslg 21，共 695 行；不以模板或产物删行替代性能判断。

重新生成三套产物后，txgen 与两个 server 全量测试通过，保留完整/部分加载、已加载字段状态及业务回归。TypeScript 产物不变，未另跑客户端或真实集群。后置 ponytail-review 无新增简化项，diff 检查通过。临时边界校验、overlay、前后二进制、bench-*.txt/raw.json、load-*.txt/load-raw.json 和测试日志在 `/private/tmp/tx-loaded-bits-20260910`，采集不进入生产路径。全部变更未提交，下一项尚需评估。

## S37：已加载字段查询直接读取名称表

六工程起始干净，S35–S36 已提交。代码基线为 txgen 1cad96d、txgame 3847c8f、txgameslg 466a080。本轮 loadedStoreFields 接收生成的 Go 名称表，按同一索引检查位图并追加名称，删除逐项 FieldID 转换、重复边界检查与固定 Go 命名回调。名称表含首项空哨兵，位图容量来自同一类型；不新增用户输入契约。

例如只加载 RoleId、Name、Items 时仍按声明顺序返回这三个 Go 名称。非 nil Store 的空结果保持原有非 nil 空切片行为；nil Store 仍由公开方法直接返回 nil。结果仍独立分配，不返回内部表的切片视图。

临时实际生成包旧实现对照覆盖字段数 0/1/10/63/64/65/128/129，空、首位、交错位和全位模式，顺序与结果一致；修改返回切片后重复查询不受影响。GDRole 实际 LoadedFields 基准使用其 10 字段布局，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错 6 轮、200ms：

| 已加载模式 | 原实现 ns/op | 收敛后 ns/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| 空 | 90.74 | 78.655 | 两版 160 | 两版 1 |
| 首字段 | 93.59 | 81.35 | 两版 160 | 两版 1 |
| 全部 10 字段 | 143.7 | 108.55 | 两版 160 | 两版 1 |

该查询样本约快 13%–24%，分配不变，不外推完整加载或业务吞吐。此次保留原有按总字段数预留结果容量，不同时改变返回分配策略。

从模板重新生成，fixture 13、txgame 164、txgameslg 8 个文件调整内部调用及共用 helper；三套生成物各净减 1 行，共 3 行。模板另净减 1 行，不计入产物。TypeScript 产物不变，已提交历史删减不计入本轮。

重新生成后 txgen、两个 server 全量测试通过，保留加载顺序、缺失字段、部分加载和对象代理行为测试。后置 ponytail-review 无新增简化项，diff 检查通过。未另跑客户端或真实集群。临时旧源码、边界校验、overlay、bench-*.txt/raw.json 和测试日志在 `/private/tmp/tx-loaded-names-20260910`，采集不进入生产路径。全部变更未提交，下一项尚需评估具体候选。

## S38：已加载字段结果按有效位数量分配

保留未提交 S37，本轮以其名称表直读实现为基线。先用 math/bits.OnesCount64 统计有效加载位，再按实际结果数量分配切片；尾字屏蔽字段数以外的位。结果仍按声明顺序复制到独立切片，非 nil Store 的空结果仍为非 nil 空切片，nil Store 的入口行为不变。

例如 GDRole 共 10 字段，只加载一个字段时不再预留 10 个字符串位置；完全未加载时不创建后备数组。代价是非空查询先经过位计数，明确测量完整加载场景的额外工作。

临时旧实现对照覆盖 0/1/10/63/64/65/128/129 字段，空、首位、交错和全位模式，内容、顺序与副本隔离一致。另对跨字和尾字填满高位的场景检查结果容量等于有效字段数量，零字段结果保持非 nil，校验通过。

实际生成 GDRole 的直接查询，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错 6 轮、200ms，中位数如下：

| 加载模式 | S37 ns/op → S38 ns/op | B/op 前 → 后 | allocs/op 前 → 后 |
|---|---:|---:|---:|
| 空 | 78.595 → 17.98 | 160 → 0 | 1 → 0 |
| 首字段 | 81.785 → 41.93 | 160 → 16 | 1 → 1 |
| 全部 10 字段 | 109.15 → 115 | 160 → 160 | 1 → 1 |

空/单字段查询约快 77%/49%，完整加载查询增加约 5.85ns（5.4%）；采用空与稀疏查询的分配节省，同时记录全字段成本。不宣称所有查询提速，不推断实际调用分布，也不将 S37 与 S38 的百分比相加。

本轮模板净增 9 行，三套生成基础层各净增 9 行，共 27 行；累计未提交 S37–S38 三套生成物各净增 8 行，共 24 行。接受少量代码增长以降低生产查询分配，不以删行数为目标。其余生成代码仅保留 S37 的调用调整，TypeScript 产物不变。

重新生成后 txgen、两个 server 全量测试通过；临时容量和边界检查通过。后置 ponytail-review 无新增简化项，diff 检查通过。未另跑客户端或真实集群。旧源码、overlay、临时校验、bench-*.txt/raw.json 及测试日志在 `/private/tmp/tx-loaded-capacity-20260910`，采集不进入生产路径。全部变更未提交，下一项尚需评估具体候选。

## S39：空字段查询直接返回及累计验收

保留未提交 S37–S38；有效加载位计数为零时直接返回非 nil 空切片，省去名称表扫描。nil Store 继续返回 nil；例如一个字段也未加载的 GDRole，结果仍为空且非 nil。非空结果保留声明顺序和副本隔离，无新增公共接口或采集逻辑。

本轮对已提交 HEAD（txgen 1cad96d）、S38、本轮 S39 三版实际生成 GDRole 查询重新测量，采用 Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后二百毫秒六轮，三版本轮换并反转执行顺序。每轮同时执行 nil/空/首字段、副本隔离和无效高位检查。中位数如下：

| 加载模式 | HEAD ns/op | S38 ns/op | S39 ns/op | HEAD → S39 B/op | HEAD → S39 allocs/op |
|---|---:|---:|---:|---:|---:|
| 空 | 88.85 | 18.40 | 5.1775 | 160 → 0 | 1 → 0 |
| 首字段 | 92.645 | 42.645 | 43.645 | 160 → 16 | 1 → 1 |
| 全部 10 字段 | 147.95 | 117.35 | 119.25 | 160 → 160 | 1 → 1 |

S38 到 S39 空查询约快 72%，非空查询分别增加约 1.00ns（2.3%）和 1.90ns（1.6%），分配不变。保留该分支以消除空查询的逐字段扫描。相对已提交 HEAD，累计空/首字段/全字段约快 94%/53%/19%；这是本轮直接对照，不累加历史百分比，不外推业务吞吐或真实调用分布。

本轮重新运行临时旧实现等价检查，覆盖 0/1/10/63/64/65/128/129 字段、空/首位/交错/全位，以及尾字高位和结果容量，全部通过。模板重新生成 fixture、txgame 和 txgameslg，差异检查确认仅改动 LoadedFields 内部调用及公共生成 helper。txgen、txgame/server、txgameslg/server 本轮全量测试通过；未另跑客户端或真实集群测试。

本轮模板增加 3 行，三套生成物各增加 3 行，共 9 行；累计未提交 S37–S39 三套生成物各净增 11 行，共 33 行。TypeScript 输出和两个客户端无改动。后置 ponytail-review 无新增简化项，diff 检查通过。采集脚本、HEAD 源码快照、三版 overlay、原始 bench-*.txt/results.json、边界与全量验证日志保留于 `/private/tmp/tx-loaded-empty-20260910`，不进入生产路径。

全部变更未提交，提交建议累计 S37–S39。已加载字段查询这组关联修改完成；下一项需要评估其他 GD/StoreView 生产生成物候选，保留此前被基准否决的直接路径实现。

## S40：字段索引查找使用标准库

保留未提交 S37–S39，storeFieldIndex 使用 slices.Index，并以 max(index, 0) 将未命中的 -1 映射为既有 FieldNone。名称表首项仍为哨兵，公开 FieldIndex 入口和首次匹配语义保持不变；例如查询不存在的名称继续返回 0。只改变生成基础层，不增加公共抽象。

临时生成包 overlay 对照 nil/空表、哨兵、重复名称、命中及未命中行为，两版一致。以下直接测量生成包内部 helper，使用 10/129 字段合成名称表，不能当作实际业务吞吐。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后两版交错六轮、每项 200ms，中位数如下；全部样本两版均为 0 B/op、0 allocs/op。

| 字段数 / 查询 | S39 ns/op | S40 ns/op |
|---|---:|---:|
| 10 / 首项 | 3.484 | 3.5875 |
| 10 / 末项 | 7.979 | 7.8705 |
| 10 / 未命中 | 10.735 | 11.185 |
| 129 / 首项 | 3.474 | 3.5745 |
| 129 / 末项 | 143.1 | 144.2 |
| 129 / 未命中 | 355.8 | 354.15 |

10 字段未命中增加约 0.45ns（4.2%），129 字段末项增加约 1.10ns（0.8%）。接受这些样本变化以减少手写查找代码，不宣称提速。脚本、源码快照、overlay、原始结果与测试日志在 `/private/tmp/tx-field-index-20260910`，不进入生产路径。

重新生成 fixture 和两个示例后，txgen、txgame/server、txgameslg/server 本轮全量测试通过。模板和每套生成基础层各净减 4 行，三套生成物本轮共减 12 行，累计未提交 S37–S40 净增 21 行。两个客户端无变化，未另跑客户端或真实集群测试。后置 ponytail-review：Lean already. Ship.；diff 检查通过。本轮未提交，下一项具体生产生成物候选仍需评估。

## S41：全字段脏状态判断使用标准库

保留未提交 S37–S40。本轮评估 allStoreFieldsDirty 与 containsStoreField 两个手写循环；最终仅保留前者的 slices.Contains 替代，并保留 len(ops) != 0 的空集合契约。公开 AllDirty 入口及 nil Store 行为不变，无新增依赖；例如 AllTouch 后为 true，取消一个字段后回到 false。

containsStoreField 替代被否决并恢复原实现：六轮候选基准中，129 字段末项查询 119.75→140.10ns，未命中 327.90→354.30ns，分别慢约 17%/8%；10 字段未命中 4.105→6.729ns。不能仅为减少五行循环承担该退化。候选原始数据为 results.json/bench-*.txt，保留用于解释拒绝理由。

恢复包含判断后重新编译最终实现，再与 S40 两版交错六轮，每项 200ms、Go 1.27.0 darwin/amd64、GOMAXPROCS=1。以下直接测量生成包 helper，合成 0/10/129 字段状态数组，全部为 0 B/op、0 allocs/op；不是完整业务吞吐。

| 字段数 / 状态 | S40 ns/op | S41 ns/op |
|---|---:|---:|
| 0 / 空 | 1.4235 | 1.543 |
| 10 / 全脏 | 6.379 | 4.5785 |
| 10 / 首项干净 | 1.3025 | 1.5415 |
| 10 / 末项干净 | 4.6405 | 4.1175 |
| 129 / 全脏 | 54.835 | 46.97 |
| 129 / 首项干净 | 1.304 | 1.545 |
| 129 / 末项干净 | 49.81 | 49.085 |

空数组的另外两个模式仍是同一输入，原始记录保留，不作为独立收益。全脏样本约快 28%/14%；首项短路增加约 0.24ns（约 18%），接受这一绝对成本以简化代码并改善完整扫描，不宣称所有场景提速，也不推断实际调用分布。

临时对照覆盖 nil/空操作表、首尾干净、全脏及字符串查询边界；新增一条公开 AllDirty 状态转换测试，覆盖 nil/初始 Store、AllTouch 和取消单字段，不绑定内部循环。测量与旧实现快照、overlay、日志均在 `/private/tmp/tx-field-predicates-20260910`；最终测量为 final-results.json/final-*.txt，采集不进入生产路径。

新增公开状态测试在旧实现上通过；重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。最终生成差异确认包含判断已恢复。模板净减 8 行，三套生成物各净减 8 行，共减 24 行；另新增行为测试 16 行。累计未提交 S37–S41 生成物净减 3 行。后置 ponytail-review 无新增简化项，diff 检查通过；两个客户端无变化，未另跑真实集群。全部未提交，下一项其他生产生成物候选仍需评估，不重开本轮已否决的包含判断替代。

## S42：Store 操作码读取与恢复复用已有 helper

保留未提交 S37–S41。Store 的 op 与 restoreOp 分别复用既有 storeViewFieldOp / setStoreViewFieldOp；数组布局与边界规则一致，保留 nil Store 的入口检查。恢复仍只设置操作码、不触发脏通知；例如撤销将某字段恢复为原操作码，不另行通知父 Store。没有新增 helper、公共接口或参数。

初始候选还让 recordOp 复用 setter，但实际生成 GDItemStore 六轮局部基准中，无 hook 的有效记录从 2.0655→2.5765ns，有 hook 从 2.544→3.035ns，分别增加约 25%/19%。即使 GDRole SetName 样本没有退化，也不能据此忽略独立记录路径；已恢复 recordOp 的直接实现，仅保留读取与恢复收敛。

恢复后重新编译最终版本，与 S41 交错六轮、每项 100ms，Go 1.27.0 darwin/amd64、GOMAXPROCS=1。测试实际生成 GDItemStore 的 nil/无 hook/有 hook 三种状态及字段 0/2/4 的读取、记录、恢复，并测量 GDRole SetName；所有样本两版均 0 B/op、0 allocs/op。代表性中位数如下：

| 场景 | S41 ns/op | S42 ns/op |
|---|---:|---:|
| 无 hook / 有效字段读取 | 1.294 | 1.2905 |
| 无 hook / 有效字段恢复 | 1.3285 | 1.3545 |
| 无 hook / 有效字段记录 | 2.075 | 2.0475 |
| 有 hook / 有效字段恢复 | 1.307 | 1.2800 |
| 有 hook / 有效字段记录 | 2.6285 | 2.5695 |
| GDRole SetName | 6.2105 | 6.1295 |

最终读取与恢复各样本中位数变化约 −0.027ns 至 +0.043ns，未见明显退化，不宣称业务吞吐提升。临时契约测试覆盖 nil、无效字段、有效记录、恢复和清除，检查状态与脏通知次数，候选及最终两版均通过。证据保留在 `/private/tmp/tx-store-ops-20260910`：results.json 为被筛选候选，final-results.json/final-*.txt 为最终版本；旧源码、overlay 与测试日志同目录，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过，保留既有嵌套 Store、集合与事务撤销回归。本轮模板净减 8 行；fixture 12 个 Store 减 96 行，txgame 163 个减 1304 行，txgameslg 7 个减 56 行，共减 1456 行。累计未提交 S37–S42 生成物净减 1459 行；本轮无新增正式测试或客户端改动，未另跑真实集群。生成范围检查确认 recordOp 已恢复，差异仅含预期操作码复用及此前字段查询改动。后置 ponytail-review 无新增简化项，diff 检查通过；全部未提交，下一项仍需评估其他具体生产候选。

## S43：HasChanges 收敛候选否决

保留未提交 S37–S42，评估两项关联修改：顶层操作码扫描复用已有 storeViewFieldOpsHaveChanges，以及在顶层扫描已确认全部为零后，删除子 Store 前重复的 rootOp 覆盖检查。逻辑上后者冗余，但性能证据未支持保留。

临时实际生成包对照覆盖 nil、干净、顶层首尾字段变脏、指针/map/slice 子 Store 变脏及已创建但干净的子 Store，行为一致。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，每项 200ms，两版交错六轮，所有样本零分配。第一版同时实施两项时，GDRole 干净查询 7.0035→9.7035ns，末字段变脏 4.4645→7.297ns，否决 helper 复用。

随后恢复直接扫描，仅删除重复覆盖检查，并重新编译交错测量六轮。以下为第二版候选的中位数，仍未保留：

| 查询 | S42 ns/op | 第二版候选 ns/op |
|---|---:|---:|
| GDRole 干净 | 7.079 | 7.213 |
| GDRole 指针子节点变脏 | 6.9265 | 6.073 |
| GDRole map 子节点变脏 | 6.3135 | 6.4315 |
| GDRole 已创建但干净的子节点 | 18.355 | 17.19 |
| GDItem 干净 | 3.328 | 4.144 |
| GDItem 末字段变脏 | 2.081 | 3.3725 |

GDItem 两项约慢 25%/62%，不能用 GDRole 局部收益覆盖。原始基准将 GDItem 用例命名为 leaf-clean/leaf-dirty，但 GDItem 含 Ext 集合 Store，不代表没有子节点的类型。未进一步证明机器码变化原因，不将退化归因于特定编译器优化；本轮全部恢复，不为了删判断引入额外生成条件或强制内联策略。

恢复后重新生成 fixture，与本轮起始快照逐文件完全一致；txgen 全量测试重新通过。两个示例和客户端本轮未修改，未重跑 server、客户端或真实集群测试。后置 ponytail-review 检查取舍记录，无新增简化项，diff 检查通过。本轮保留代码净变化 0 行，累计未提交 S37–S42 生成物仍净减 1459 行，提交建议包含此前代码和本轮 S43 取舍记录。

证据在 `/private/tmp/tx-has-changes-20260910`：before.tmpl 是起始版本，candidate-helper.tmpl / candidate-checks.tmpl 为被否决的两版模板；results.json 为第一版，final-results.json 为第二版候选（不是交付代码），bench-*.txt/final-*.txt 为原始输出，tests-restored.log 为恢复后验证。采集与试验不进入生产路径。下一项需评估其他生成物候选，不再重复这组 HasChanges 抽取。

## S44：嵌套字段判断合并静态 case

六工程起始均干净，S37–S43 已提交；本轮基线为 tx bbd3053、txgen dd6a695、txgame f3e6a93、txgameslg a7b3aa3。StoreView 的 hasNestedField 原先对每个嵌套字段重复生成 case 与 return true，本轮静态合并为一个多值 case，保留原字段集合与 default false；无新增运行时循环、公共接口或依赖。例如 GDRole 的 Items/DailyTask/Heroes/Tags/Dress 共用同一返回分支，普通标量和无效字段仍返回 false。

实际生成 GDRole helper 的旧/新两版临时检查覆盖字段 0–11，结果一致；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后二百毫秒六轮交错测量，全部样本两版均 0 B/op、0 allocs/op：

| 字段编号 | 原实现 ns/op | 合并后 ns/op |
|---|---:|---:|
| 0（未命中） | 2.1735 | 1.319 |
| 6（Items） | 1.3005 | 1.425 |
| 8（Heroes） | 1.200 | 1.423 |
| 10（Dress） | 1.684 | 1.4125 |
| 11（未命中） | 2.3135 | 1.279 |

Items/Heroes 命中增加约 0.12/0.22ns（约 10%/19%），两个默认分支样本更快；接受少量命中成本以减少生成分支重复，不宣称所有查询提速，不外推完整视图或业务吞吐。脚本、旧源码、overlay、bench-*.txt/results.json 和验证日志位于 `/private/tmp/tx-nested-cases-20260910`，不进入生产路径。

重新生成后三个 Go 工程的本轮全量测试通过：txgen、txgame/server、txgameslg/server。逐文件比较确认变化只包含同值 case 合并，字段集合不变。fixture 2 个文件净减 10 行，txgame 41 个文件净减 248 行，共减 258 行；模板净行数不变，SLG 与两个客户端生成物无变化。后置 ponytail-review 无新增简化项，diff 检查通过；未另跑客户端或真实集群。全部本轮变更未提交，提交建议仅含 S44，不累计已提交的 S37–S43；下一项具体候选尚需评估。

## S45：无嵌套追踪字段的视图裁剪 HasChanges 分支

保留未提交 S44。复用既有 HasViewNestedFields 生成条件：无可追踪嵌套字段的 StoreView 将 HasChanges 生成成 nil 检查与操作码扫描的布尔表达式，含嵌套追踪字段的实现逐字保留。此处指视图追踪结构，不代表业务 GD 没有集合字段。没有新增运行时分类、参数、helper 或接口；例如空视图仍为 false，任意字段变脏为 true，清除后恢复 false。

实际生成 viewStateGDItem 的旧/新两版临时检查覆盖 nil、干净、逐字段变脏与清除，行为一致；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后 200ms 六轮交错测量，全部样本两版均 0 B/op、0 allocs/op：

| 状态 | S44 ns/op | S45 ns/op |
|---|---:|---:|
| nil | 1.3095 | 1.318 |
| 干净 | 3.7345 | 3.777 |
| 首字段变脏 | 1.303 | 1.1905 |
| 末字段变脏 | 3.393 | 3.322 |

变化约 −0.11 至 +0.04ns，未见明显退化；本轮目的为减少生成分支，不宣称业务提速。脚本、旧源码、overlay、bench-*.txt/results.json 与回归日志位于 `/private/tmp/tx-leaf-view-20260910`，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。逐文件验证确认仅无嵌套追踪字段的 HasChanges 被裁剪，嵌套类型只保留 S44 已有改动。本轮 fixture 4 个视图减 24 行、txgame 78 个减 468 行、txgameslg 3 个减 18 行，共减 510 行，模板增加 1 行。累计未提交 S44–S45 三套生成物净减 768 行；客户端无变化，无新增正式测试或生产采集，未另跑真实集群。后置 ponytail-review 无新增简化项，diff 检查通过；本轮未提交，提交建议累计 S44–S45，下一项具体候选尚需评估。

## S46：无嵌套追踪字段视图的 hasField 分支裁剪

保留未提交 S44–S45。按已有 HasViewNestedFields 条件，将无嵌套追踪字段视图的 hasField 生成成 nil 检查与操作码读取的布尔表达式；含嵌套追踪字段的实现不变。字段未变脏、编号无效或视图为 nil 时仍为 false；例如字段 2 变脏后仅该字段返回 true。

同轮尝试将 contains 的外层路径校验并入已有 leaf helper，但候选基准中，空路径 10.35→16.44ns，nil 视图合法路径 4.504→5.1815ns；虽非 nil 合法路径 22.29→18.375ns 更快，仍因缺少真实调用分布而恢复 contains。未引入分支特例或削弱路径校验。

恢复 contains 后重新编译最终版本，与 S45 六轮交错测量，每项 200ms、Go 1.27.0 darwin/amd64、GOMAXPROCS=1。实际生成 viewStateGDItem 的 hasField 中位数如下，两版全部 0 B/op、0 allocs/op：

| 视图 / 字段 | S45 ns/op | S46 ns/op |
|---|---:|---:|
| nil / 0 | 1.072 | 1.0925 |
| nil / 2 | 1.0685 | 1.081 |
| nil / 4 | 1.062 | 1.075 |
| 字段 2 变脏 / 0 | 1.300 | 1.2925 |
| 字段 2 变脏 / 2 | 1.309 | 1.2875 |
| 字段 2 变脏 / 4 | 1.3135 | 1.2945 |

变化约 −0.02 至 +0.02ns，未见明显退化，不宣称业务提速。临时前后对照覆盖 nil、有效及无效字段、空/有效/多层/带 key 路径，候选及最终实现均符合原有结果。证据在 `/private/tmp/tx-leaf-queries-20260910`：results.json/bench-*.txt 为包含路径改动的被筛选候选；final-results.json/final-*.txt 为最终字段查询版本，旧源码、overlay 和测试日志同目录。采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。范围检查发现并恢复嵌套类型无关的空白行变化后，重新生成三套产物，并再次通过 txgen 全量测试；最终逐文件比较确认只含已确认的 S44–S46 变更，contains 与嵌套类型 hasField 保持原实现。

本轮 85 个视图的 hasField 共减 425 行（255 行 nil 分支、170 行该分支移除后多余空白）；fixture / txgame / txgameslg 分别减 20/390/15 行，模板净行数不变。累计未提交 S44–S46 三套生成物净减 1193 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S44–S46；下一项具体候选尚需评估，不重复被否决的路径校验合并。

## S47：视图捕获源使用已知具体类型

保留未提交 S44–S46。capture 中 source 的两个赋值来源分别是接收者 s 与 selectionCopy，均为同一生成 Store 指针；将局部 change.Source 声明改为 source := s。选择逻辑、公开 change.Source 实现与 Visitor 契约保留，不删除 SPI，不引入新接口或 wrapper。例如选择 Quantity 时仍只捕获该字段，捕获值与后续 Store 修改隔离，Capture 仍可向同源视图追加。

临时前后检查覆盖全量捕获、字段选择、值隔离和追加捕获，全部通过。实际生成 GDItemStore.View 完整调用（根 identity 在计时前初始化），Go 1.27.0 darwin/amd64、GOMAXPROCS=1，每项 200ms 六轮交错，中位数如下：

| 源状态 / 选择 | S46 ns/op | S47 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 干净 / 无选择回调 | 91.915 | 94.8 | 96 | 3 |
| 干净 / 选择 Quantity | 241.4 | 233.05 | 160 | 4 |
| 变脏 / 无选择回调 | 297 | 282.75 | 96 | 3 |
| 变脏 / 选择 Quantity | 238.3 | 240.65 | 160 | 4 |

耗时变化约 −4.8% 至 +3.1%，分配不变；接受干净全量捕获约 +2.89ns 的样本成本，以明确局部实际类型。不宣称所有场景提速，亦未证明旧版编译器去虚拟化情况或将全部变化归因于接口派发。旧源码、overlay、基准脚本、bench-*.txt/results.json 与验证日志位于 `/private/tmp/tx-capture-source-20260910`，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。最终逐文件比较确认仅含此前 S44–S46 变更及本轮 source 类型替换：fixture 12、txgame 163、txgameslg 7 个 Store 调整，模板及生成物净行数不变。累计未提交 S44–S47 生成物仍净减 1193 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群，无新增正式测试或生产采集。本轮未提交，提交建议累计 S44–S47；下一项具体候选尚需评估。

## S48：可直接复制的集合元素不生成快照错误分支

保留未提交 S44–S47。复用 MapValueSnapshotDirect / SliceValueSnapshotDirect，视图复制对可直接复制的元素生成赋值，不再调用仅返回 value, nil 的 snapshotClone…Item，并停止生成这些私有 helper。标记与可复制类型规则不变；需深拷贝的 GD 指针和 JSON 兜底仍保留错误处理。目标 map 分配、slice 扩容、源元素存在性检查及删除行为保持不变。例如复制 Tasks[1] 的整数或 Tags[1] 的字符串仍写入独立目标容器，不共享容器。

临时前后对照覆盖 map/slice 复制、目标分配/扩容、缺失源元素错误、map 删除及容器隔离，全部通过。实际生成 copyGDDailyTaskStoreViewPath / copyGDRoleStoreViewPath 两版预编译后交错六轮，每项 200ms、Go 1.27.0 darwin/amd64、GOMAXPROCS=1，中位数如下：

| 场景 | S47 ns/op | S48 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map 目标已存在 | 17.13 | 16.945 | 0 | 0 |
| slice 目标已存在 | 8.7175 | 8.705 | 0 | 0 |
| map 每次分配目标 | 113.6 | 112.75 | 192 | 2 |
| slice 每次扩容目标 | 50.915 | 50.42 | 32 | 1 |

变化约 −1.1% 至 −0.1%，分配不变；主要收益是减少生成 helper 与不可能返回错误的分支，不外推业务吞吐。证据保留在 `/private/tmp/tx-direct-elements-20260910`，包含旧源码、overlay、scope-before.json、bench-*.txt/results.json 与测试日志；测量不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过，保留原有深拷贝、快照和视图可靠性回归。逐文件从本轮起始快照替换对照，确认仅删除直接返回 helper 并改写对应调用，其他代码一致。本轮删除 97 个直接返回 helper，fixture 2 文件减 14 行，txgame 50 文件减 649 行，txgameslg 1 文件减 7 行，共净减 670 行；模板净行数不变。累计未提交 S44–S48 生成物净减 1863 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S44–S48，下一项具体候选尚需评估。

## S49：直接复制 map 元素省去中间别名

保留未提交 S44–S48。对已标记为可直接复制的 map 元素，去掉 cloned := item，在完成目标容器初始化后直接写入已读取的 item；非直接复制元素保留 cloned 与错误处理。没有改变源元素读取顺序、缺失校验或容器分配。

slice 的 cloned 临时值保留：源/目标可能共享底层数组，必须在目标扩容写零前取得源元素。本轮临时检查以源 Tags 为 [a,b]、目标 Tags 为同一数组的前一项，复制第二项后仍为 b，验证保留顺序的意义；不把 map 的别名消除机械扩展到 slice。

实际生成 copyGDDailyTaskStoreViewPath 两版预编译后交错六轮，每项 200ms、Go 1.27.0 darwin/amd64、GOMAXPROCS=1，中位数如下：

| 场景 | S48 ns/op | S49 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 已有目标 map | 16.71 | 16.63 | 0 | 0 |
| 每次新建目标 map | 111.95 | 112.70 | 192 | 2 |

变化约 −0.5% 至 +0.7%，分配不变，不宣称提速。前后 map/slice 复制、缺失元素、删除及容器隔离对照通过；采集、旧源码、overlay、scope-before.json、bench-*.txt/results.json 及测试日志位于 `/private/tmp/tx-map-copy-value-20260910`，不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。逐文件增量对照确认仅省去 map 中间别名并改变对应赋值，slice 及非直接复制链路不变。本轮 fixture 减 1 行、txgame 27 个文件减 57 行、txgameslg 减 1 行，生成物共净减 59 行，模板净减 1 行。累计未提交 S44–S49 生成物净减 1922 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S44–S49；下一项具体候选尚需评估。

## S50：新建子 Store 直接继承 selector

六工程起始均干净，S44–S49 已提交；基线为 tx dba0332、txgen 2a2c272、txgame a94f71d、txgameslg f55d4bf。本轮只在新建或重绑子 Store 的分支中，将 if parent.selector { child.selector = true } 改为布尔直接赋值。NewStore 保证返回新对象，selector 默认 false；缓存复用分支不变，undo recorder 与 dirty hook 装配顺序保留。指针、map、slice 三条链路分别生成，不合并独立示例或增加公共抽象。

临时实际 GDRole 检查覆盖 selector 为 false/true 时的子 Store 创建、缓存复用与值变化重绑，结果一致。例如选择器父 Store 新建子节点后仍限制业务值写入，普通父 Store 新建子节点保持普通模式；未改变模式检查入口。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译后交错六轮、每项 200ms。新建样本在循环内清除对应缓存槽，map/slice 容器在计时前创建；数字包含槽位清理和子节点创建成本，不是整个业务请求。中位数如下：

| 新建路径 / selector | 原 ns/op | 修改后 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 指针 / false | 68.005 | 68.515 | 96 | 2 |
| map / false | 87.385 | 86.285 | 88 | 2 |
| slice / false | 87.3 | 85.9 | 88 | 2 |
| 指针 / true | 68.2 | 68.415 | 96 | 2 |
| map / true | 86.22 | 87.135 | 88 | 2 |
| slice / true | 86.08 | 87.145 | 88 | 2 |

六个缓存复用样本均零分配，中位数变化约 −0.104 至 +0.116ns；新建样本约 −1.4 至 +1.1ns，未见明显退化，不宣称业务提速。证据位于 `/private/tmp/tx-child-selector-20260910`，含旧源码、overlay、契约测试、bench-*.txt/results.json 和回归日志，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。逐文件从 HEAD 对照，确认仅将 123 处新建分支改为直接赋值；fixture 7 文件减 22 行、txgame 47 文件减 218 行、txgameslg 3 文件减 6 行，生成物共净减 246 行，模板另减 6 行。本轮独立起算，不累计已提交改动。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群，无新增正式测试或生产采集。全部本轮变更未提交，下一项具体候选尚需评估。

## S51：全字段直接复制类型简化 snapshotCopy

保留未提交 S50。模板根据已有字段 SnapshotDirect 标记静态判定：全部字段可直接复制时生成 return gd.Copy(), nil，省去中间变量及空白；需要深拷贝的类型保持原函数体。判定只发生在生成期，不新增运行时分类或公共接口。Copy 仍负责清除私有 Store 状态；例如已绑定 Store 的 GDHero 复制后保留字段值，副本不携带原 state，也不与源对象共享可写字段。

实际生成 GDHero.snapshotCopy 的前后对照覆盖 nil、绑定状态清除、字段内容与副本隔离，结果一致。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版预编译后交错六轮、每项 200ms，中位数如下：

| 源对象 | S50 ns/op | S51 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| nil | 1.0455 | 1.0495 | 0 | 0 |
| 普通对象 | 24.755 | 24.700 | 16 | 1 |
| 已绑定 Store | 24.940 | 25.405 | 16 | 1 |

非 nil 样本变化约 −0.06 至 +0.47ns，分配不变；接受已绑定样本约 +1.9% 的成本，不宣称提速。证据位于 `/private/tmp/tx-direct-snapshot-20260910`，包含旧源码、overlay、契约测试、bench-*.txt/results.json 与回归日志，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。逐文件对照确认只有可直接复制类型的函数体被裁剪，深拷贝类型不变；其他差异仅为未提交 S50。fixture 3 类型减 6 行、txgame 67 类型减 134 行、txgameslg 3 类型减 6 行，共减 146 行（含 73 行空白），模板增加 2 行。累计未提交 S50–S51 生成物净减 392 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S50–S51；下一项具体候选尚需评估。

## S52：集合子 Store 缓存由绑定入口初始化

保留未提交 S50–S51。map/slice storeFor 直接读取可为 nil 的 stores map，移除前置初始化；需要创建或重绑子 Store 时，已有 bindStore 负责初始化并绑定。NewStore 返回非 nil 新对象，缓存读取、模式/undo recorder 继承及 dirty hook 绑定顺序保持一致；指针子 Store 不受本轮改动影响。没有新增公共接口或 helper。

临时对照覆盖两种 selector 模式的创建、缓存复用和值变化重绑，结果一致。实际生成 GDRole 集合子 Store 两版预编译后交错六轮，每项 200ms、Go 1.27.0 darwin/amd64、GOMAXPROCS=1，中位数如下：

| 场景 | S51 ns/op | S52 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map 缓存已存在，新建子 Store，selector=false | 89.835 | 89.705 | 88 | 2 |
| slice 缓存已存在，新建子 Store，selector=false | 88.415 | 90.35 | 88 | 2 |
| map 缓存已存在，新建子 Store，selector=true | 90.25 | 87.52 | 88 | 2 |
| slice 缓存已存在，新建子 Store，selector=true | 89.835 | 88.71 | 88 | 2 |
| map 缓存也需新建 | 191.9 | 188.8 | 280 | 4 |
| slice 缓存也需新建 | 190.1 | 197.1 | 280 | 4 |

四个 map/slice 已有子 Store 复用样本均零分配，变化约 −0.308 至 +0.289ns；普通 map 复用约 +4.7%。接受 slice 首次创建约 +7ns（3.7%）的样本成本，以收敛重复初始化责任，不宣称全部路径提速或实际业务吞吐改善。指针路径作为未修改对照保留在原始数据中。

证据位于 `/private/tmp/tx-child-cache-20260910`，包含旧源码、overlay、契约测试、bench-*.txt/results.json 与回归日志；首次创建样本每轮将缓存置 nil，已存在缓存的新建样本只清理对应槽位，容器与 parent 在计时前创建，采集不进入生产路径。

重新生成后 txgen、txgame/server、txgameslg/server 本轮全量测试通过。逐文件从 HEAD 对照确认仅包含 S50–S51 已有改动和 storeFor 前置初始化移除，bindStore 未改动。本轮 fixture 删除 7 处初始化减 21 行、txgame 64 处减 192 行、txgameslg 3 处减 9 行，共减 222 行，模板另减 6 行。累计未提交 S50–S52 生成物净减 614 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S50–S52；下一项具体候选尚需评估。

## S53：集合撤销记录直接读取操作 map

保留未提交 S50–S52。删除内部 storeCollectionOps.get，它仅转发 map 双返回值读取；五处模板调用直接使用 s.ops[key] 或 s.ops[index]。操作读取位置、值及存在标志保持一致，命名 map 类型、克隆/恢复入口和 UndoRecorder SPI 保留。例如依次 Set、Touch、Delete 后逆序撤销，仍恢复原值及原操作状态；nil map 首次 Set 后撤销仍恢复 nil。

临时对照在修改前后验证 map 缺失/已有键、nil map、已有/缺失操作记录，以及 slice 连续 SetAt/TouchAt 的值和脏状态恢复。实际生成的 GDDailyTaskTasksMapStore.Set 与 GDRoleTagsSliceStore.SetAt 两版预编译后交替六轮，每项 200ms，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，中位数如下：

| 场景 | S52 ns/op | S53 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map / 无撤销记录器 | 26.145 | 26.310 | 0 | 0 |
| slice / 无撤销记录器 | 12.230 | 12.605 | 0 | 0 |
| map / 有撤销记录器 | 59.520 | 60.140 | 48 | 1 |
| slice / 有撤销记录器 | 95.560 | 94.455 | 80 | 2 |

分配不变，命中替换分支的样本变化 −1.105 至 +0.620ns；无记录器 slice 未经过替换分支，仍有 +0.375ns 波动。不宣称提速或业务吞吐改善，保留以语言原语替代包装的简化。基准记录器丢弃闭包以避免日志增长，契约验证使用实际收集与逆序执行的记录器；容器预先创建，采集只在 /private/tmp/tx-collection-get-20260910，包含旧源码、overlay、契约测试、六轮原始数据/results.json 与回归日志。

本轮重新生成，txgen、txgame/server、txgameslg/server 全量测试通过。逐文件由 HEAD 对照确认差异仅为 S50–S53：fixture 25 处、txgame 431 处、txgameslg 12 处调用替换，三套生成物各删一个 5 行 helper，本轮共减 15 行，模板另减 5 行。累计未提交生成物净减 629 行（fixture 54、txgame 549、txgameslg 26）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群，无生产采集或新增正式测试。全部未提交，提交建议累计 S50–S53；下一项具体候选尚需评估。

## S54：集合操作重置与恢复直接赋值

保留未提交 S50–S53。删除只包装赋值的 storeCollectionOps.clear/restore，八处模板调用改为 ops = nil 或 ops = previousOps。保留克隆和单项恢复，维持原赋值时机与 nil 检查。例如 UnTouch 后旧 map 的内容不变，当前集合释放其引用；逆序撤销后重新恢复操作记录，不能用 clear(map) 替代这一语义。

临时前后验证覆盖 map/slice 取消标记、原 map 内容保留、操作记录恢复、map 整体替换和 slice 删除后的回滚。真实生成方法两版预编译后交替六轮，每项 200ms，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，中位数如下：

| 场景 | S53 ns/op | S54 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map / UnTouch 无记录器 | 2.557 | 2.593 | 0 | 0 |
| map / UnTouch 并撤销 | 131.650 | 132.400 | 160 | 3 |
| slice / UnTouch 并撤销 | 150.300 | 152.950 | 224 | 3 |

分配不变，样本成本分别增加约 0.036、0.750、2.650ns；接受最大约 1.8% 的中位数差异，以移除仅转发赋值的包装，不宣称提速。记录器真实收集并逆序执行闭包，容器和日志容量预先创建；无记录器样本每轮恢复同一操作 map 后调用 UnTouch。证据位于 /private/tmp/tx-collection-assign-20260910，包含旧源码、overlay、契约测试、六轮原始数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮重新生成，txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对确认仅包含 S50–S54：fixture 38 处、txgame 742 处、txgameslg 16 处调用替换，共 796 处；各套生成物删除 8 行 helper，本轮净减 24 行，模板另减 8 行。累计未提交生成物净减 653 行（fixture 62、txgame 557、txgameslg 34）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S50–S54；下一项具体候选尚需评估。

## S55：固定操作码转发直接调用 recordOp

六工程起始干净，S50–S54 已提交。删除每个 Store 重复生成的 recordSet/recordReplace/recordClear，调用处显式传入 change.OpSet/OpReplace/OpClear；recordOp 的 nil/字段边界校验、状态写入和 notifyDirty 保持原样。覆盖设置、清除、增减、快照差异以及集合清除后再写入的操作码升级入口。仅替换转发，不修改状态机或 SPI；生成检查删除旧方法存在断言并更新调用文本。

临时前后验证 SetName、SetTags、ClearTags 的值、操作码及 dirty hook 通知次数一致。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成方法预编译后交替六轮、每项 200ms，中位数如下：

| 方法 | S54 ns/op | S55 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| SetName | 6.007 | 5.9355 | 0 | 0 |
| SetTags | 6.160 | 6.2970 | 0 | 0 |
| ClearTags | 5.850 | 5.8245 | 0 | 0 |

样本变化约 −0.072 至 +0.137ns，均零分配；接受 SetTags 约 +2.2% 的中位数差异，不宣称提速，也不外推到未单独测量的快照、集合操作或业务吞吐。容器与替换 slice 在计时前创建，基准不携带撤销记录器；契约测试额外验证 dirty hook。证据位于 /private/tmp/tx-record-op-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮重新生成，txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对，仅删除固定操作码转发并展开原调用：fixture 12 类型/160 调用净减 144 行，txgame 163 类型/5105 调用净减 1956 行，txgameslg 7 类型/137 调用净减 84 行；合计 182 类型、5402 调用、生成物净减 2184 行，模板另减 12 行，文本生成检查净减 2 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，仅累计 S55；下一项具体候选尚需评估。

## S56：清除字段操作码直接复用 restoreOp

保留未提交 S55。删除仅转发 restoreOp(field, 0) 的 clearOp，三处模板调用覆盖加载字段合并、普通字段取消标记与集合字段取消标记。保留 restoreOp 的 nil/字段边界校验以及不触发 notifyDirty 的语义，撤销记录顺序不变。例如 UnTouchName 清除操作码后，逆序撤销恢复 OpSet；mergeFieldsFrom 合入加载值并清除字段操作码，不产生新的脏通知。

临时前后验证普通/集合取消标记、撤销操作码恢复、加载字段合并的值和零通知次数均通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成方法预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S55 ns/op | S56 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| UnTouchName / 无记录器 | 3.6310 | 3.5125 | 0 | 0 |
| UnTouchTags / 无记录器 | 2.1105 | 2.1045 | 0 | 0 |
| UnTouchName / 记录并撤销 | 44.820 | 44.170 | 48 | 1 |

样本变化约 −0.119、−0.006、−0.650ns，分配不变，未见明显退化，不外推为业务吞吐改善。无记录器样本重复清零同一字段，有记录器样本每轮真实执行撤销恢复原操作码；容器与日志容量预先创建。证据位于 /private/tmp/tx-clear-op-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮重新生成，txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅包含 S55–S56：本轮 fixture 44 调用净减 48 行、txgame 750 调用净减 652 行、txgameslg 29 调用净减 28 行，共 823 调用、182 类型，生成物净减 728 行，模板另减 4 行。累计未提交生成物净减 2912 行（fixture 192、txgame 2608、txgameslg 112）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S55–S56；下一项具体候选尚需评估。

## S57：Store 路径拼接按生成包共享

保留未提交 S55–S56。每个 Store 的 appendPathNode 不读取接收者，将相同函数体移至 type_registry 的 appendStorePathNode，所有 Store/集合遍历直接调用。共享函数仍构造单节点 child 并调用 change.JoinPathLimited，不修改定长路径布局与祖先截断语义；StoreView 的 appendStoreViewPath 仍调用严格 JoinPath，独立保留。未增加公共 SPI 或配置。

临时前后验证实际 GDHeroStore.visitChanges 在前缀深度 0 至 MaxPathDepth 的路径、操作码和通知次数一致；到达上限仍报告并用最近祖先 OpReplace。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成遍历预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S56 ns/op | S57 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| GDHero 单字段 / 空前缀 | 80.990 | 79.005 | 0 | 0 |
| GDHero 单字段 / 深度 3 前缀 | 97.585 | 87.385 | 0 | 0 |
| GDRole 单字段 | 91.065 | 86.160 | 0 | 0 |
| GDDailyTask map 单项 | 140.350 | 128.300 | 0 | 0 |

四个样本中位数分别下降约 2.5%、10.5%、5.4%、8.6%，均零分配，未见明显退化；不外推为业务吞吐改善。Store 和有状态 visitor 在计时前创建，每轮遍历已有脏状态；visitor 只保存最后一次路径/操作码并计数。证据位于 /private/tmp/tx-store-path-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮重新生成，txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅包含 S55–S57：本轮 fixture 39 调用净减 77 行、txgame 496 调用净减 1134 行、txgameslg 17 调用净减 42 行，共 552 调用；182 份方法改为三个生成包各一份共享函数，生成物净减 1253 行，模板净行数不变。累计未提交生成物净减 4165 行（fixture 269、txgame 3742、txgameslg 154）。后置 ponytail-review 无新增简化项；严格拼接与祖先截断仍保留各自语义，未为共用而增加模式参数。diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议累计 S55–S57；下一项具体候选尚需评估。

## S58：无子 Store 集合直接返回操作表是否非空

保留未提交 S55–S57。没有生成子 Store 的 map/slice，其 hasChanges 直接返回 len(s.ops) > 0；无可追踪操作表的 map 仍返回 false。含子 Store 的集合和根 Store 的函数体逐字保留，不重试此前根扫描被否决的方案。模板利用已有字段分类，不新增运行时分支或元数据。例如清空操作表返回 false，存在 Set/Replace/Delete 记录返回 true，根 Store 仍能观察到集合变更。

临时前后验证 nil/空操作表、多种有效操作码及根 Store 判断一致。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成方法预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S57 ns/op | S58 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map / 干净 | 1.1365 | 1.1595 | 0 | 0 |
| slice / 干净 | 1.1770 | 1.2330 | 0 | 0 |
| 根 Store / 干净集合 | 4.1330 | 4.1320 | 0 | 0 |
| map / 有操作 | 0.64805 | 0.65250 | 0 | 0 |
| slice / 有操作 | 0.65765 | 0.64760 | 0 | 0 |
| 根 Store / 有集合操作 | 3.6540 | 3.6280 | 0 | 0 |

所有样本零分配。集合直接查询变化约 −0.010 至 +0.056ns；干净 slice 相对约 +4.8%，接受这项绝对值很小的样本差异。根查询变化约 −0.026 至 −0.001ns，不宣称业务提速。容器和操作记录预先创建，结果写入包变量防止消除；临时基准不进入生产路径。证据位于 /private/tmp/tx-leaf-collection-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志。整理模板重复条件后重新生成，确认与已测版本逐字一致。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅包含 S55–S58：fixture 1 map/1 slice 减 7 行、txgame 67 map/37 slice 减 379 行、txgameslg 1 map 减 4 行，共 107 集合净减 390 行（含 69 行 map 原空白），模板增加 2 行。累计未提交生成物净减 4555 行（fixture 276、txgame 4121、txgameslg 158）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S55–S58；下一项具体候选尚需评估。

## S59：map 选择副本在字面量中初始化操作表

保留未提交 S55–S58。map 集合 selectionCopy 与现有 slice 写法一致，在结构体字面量中初始化 ops: s.ops.clone()，省去空对象创建后的赋值语句。nil 接收者、空表规范化为 nil、操作记录克隆、子 Store 绑定逻辑保持不变；例如修改副本的操作码不会改变原 map 的记录。

临时前后验证 nil/空表、副本 parent 未绑定和操作记录隔离通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版 GDDailyTaskTasksMapStore.selectionCopy 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S58 ns/op | S59 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 空操作表 | 26.650 | 26.005 | 16 | 1 |
| 单项操作表 | 112.750 | 111.750 | 144 | 3 |

分配不变，中位数分别减少约 0.645ns、1ns，未见明显退化，不外推为业务吞吐提升。容器与操作表预先准备，副本写入包变量使其逃逸；证据位于 /private/tmp/tx-map-selection-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅包含 S55–S59：本轮 fixture 7 处、txgame 103 处、txgameslg 4 处初始化合并，生成物净减 114 行，模板另减 1 行；子 Store 复制分支逐字未变。累计未提交生成物净减 4669 行（fixture 283、txgame 4224、txgameslg 162）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S55–S59；下一项具体候选尚需评估。

## S60：无子 Store 集合直接返回选择副本

保留未提交 S55–S59。无生成子 Store 的 map/slice selectionCopy 直接返回结构体字面量，省去 view 中间变量；需要复制与绑定子 Store 的函数体逐字保留。nil 接收者、空表和操作记录隔离保持一致；不可追踪 key 的 map 仍返回不含操作表的副本，不增加字段。

前后临时验证 map/slice 的 nil、空表、parent 未绑定与操作表隔离通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成 selectionCopy 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S59 ns/op | S60 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map / 空表 | 26.630 | 26.650 | 16 | 1 |
| map / 单项表 | 113.150 | 111.650 | 144 | 3 |
| slice / 空表 | 27.760 | 27.635 | 16 | 1 |
| slice / 单项表 | 130.800 | 129.650 | 208 | 3 |

分配不变，样本中位数变化约 −1.5 至 +0.02ns，未见明显退化，不外推为业务吞吐提升。容器与操作表预先准备，结果写入包变量；证据位于 /private/tmp/tx-direct-selection-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅包含 S55–S60：本轮 fixture 2 集合减 3 行、txgame 111 集合减 185 行、txgameslg 1 集合减 2 行，共 114 集合净减 190 行（含 76 行 map 空白），模板增加 2 行。累计未提交生成物净减 4859 行（fixture 286、txgame 4409、txgameslg 164）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S55–S60；下一项具体候选尚需评估。
