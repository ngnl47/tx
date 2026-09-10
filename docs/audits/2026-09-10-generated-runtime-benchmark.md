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

## S61：无需子节点绑定的 Store 直接返回选择副本

六工程起始干净，S55–S60 已提交。selectionCopy 在没有 GeneratedPointer/MapCollection/SliceCollection 字段时直接返回结构体字面量；需要子节点绑定的类型保持原函数体。分类使用已有字段标记，只发生在生成期，不新增运行时判断或公共元数据。Value 仍共享引用，ops/loadedFields 按值复制，selector 仍为 true，不携带原 recorder/hook。

临时前后验证 GDHeroStore 的 nil、Value 引用、状态数组隔离以及 selector 拒绝修改均通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成 selectionCopy 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S60 ns/op | S61 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 无操作记录 | 34.265 | 34.550 | 64 | 1 |
| 有操作记录与加载标记 | 34.400 | 34.230 | 64 | 1 |

分配不变，中位数变化 +0.285ns、−0.170ns，未见明显退化，不宣称业务提速。Store 预先创建，副本写入包变量；证据位于 /private/tmp/tx-leaf-store-selection-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对：fixture 4 类型、txgame 76 类型、txgameslg 3 类型各减一行，共净减 83 行；模板增加 1 行，需要子节点绑定的函数体未变。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议仅含 S61；下一项具体候选尚需评估。

## S62：加载字段标记直接遍历输入

保留未提交 S61。markLoadedFields 移除循环前的 len(fields)==0 返回；Go range 对 nil/空切片自然不执行，字段名解析与加载位标记逻辑保持不变。当前唯一模板调用来自 LoadFields，其空输入已提前转到 Load，因此生产调用传入非空列表；保留空输入的等价无操作行为，不将它升级为前置条件。

临时前后验证 nil Store 的空输入、普通空列表、重复/未知字段及加载标记通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成 GDHeroStore.markLoadedFields 预编译后交替六轮、每项 200ms，中位数如下：

| 输入 | S61 ns/op | S62 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 空列表 | 1.5615 | 1.8165 | 0 | 0 |
| Level | 6.4720 | 6.6015 | 0 | 0 |
| HeroId、Level、未知字段 | 14.7950 | 15.0400 | 0 | 0 |

分配均为零，中位数增加 0.255ns（空列表约 16.3%）、0.1295ns（约 2.0%）、0.245ns（约 1.7%）。明确接受这项微基准成本，以删除重复的空集合判断；当前生产入口已过滤空输入，不宣称提速或业务吞吐无损。输入和 Store 在计时前创建，测量重复标记同一列表；证据位于 /private/tmp/tx-loaded-loop-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S62：本轮 fixture 12 类型减 36 行、txgame 163 类型减 489 行、txgameslg 7 类型减 21 行，共净减 546 行，模板另减 3 行。累计生成物净减 629 行（fixture 40、txgame 565、txgameslg 24）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S62；下一项具体候选尚需评估。

## S63：无嵌套 StoreView 状态停止生成无调用方法

保留未提交 S61–S62。fieldOp/setFieldOp 的全部模板调用都位于 HasViewNestedFields 分支；无嵌套状态的 record/hasField/contains 已直接使用共享函数，因此将两个方法也纳入该生成条件。含嵌套状态的实现与现有 SPI 保留，不删除运行时校验或可靠性测试。

临时前后验证 viewStateGDHero 记录、包含查询、非法路径拒绝与 nil 状态行为通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际 record/contains 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S62 ns/op | S63 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 记录单字段 OpSet | 18.705 | 19.165 | 0 | 0 |
| 包含查询 | 23.640 | 25.280 | 0 | 0 |

均零分配，样本中位数增加 0.460ns（约 2.5%）、1.640ns（约 6.9%）。实际运行函数体逐字不变，删除的是无调用私有方法；保留删除，但不将采样差异归因为已证实的原因，也不宣称提速或性能无损。基准复用预先构造的状态与路径，查询结果写入包变量；证据位于 /private/tmp/tx-leaf-view-methods-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S63：本轮 fixture 4 类型减 56 行、txgame 78 类型减 1092 行、txgameslg 3 类型减 42 行，合计 85 类型的 170 个私有方法净减 1190 行，模板净行数不变。累计生成物净减 1819 行（fixture 96、txgame 1657、txgameslg 66）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S63；下一项具体候选尚需评估。

## S64：无子 Store 类型不生成无调用 recordUndo

保留未提交 S61–S63。私有 recordUndo 只由指针子 Store 与集合的撤销闭包调用；无这些字段的类型通过 recordStoreFieldUndo/setStoreFieldValue 共享函数记录撤销。复用 S61 已有的字段分类条件，停止为无调用类型生成该方法；公开 SetUndoRecorder、共享函数及所有实际撤销调用保持不变。

临时前后验证 GDHeroStore 设置、清除、逆序撤销恢复值与操作码，以及解除记录器绑定通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成字段路径预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S63 ns/op | S64 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 普通设置 | 5.403 | 5.431 | 0 | 0 |
| 设置并逆序撤销 | 56.680 | 57.375 | 80 | 1 |

分配不变，样本增加约 0.028ns、0.695ns（约 0.5%、1.2%），实际被测函数体未改，不宣称提速。撤销样本预先创建 Store 与日志容量，每轮真实调用撤销闭包；证据位于 /private/tmp/tx-leaf-undo-method-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S64：本轮 fixture 4 类型减 24 行、txgame 76 类型减 456 行、txgameslg 3 类型减 18 行，合计 83 个无调用方法净减 498 行，模板增加 1 行。累计生成物净减 2317 行（fixture 120、txgame 2113、txgameslg 84）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S64；下一项具体候选尚需评估。

## S65：无子 Store 类型不生成无调用 rootOp

保留未提交 S61–S64。rootOp 仅供子 Store/集合覆盖状态判断使用；没有相关字段的类型不再生成此私有方法，复用已有字段分类条件。有调用类型的方法和所有覆盖检查保持不变，不重试 S43 已否决的检查移除。公开 Source、根操作码扫描和通知逻辑保留。

临时前后验证 GDHeroStore 的 nil、设置、取消标记、全字段标记与清除状态通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际 HasChanges 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S64 ns/op | S65 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 干净 | 3.0470 | 3.0010 | 0 | 0 |
| 有操作记录 | 1.5635 | 1.5200 | 0 | 0 |

均零分配，样本中位数减少约 0.046ns、0.0435ns；实际被测函数体未改，不宣称业务提速。状态预先创建，结果写入包变量；证据位于 /private/tmp/tx-leaf-root-op-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S65：本轮 fixture 4 类型减 20 行、txgame 76 类型减 380 行、txgameslg 3 类型减 15 行，共 83 个方法净减 415 行，模板增加 1 行。累计生成物净减 2732 行（fixture 140、txgame 2493、txgameslg 99）。后置 ponytail-review 无本轮新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S65。范围核对另发现部分不可追踪 key 集合类型的 rootOp 也没有调用，本轮未扩大分类；下一项评估其生成条件和删除边界。

## S66：按实际子变更条件生成 rootOp

保留未提交 S61–S65。rootOp 的生成条件收窄为 GeneratedPointer、SliceCollection 或 MapCollection 且（MapKeyTrackable 或 MapValueGeneratedPtr），与实际模板调用一致。不能直接使用 HasViewNestedFields 替代，因为不可追踪 key、但值为生成子 Store 的 map 仍使用覆盖检查。仅删除 txgame 的 GDRankLegionStore/GDRoleTeamStore 两个无调用方法；状态判断、集合更新及所有有调用类型保持原样。

修改前后直接在 txgame 生成包验证两个类型的初始、AllTouch、ClearChanges 状态通过。全量测试结束后，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际 HasChanges 预编译后交替六轮、每项 200ms，中位数如下：

| 有操作记录的类型 | S65 ns/op | S66 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| GDRankLegion | 1.3515 | 1.3190 | 0 | 0 |
| GDRoleTeam | 1.6330 | 1.6505 | 0 | 0 |

零分配，样本变化约 −0.0325ns、+0.0175ns；实际被测函数体未改，不宣称提速。Store 在计时前设置为有变更状态，查询结果写入包变量；证据位于 /private/tmp/tx-untracked-root-op-20260910，包含两个旧源码文件、overlay、契约测试、六轮数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S66：本轮仅 txgame 两个方法净减 10 行，模板增加 1 行；fixture 和 txgameslg 无本轮生成差异。累计生成物净减 2742 行（fixture 140、txgame 2503、txgameslg 99）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S66；下一项具体候选尚需评估。

## S67：无子 Store 类型不生成无调用 op

保留未提交 S61–S66。无子 Store 类型的 rootOp 删除后，私有 op 也没有调用；普通字段撤销由共享函数处理。复用已有字段分类停止生成该方法，保留 restoreOp、公开接口及所有有调用类型的 op 方法。实际设置、清除、撤销函数体不变。

本轮重新执行临时前后验证，覆盖 GDHeroStore 设置、清除、逆序撤销和解除记录器绑定，结果通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际字段路径预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S66 ns/op | S67 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 普通设置 | 5.3860 | 5.4135 | 0 | 0 |
| 设置并逆序撤销 | 55.420 | 55.620 | 80 | 1 |

分配不变，样本中位数增加约 0.0275ns、0.200ns；实际被测函数体未改，不宣称提速。复用 S64 临时测试的逻辑但重新快照、编译和测量；证据位于 /private/tmp/tx-leaf-op-method-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S61–S67：本轮 fixture 4 类型减 28 行、txgame 76 类型减 532 行、txgameslg 3 类型减 21 行，共 83 个方法净减 581 行，模板增加 1 行。累计生成物净减 3323 行（fixture 168、txgame 3035、txgameslg 120）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S61–S67；下一项具体候选尚需评估。

## S68：StoreView 字段操作设置短路返回

六工程起始干净，S61–S67 已提交。嵌套 StoreView 状态的 setFieldOp 使用 s != nil && setStoreViewFieldOp(...)，替代先判 nil 再返回共享函数结果。短路保证 nil 不访问 ops，字段边界检查及返回值保持原样，不修改 record 状态机或 SPI。

临时前后验证 nil、零/越界字段、设置与清零以及实际 record/contains 路径通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际 viewStateGDRole.record 预编译后交替六轮、每项 200ms，中位数如下：

| 字段 | S67 ns/op | S68 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| Name | 12.095 | 11.965 | 0 | 0 |
| Tags | 12.440 | 12.480 | 0 | 0 |

均零分配，样本变化约 −0.130ns、+0.040ns，未见明显退化，不宣称业务提速。状态与路径在计时前创建，每轮记录字段整体替换；证据位于 /private/tmp/tx-view-set-op-20260910，包含旧源码、overlay、契约测试、六轮数据/results.json、范围检查与回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅改变 setFieldOp：fixture 8 类型减 24 行、txgame 85 类型减 255 行、txgameslg 4 类型减 12 行，共 97 类型净减 291 行，模板另减 3 行。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。本轮未提交，提交建议仅含 S68；下一项具体候选尚需评估。

## S69：不可追踪 key 的 map 不生成单项复制 helper（2026-09-11）

保留未提交 S68。单项 snapshotClone…Item 的调用位于 MapKeyTrackable 分支，将生成条件补齐为 MapCollection && MapKeyTrackable && !MapValueSnapshotDirect。不可追踪 key 的 map 仍通过整字段 snapshotClone 深拷贝；slice 及所有有调用的单项复制保留。按各工程实际定义核对，不将 txgame 与 fixture 的同名类型视为同一结构。

前后直接在 txgame 验证 GDRankLegion.Nameplate 整字段复制，嵌套 map 及 GDNameplate 对象均独立。全量回归完成后，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际 snapshotCopy 预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S68 ns/op | S69 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 空 Nameplate | 84.845 | 83.965 | 112 | 1 |
| 单项嵌套 map | 2647.5 | 2611.5 | 608 | 17 |

分配不变，样本中位数减少约 0.88ns、36ns；实际整字段复制函数体未改，不宣称业务提速。数据在计时前准备，副本写入包变量；证据位于 /private/tmp/tx-untracked-clone-item-20260910（本轮开始时创建，跨日沿用），包含八个旧源码文件、overlay、隔离测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S68–S69：仅 txgame 删除 8 个无调用 helper 净减 56 行，模板净行数不变；fixture 和 txgameslg 无本轮生成差异。累计生成物净减 347 行（fixture 24、txgame 311、txgameslg 12）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S68–S69；下一项具体候选尚需评估。

## S70：JSON 快照复制统一返回解码结果（2026-09-11）

保留未提交 S68–S69。snapshotCloneJSON 在 json.Marshal 成功后，先将 json.Unmarshal 错误赋给 err，再统一返回 cloned, err，省去重复返回分支。必须先完成解码再读取 cloned，不能写成 return cloned, json.Unmarshal(...)；错误时可能已有部分结果，仍需与错误一起返回。编码失败的原分支保留。

临时前后验证自定义 UnmarshalJSON 写入部分结果后返回错误，以及实际 snapshotCloneGDRoleTags 的复制隔离通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版实际生成 slice 快照复制预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S69 ns/op | S70 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 空 slice | 433.60 | 437.90 | 80 | 4 |
| 两项 slice | 792.50 | 798.25 | 136 | 6 |

分配不变，样本中位数增加约 4.3ns（1.0%）、5.75ns（0.7%），记录并接受这一微基准成本，不宣称提速。输入预先创建，副本写入包变量；证据位于 /private/tmp/tx-json-clone-return-20260911，包含旧 registry、overlay、错误语义/隔离测试、六轮数据/results.json、范围检查和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S68–S70：本轮三套 registry 各减 2 行，共净减 6 行，模板另减 2 行。累计生成物净减 353 行（fixture 26、txgame 313、txgameslg 14）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群。全部未提交，提交建议累计 S68–S70；下一项具体候选尚需评估。


## S71：生成 Store 直接调用撤销记录 SPI（2026-09-11）

保留未提交 S68–S70。逐处核对模板中的 13 个 recordUndo 调用点，均已在 undoRecorder 非 nil 分支内，参数均为现场创建的非 nil 撤销闭包。删除生成 Store 私有转发方法，调用处直接执行 undoRecorder.RecordUndo；保留 UndoRecorder SPI、原有守卫、闭包内容和登记顺序。例如 SetDailyTask 仍先记录原指针、操作及子 Store，再替换字段值，回滚时恢复原状态。用于其他生成分支的 $hasSelectionChildren 保留。

临时前后测试覆盖指针替换/清除、slice 修改、map 新增/覆盖/删除的值和操作恢复。现有全量测试继续覆盖嵌套对象、集合和事务前已有增量的回滚。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译两版实际 fixture 生成物，交替六轮、每项 200ms，写入后执行撤销，中位数如下：

| 场景 | S70 ns/op | S71 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 指针 SetDailyTask | 86.990 | 85.965 | 48 | 1 |
| map Set | 166.3 | 165.7 | 48 | 1 |
| slice SetAt | 227.1 | 220.7 | 80 | 2 |

分配不变，中位数分别约下降 1.2%、0.4%、2.8%，本轮样本未发现退化，不外推业务整体提速。Store、输入及预分配撤销记录器在计时前准备，每次反序回滚并复用记录 slice；基准包含闭包登记和撤销成本。证据位于 /private/tmp/tx-direct-undo-20260911，包含改前 fixture、overlay、隔离测试、六轮原始结果/results.json、范围校验及回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。逐文件从 HEAD 核对生成物仅包含 S68–S71：本轮删除 99 个私有方法，fixture 减 48 行、txgame 减 522 行、txgameslg 减 24 行，共 594 行，模板另减 7 行。累计生成物净减 947 行（fixture 74、txgame 835、txgameslg 38）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群或 race。全部未提交，提交建议累计 S68–S71；下一项具体候选尚需评估。


## S72：生成包统一路径深度告警格式（2026-09-11）

> 已撤回：按用户反馈恢复直接 txlog.Warn，不为一条日志增加转发函数。以下为当时的验证记录，880 行减少不再计入保留成果。

保留未提交 S68–S71。182 个 Store 的 reportPathDepthFallback 保留 nil 守卫和各类型独立 sync.Once，闭包改为调用生成包内 warnStorePathDepth，统一同一条日志正文及 type/maxDepth 字段。三个独立生成包分别包含该私有函数，Store 文件不再导入 txlog；导入模型同步清理。深路径回退仍在原调用点触发，不改变最近祖先替换与访问次数。启用源码定位时，日志来源会集中到 type_registry.go 的公共输出函数，类型仍通过 type 字段区分。

临时前后测试验证 nil Store 不输出、同类型重复调用仅输出一次、不同类型各输出一次且包含对应类型名称。txgen 全量回归覆盖深层子节点/快照回退及最近祖先仅访问一次。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，两版预编译后交替六轮、每项 200ms，中位数如下：

| 场景 | S71 ns/op | S72 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| 已告警后的调用 | 1.919 | 1.929 | 0 | 0 |
| 首次告警，JSON handler 输出至 io.Discard | 1039 | 1049 | 16 | 1 |

分配不变，中位数约增加 0.010ns（0.5%）、10ns（1.0%）；接受此次微基准成本，不宣称提速。首次场景每次重置该类型 Once，重置成本包含在内；仅用于隔离采集，不进入生产代码。证据目录 /private/tmp/tx-depth-warning-20260911 包含旧生成物、overlay、语义测试、六轮数据/results.json、生成范围检查与全量回归日志。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过。由 HEAD 逐文件核对仅含 S68–S72。本轮生成物净减 880 行（fixture 50、txgame 805、txgameslg 25），模板与导入模型合计净增 3 行；累计生成物净减 1827 行（fixture 124、txgame 1640、txgameslg 63）。后置 ponytail-review 无新增简化项，diff 检查通过。两个客户端无变化，未另跑真实集群或 race。全部未提交，提交建议累计 S68–S72；下一项具体候选尚需评估。


## S73：无记录键类型不生成加载标记方法（2026-09-11）

保留未提交 S68–S72。markLoadedFields、markAllFieldsLoaded 的加载入口与 codec 回调仅在 RecordKey.Has 条件内生成；markFieldLoaded 仅由 markLoadedFields 调用。将三个私有方法的生成条件与调用方对齐，无记录键的 142 个类型不再输出这组无调用代码。公开 LoadedFields、IsFieldLoaded、字段查询方法及 loadedFields 状态保持不变；有记录键类型的加载/投影与标记代码全部保留。例如 GDRole 仍能记录 RoleId/Name 已加载，GDItem 的查询仍返回未加载状态。

临时前后验证字段顺序、未知/重复字段处理、结果切片隔离、无记录键类型及 nil 查询通过。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译两版 fixture，交替六轮、每项 200ms，实际 LoadedFields 查询中位数如下：

| 场景 | S72 ns/op | S73 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| GDRole，两字段已加载 | 51.865 | 54.300 | 32 | 1 |
| GDItem，未加载 | 5.288 | 5.267 | 0 | 0 |

首组记录类型耗时增加约 4.7%，因此进一步交替六轮、每项 1s 复测该场景，得到 51.915 → 52.535ns（约 +1.2%），仍为 32B/1 次分配。两组结果均保留，接受复测中的小幅成本，不宣称提速或零退化。GDRoleStore.go 与共享 type_registry.go 经字节比较和范围检查确认未发生本轮变化；无法由微基准确定差异原因。Store 和加载字段在计时前准备，结果写入包变量。证据位于 /private/tmp/tx-loaded-markers-20260911，包含旧生成物、overlay、隔离测试、两组数据/results.json 与 repeat-results.json、范围校验和回归日志，采集不进入生产路径。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过，包括现有投影和加载状态测试。由 HEAD 逐文件核对生成物仅含 S68–S73，并验证有记录键类型保留三个方法、无记录键类型不再生成。删除 426 个私有方法，生成物净减 2556 行（fixture 162、txgame 2340、txgameslg 54），模板增加 2 行条件；累计生成物净减 4383 行（fixture 286、txgame 3980、txgameslg 117）。后置 ponytail-review 无新增简化项，diff 检查通过；客户端无变化，未另跑真实集群或 race。全部未提交，提交建议累计 S68–S73；下一项具体候选尚需评估。


## S72 撤回：恢复 Store 直接记录日志（2026-09-11）

本次起始六工程 git status 均干净，S68–S73 已提交。按用户要求删除 warnStorePathDepth，恢复每个 Store 在原 sync.Once 闭包中直接调用 txlog.Warn，并恢复 Store 导入模型；三个独立生成包全部重新生成。日志正文、type/maxDepth、每类型一次及 nil 守卫保留，源码定位回到相应 Store。今后不以减少生成行数为由，为单条日志增加转发函数。其余已实施简化保留。

此次生成物恢复 880 行（fixture 50、txgame 805、txgameslg 25），模板与模型净减 3 行；S68–S73 扣除 S72 后保留净减 3503 行。逐文件从 HEAD 核对本轮只有日志封装还原，证据位于 /private/tmp/tx-direct-depth-log-20260911。此次未重跑 benchmark，恢复原直接调用，不据历史结果宣称本轮性能变化。本次不提交，提交建议仅包含此次还原；下一项具体候选尚需评估。

本轮告警语义验证、txgen 和两个 server 全量测试通过，生成范围及 diff 检查通过。后置 ponytail-review 无新增简化项，尊重用户明确要求保留直接日志调用；客户端无变化，未另跑真实集群或 race。


## S74：集合视图直接分派已记录操作（2026-09-11）

保留未提交的 S72 撤回，日志继续直接输出。生成指针元素 map/slice 的视图 record 原先先检查 current != 0，再 switch current；case 仅含有效非零操作，零值自然不匹配，因此改为直接 switch 集合操作表读取值。删除额外分支及临时变量，不引入函数或公共抽象。路径合法性、集合 key 校验及子状态创建逻辑不变；已有 Set/Replace 时维持整项操作，已有 Clear/Delete 后修改子项时升级为 Replace，没有已记录操作时继续记录子项。

临时前后测试覆盖 map/slice 的无操作、Set、Replace、Delete、Clear，核对合并后的操作和精确路径选择。计时使用预建子状态，覆盖记录子字段、已替换整项、删除后修改三类路径；删除场景每次恢复 Delete，恢复成本计入。Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译两版后交替六轮、每项 200ms，中位数如下：

| 场景 | 修改前 ns/op | 修改后 ns/op | 两版 B/op | 两版 allocs/op |
|---|---:|---:|---:|---:|
| map，无整项操作 | 58.865 | 58.255 | 0 | 0 |
| map，已替换整项 | 22.185 | 21.035 | 0 | 0 |
| map，删除后修改 | 37.685 | 36.610 | 0 | 0 |
| slice，无整项操作 | 62.365 | 64.060 | 0 | 0 |
| slice，已替换整项 | 22.115 | 22.140 | 0 | 0 |
| slice，删除后修改 | 35.695 | 35.505 | 0 | 0 |

分配不变，map 中位数约下降 1.0%–5.2%；slice 无整项操作约增加 1.695ns（2.7%），其余两项变化低于 0.6%。接受该局部成本，不宣称全面提速或零退化。证据在 /private/tmp/tx-view-switch-20260911，包含旧 fixture、overlay、语义测试、六轮数据/results.json、范围校验及回归日志。采集不进入生产路径。

本轮 70 个生成分支净减 140 行（fixture 14、txgame 120、txgameslg 6），模板减 4 行。累计未提交变更包含日志还原 +880 行，因此本批生成物净增 740 行（fixture 36、txgame 685、txgameslg 19）。从 HEAD 逐文件核对仅包含日志还原和本轮 switch 调整。此前成果扣除撤回并计入 S74 后保留净减 3643 行。本批不提交，提交建议累计日志还原与 S74；下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；生成范围与 diff 检查通过。后置 ponytail-review 无新增简化项，直接日志调用保留。客户端无变化，未另跑真实集群或 race。


## S75：展开加载标记转发未采纳（2026-09-11）

保留未提交的日志还原及 S74。本轮评估删除 markFieldLoaded，并让 markLoadedFields 直接调用既有 markStoreFieldLoaded。方案一把 nil 检查移到批量入口；方案二保留逐字段解析和 nil 检查的原有顺序，在循环内展开调用。两方案均通过临时 nil、空输入、未知/重复字段、声明顺序及追加后状态保持验证，但没有明确性能收益，第二种展开还需显式 fieldID 局部变量与 nil 分支，因此均不保留。原 helper 是已存在的标记边界；不因单调用就必须删除，也不为追求行数更少持续堆叠小幅性能成本。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，每方案两版预编译后二者交替六轮、每项 200ms。所有场景均为 0B/0 次分配，中位数如下：

| 场景 | 方案一基线 ns/op | 入口判 nil ns/op | 方案二基线 ns/op | 保留原顺序展开 ns/op |
|---|---:|---:|---:|---:|
| 空输入 | 1.785 | 2.109 | 1.7915 | 1.7895 |
| 单字段 | 6.6405 | 6.5495 | 6.6570 | 6.7095 |
| 六项（含未知及重复字段） | 53.325 | 54.910 | 52.640 | 53.720 |

方案一空输入约 +18.2%、六项约 +3.0%；方案二空输入持平，六项约 +2.1%。只据样本记录局部耗时变化，不断言所有生产负载必然退化。证据目录 /private/tmp/tx-direct-loaded-mark-20260911 包含旧 fixture、overlay、隔离测试、results.json、inline-results.json 和回归日志；所有采集逻辑位于临时目录。

已恢复模板和原生成断言，重新生成 fixture 后与本轮开始前逐文件字节一致。两个示例本轮未应用该候选，未重新执行 server 测试；此前测试不作为本轮结果。本轮无新增生产代码变更，原未提交生成物净增 740 行的统计保持不变。提交建议仍累计日志还原、S74 及本轮未采纳记录。除非有新需求或新证据，不重复该候选；下一项具体候选尚需评估。

恢复后本轮 txgen 全量测试通过；累计生成范围及六工程 diff 检查通过。后置 ponytail-review 无新增简化项，保留原有加载标记 helper。


## S76：标量集合快照逐项直接比较（2026-09-11）

保留未提交的日志还原、S74 和 S75 未采纳记录。map/slice 快照逐项差异原先统一 reflect.DeepEqual；对现有 SnapshotDirect 判定中的非 time.* 元素改用 ==/!=，减少反射和装箱。保留 time.*、指针、嵌套集合等类型的原深比较：time.Time 的可直接复制属性不等于 == 与 DeepEqual 语义一致。顶层整字段 DeepEqual、nil/长度检查、新增/删除项处理和深度回退保持不变。无新增生产函数、模型字段或公共抽象。

新增生成类型边界回归，覆盖整数、浮点、time.Time map/slice 和嵌套 map slice 的比较选择；改前失败、改后通过。临时前后测试验证整数 map 新增/删除/修改、字符串 slice 精确变化及 nil/空集合替换。另用实际 txgame GDHero.Attribute 方法的反射版/直接比较版验证 NaN、正负零、无穷和普通值变化，采集位于临时目录。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预建旧值/当前值和复用的 Store 操作表，两版预编译后交替六轮，每项 200ms；输入中仅一个元素变化。先测逐项 helper，再测保留顶层深比较的实际 snapshotDiffFrom 入口，中位数如下：

| 场景 | 改前 ns/op | 改后 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 逐项，整数 map 100 项 | 8793 | 3370 | 1600→0 | 200→0 |
| 逐项，字符串 slice 32 项 | 2541 | 72.665 | 1024→0 | 64→0 |
| 差异入口，整数 map 100 项 | 14823.5 | 9135.5 | 2852.5→1205.5 | 356.5→150.5 |
| 差异入口，字符串 slice 32 项 | 3083.5 | 674.4 | 1072→48 | 66→2 |

完整差异入口中位耗时约下降 38.4% 和 78.1%。map 外层 DeepEqual 的提前结束受遍历顺序影响，每轮耗时和分配数不同，表中小数为六轮中位数，不是固定单次分配量。此处不包含 BeginChange 的快照复制、数据库写入或业务流程，不外推整条持久化链路收益。证据目录 /private/tmp/tx-scalar-diff-20260911 包含旧 fixture、overlay、两组测试与六轮数据/results.json、entry-results.json、边界测试及范围/回归日志；压测采集与生产路径隔离。

94 处逐项比较改为直接比较（fixture 2、txgame 91、txgameslg 1）。生成文件与模板净行数不变；增加一个生成类型边界回归测试，不以删行为目标。累计未提交生成物仍净增 740 行（fixture 36、txgame 685、txgameslg 19）。从 HEAD 逐文件核对仅包含日志还原、S74、S76。S75 原代码保留。本批不提交，提交建议累计实际变更及评估记录；下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过，前后语义与浮点边界验证通过；生成范围及六工程 diff 检查通过。后置 ponytail-review 无新增简化项。客户端无变化，未另跑真实集群或 race。


## S77：内置标量 slice 的外层比较使用 slices.Equal（2026-09-11）

本轮起始六工程干净，前批次已提交（tx c8aeb73、txgen 47ef186、txgame aa05465、txgameslg 87a9c6e）。在生成模型中明确选择内置整数、bool、string 元素的 slice，其外层 snapshotDiffFrom 比较改为 nil 状态不同或 !slices.Equal。nil 与空 slice 仍触发整字段替换；相同内容、相同底层数组均无变化。浮点/复数因 NaN、time.* 因深比较语义、指针和复杂值、别名及 map 均保留原反射路径。不改变 S76 已采用的逐项比较，也不增加运行时 helper。

选择规则用于模板和导入决策；仅需 typed slice 比较的 Store 可去掉 reflect，slices 导入归入标准库组。扩展现有类型边界测试，改前失败、改后通过，确认 string slice 选用标准库，float/time/嵌套集合保留深比较。临时前后测试覆盖 nil、nil/空互换、空集合、内容相等/变化和长度改变，核对变化及整字段替换。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，实际 GDRole snapshotDiffFrom，32 项 string slice，两版预编译后交替六轮、每项 200ms。旧值、当前值和 Store 在计时前创建，操作表预热复用；不包含快照创建和持久化，中位数如下：

| 场景 | S76 ns/op | S77 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 第 17 项变化 | 683.55 | 269.65 | 48→0 | 2→0 |
| 内容相等，独立数组 | 786.75 | 222.75 | 48→0 | 2→0 |
| 共享底层数组 | 336.20 | 222.20 | 48→0 | 2→0 |

中位耗时分别约下降 60.6%、71.7%、33.9%，三个场景分配均降为零。证据目录 /private/tmp/tx-slice-equal-20260911 包含旧 fixture、overlay、语义测试、六轮原始数据/results.json、范围核对和回归日志。之后仅修正 slices 的导入分组并重新生成，比较函数体未变；采集逻辑不进入生产路径。

34 处外层比较调整（fixture 1、txgame 33）。因标准库导入生成物净增 18 行（fixture 1、txgame 17），不以删行数为目标。txgameslg 与两个客户端生成物无变化。按 HEAD 逐文件核对仅含预期比较及导入变化。本轮不提交，提交建议从此次干净基线重新统计；下一项具体候选尚需评估，不能将该规则直接扩大到包含 NaN 或复杂值的集合。

本轮 txgen、txgame/server、txgameslg/server 全量回归通过；导入分组修正后重新生成，并复跑 txgen 全量测试和两个 server 生成包编译，均通过。生成范围及六工程 diff 检查通过，后置 ponytail-review 无新增简化项。未另跑真实集群或 race。


## S78：内置标量 map 的外层比较使用 maps.Equal（2026-09-11）

保留未提交 S77。仅 map 的键和值均为内置整数、bool 或 string 时采用 typed 比较；生成模型与 slice 复用内置标量分类规则。浮点/复数键值、时间、指针、别名和复杂类型继续 DeepEqual。生成表达式先比较 reflect.ValueOf(map).Pointer()，保留同一 map 的身份快速路径；身份不同时，nil 状态不同或 !maps.Equal 才触发原差异处理。不新增运行时包装函数。

Go 1.27 maps.Equal 源码按键遍历，没有同一 map 立即返回的分支；reflect.DeepEqual 有该快速路径，故不能只替换函数名后忽略共享 map 性能。使用标准 reflect.Value.Pointer 读取 map 标识，不访问运行时内部布局；只去掉外层 DeepEqual 的深反射比较，仍保留身份判断所需的反射。nil/空区别、新增/删除/覆盖项的原逐项差异逻辑和深度回退不变。

扩展既有生成边界测试，确认整数 map 选择身份检查和 maps.Equal，浮点键和值保留 DeepEqual：改前失败，改后通过。临时前后测试覆盖 nil、nil/空互换、空 map、相同内容、值变化及键集合变化。先在临时 overlay 验证表达式，再应用模板；最终 GDDailyTaskStore.go 与格式化后的基准候选逐字节一致。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，实际 GDDailyTask snapshotDiffFrom，100 项 int32→int map；旧值、当前值与 Store 预先创建，操作表预热复用。两版预编译后交替六轮，每项 200ms，中位数如下：

| 场景 | S77 ns/op | S78 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 一项值变化 | 8930 | 4212.5 | 1214.5→0 | 151→0 |
| 内容相等，独立 map | 10622 | 1710 | 2400→0 | 300→0 |
| 同一个 map | 99.72 | 10.93 | 0→0 | 0→0 |

耗时分别约下降 52.8%、83.9%、89.0%，变化和相等场景的分配降为零。改前变化场景受 map 遍历提前结束影响，表中为六轮中位数，并非固定分配量。计时不含 BeginChange 复制和实际持久化，不能外推整条存储链路。证据在 /private/tmp/tx-map-equal-20260911，包含旧 fixture、候选、overlay、前后语义测试、六轮原始数据/results.json、字节一致性核对及范围/回归日志，采集不进入生产路径。

本轮 50 处 map 比较调整（fixture 1、txgame 48、txgameslg 1），新增标准库导入使生成物净增 25 行。累计 S77–S78 共 34 处 slice 和 50 处 map 比较，生成物净增 43 行（fixture 2、txgame 40、txgameslg 1）。按 HEAD 逐文件核对仅含上述比较与导入变化。全部未提交，提交建议累计 S77–S78；下一项具体候选尚需评估，不直接扩大特殊类型范围。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；生成范围及六工程 diff 检查通过。后置 ponytail-review 无新增简化项；两个客户端无变化，未另跑真实集群或 race。


## S79：浮点值 map 复用身份检查与 maps.Equal（2026-09-11）

保留未提交 S77–S78。本轮仅扩展 snapshotMapEqualType 的值类型，允许 float32/float64；键仍限定为内置整数、bool、string。继续复用 S78 的身份检查、nil 状态判断和 maps.Equal 表达式，不新增运行时 helper，也不改模板。浮点键、复数、时间、指针、别名和浮点 slice 均保持原范围。

核对当前 Go reflect.DeepEqual：map 先检查 nil、长度和身份，再查找键并深比较值；浮点值使用 ==。因此同一个含 NaN 的 map 在原实现中相等，而两个独立 map 的 NaN 值不相等；已有身份快速路径使本次扩展保持这一差异。原有类型边界回归扩展 float32/float64 map 值，继续断言浮点键和浮点 slice 使用反射，改前失败、改后通过。实际 GDHero 前后验证独立 NaN、共享 NaN、正负零、无穷、普通值变化和 nil/空区别均通过。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，实际 txgame GDHero.snapshotDiffFrom，Attribute 为 100 项 int32→float64 map。旧值、当前值和 Store 在计时前创建，操作表预热复用；两版预编译后交替六轮，每项 200ms，中位数如下：

| 场景 | S78 ns/op | S79 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 一项值变化 | 10808.5 | 5413.5 | 1388→192 | 157→8 |
| 内容相等，独立 map | 12889.5 | 2845.5 | 2592→192 | 308→8 |
| 同一个 map | 1045.5 | 939.85 | 192→192 | 8→8 |

实际差异入口中位耗时分别约下降 49.9%、77.9%、10.1%。GDHero 整体入口仍有 192B/8 次分配，本轮没有继续归因或修改其他字段。改前变化场景因 map 遍历顺序而波动，表中为六轮中位数。计时不含 BeginChange 和持久化；有限数值用于基准，NaN 等特殊值单独验证行为，不由吞吐样本代替正确性验证。证据目录 /private/tmp/tx-float-map-equal-20260911 包含旧 GDHeroStore、overlay、前后测试、六轮数据/results.json、范围及回归日志，采集与生产路径隔离。

本轮仅 txgame 的 9 处浮点值 map 比较变化，导入使生成物净增 4 行；fixture 和 txgameslg 无本轮生成差异。累计 S77–S79 共 34 处 slice 与 59 处 map 比较，生成物净增 47 行（fixture 2、txgame 44、txgameslg 1）。按 HEAD 逐文件核对仅包含上述比较与导入变化。全批未提交，提交建议累计 S77–S79；下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；生成范围及六工程 diff 检查通过，后置 ponytail-review 无新增简化项。两个客户端无变化，未另跑真实集群或 race。


## S80：纯标量 GD 值先直接判等（2026-09-11）

S77–S79 已提交，本轮起始六工程干净。GDHero 四个 GDSkill 值字段的反射装箱产生 192B/8 次分配。仅对本次数据扫描中全部字段均为内置整数、bool、string 的 GD 值生成 `old.Skill != current.Skill && !reflect.DeepEqual(old.Skill, current.Skill)`。内部 state 指针也参与直接比较，因此不等时必须保留深比较；两个 state 地址不同但内容相等不能误报变更。浮点数、接口、集合、未知类型不进入此规则；命令辅助类型保持原比较路径，不新增运行时 helper。

先评估的按地址反射 helper 虽消除装箱，但相等/变化场景耗时约增 3–4%，且需处理同址语义，未采用。原始数据保留在 results.json；最终直接判断方案见 direct-results.json。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，实际 GDHero snapshotDiffFrom，四个技能值及其他字段为预建数据，操作表预热；两版预编译、交替六轮，每项 200ms，中位数如下：

| 场景 | 改前 ns/op | 改后 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 技能相等 | 995.85 | 133.15 | 192→0 | 8→0 |
| 一个技能 Level 变化 | 912.5 | 294 | 192→48 | 8→2 |
| 同一个 Hero | 1006 | 135.05 | 192→0 | 8→0 |

耗时分别约下降 86.6%、67.8%、86.6%。此样本不含非空 Attribute map、BeginChange 复制或持久化，不能与 S79 的 100 项 map 样本直接比较，也不代表端到端收益。变化字段仍反射比较，收益主要来自相等字段跳过装箱。采集代码仅位于 /private/tmp/tx-value-diff-20260911，包含两个候选、overlay、前后语义测试、六轮数据与回归日志。最终 GDHeroStore 与实测候选 gofmt 后字节一致。

生成器类型边界测试改前失败、改后通过；实际 GDHero 前后验证技能变更、同址值和不同 state 地址但内容相等。txgame 三个文件共 7 处比较增加相等快速判断，逐文件还原这 7 处后与 HEAD 一致，生成物净增 0 行；txgen 实现和测试净增 28 行。fixture、txgameslg 和两个客户端无生成变化。下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；收紧复杂 GD 类型的生成断言后复跑 txgen 全量测试通过。生成范围与六工程 diff 检查通过；后置 ponytail-review：Lean already. Ship. 未新增可删减项。未另跑真实集群或 race，全部变更未提交。


## S81：直接生成对象指针字段先比较身份（2026-09-11）

S80 已提交，本轮起始六工程干净。复用现有 GeneratedPointer 模板标记，在直接生成对象指针字段的 DeepEqual 前增加 `old.Field != current.Field`；同为 nil 或同指针时跳过反射，不同指针继续深比较。指针相等本来就满足 reflect.DeepEqual，不扩大到结构值或接口；未修改集合元素比较，不新增 helper、模型字段或导入。

例如活动对象引用未变化时直接跳过，两个独立但内容相等的活动对象仍由 DeepEqual 判等；BeginChange 的深拷贝保留，之后原地修改子对象仍能被检测。生成类型边界断言改前失败、改后通过，现有嵌套指针快照复制测试保留。实际 GDActivity 前后验证 nil、同指针、独立相等、内容变化、新增和移除六种情况通过。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，实际 GDActivity snapshotDiffFrom；旧值、当前值和 Store 预先创建，操作表预热，两版预编译交替六轮，每项 200ms，中位数如下：

| 场景 | 改前 ns/op | 改后 ns/op | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|
| 三个指针均 nil | 84.805 | 6.283 | 0→0 | 0→0 |
| ActNewbie 同指针 | 153.7 | 6.2125 | 0→0 | 0→0 |
| ActNewbie 独立且相等 | 322.05 | 260.65 | 0→0 | 0→0 |
| ActNewbie.StartTime 变化 | 198.15 | 132.1 | 0→0 | 0→0 |

后三种场景另外两个字段均 nil；耗时分别约下降 92.6%、96.0%、19.1%、33.3%，分配不变。计时不含 BeginChange 复制和持久化，不能外推端到端收益，也不代表所有字段均为独立非空对象时的收益。证据在 /private/tmp/tx-pointer-diff-20260911，包含 overlay、前后语义测试、六轮原始数据/bench-results.json、范围核对和回归日志，采集不进入生产路径。最终 GDActivityStore 与实测候选 gofmt 后字节一致。

本轮 fixture 4 处、txgame 45 处比较更新，共 49 处，生成物净增 0 行；模板行数不变，生成边界测试净增 2 行。逐文件移除新增身份判断后与 HEAD 一致，且每处字段均为已生成对象指针。txgameslg 和两个客户端无变化。下一项评估集合元素的同指针比较，尚需独立 benchmark 后决定。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；前后指针语义验证、最终候选一致性和六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。未另跑真实集群或 race；全部变更未提交。


## S82：集合元素身份判断未采用（2026-09-11）

保留未提交 S81。本轮在临时 overlay 中评估 map 的 `oldItem == currentItem || reflect.DeepEqual(...)` 和 slice 的 `oldItem != currentItem && !reflect.DeepEqual(...)`，仅涉及生成对象指针元素。候选未进入模板和生成物，不新增运行时抽象。

fixture 的实际 GDRoleStore，100 项集合，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，前后预编译交替六轮、每项 200ms。直接调用集合差异方法，中位 ns/op：

| 场景 | map 前→后 | slice 前→后 |
|---|---:|---:|
| 全部共享元素 | 12693→3387.5 | 8418→84.515 |
| 全部独立且相等 | 21876→22263.5 | 15607.5→16119.5 |
| 99 项共享、1 项变化 | 12907.5→3762 | 8634.5→327.95 |

相等场景均为 0B/0 次分配；变化场景均为 88B/2 次分配。共享元素明显受益，但独立相等元素耗时分别上浮约 1.8%、3.3%。当前 BeginChange 的 snapshotCopy 会逐元素复制非 nil 生成对象，普通保存路径以独立对象为主，不能仅凭共享元素微基准扩大优化。

补测完整 GDRoleStore.snapshotDiffFrom 入口：旧集合和当前集合全部使用独立对象；其他字段为零值，操作表预热。计时不含 BeginChange 和实际持久化，中位数如下：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| map 相等 | 34480→34358.5 | 10712→10712 | 109→109 |
| map 一项变化 | 42654→41977 | 6863.5→6528.5 | 59→59 |
| slice 相等 | 23808.5→23819 | 10360→10360 | 11→11 |
| slice 一项变化 | 28470.5→28851 | 5040→5040 | 11→11 |

完整入口耗时变化约 -1.6% 至 +1.3%，未显示稳定收益。map 变化场景受反射提前结束和随机遍历影响，B/op 差异不能据此认定固定分配收益；分配次数保持不变。综合调用方式与结果，本轮不采用候选，避免给普遍的独立指针路径增加判断。之后只有实际 profile 证明大量共享或 nil 元素进入集合差异循环时才重开。

证据在 /private/tmp/tx-collection-pointer-20260911：S81 起点生成物副本、候选 overlay、前后语义测试、bench-results.json 与 entry-results.json 和各六轮原始数据。nil、独立相等、变更、nil 转换以及 map 同指针含 NaN 的语义测试通过；全部采集与生产路径隔离。本轮生产代码及生成物净变化 0 行，提交建议累计 S81 代码及 S82 评估文档。下一项具体生成物候选尚需评估。

本轮 txgen 全量测试、前后候选语义测试和六工程 diff 检查通过；182 个 Store 生成文件与 S81 起点字节一致。后置 ponytail-review：Lean already. Ship. 无新增简化项。本轮未重复两个 server 全量回归，未另跑真实集群或 race；保留 S81 全部未提交变更。


## S83：完整加载字段复制快速路径未采用（2026-09-11）

保留未提交 S81–S82。loadedStoreFields 已计算置位数量，候选在全部字段加载时直接复制字段名表，跳过逐项位图检查。分别评估 slices.Clone 与复用分配后 copy 两种写法；只使用临时 overlay，未改模板或生成物。公开 LoadedFields 契约及其返回副本隔离保持不变，不因缺少当前业务调用而删除 API。

实际 fixture 的生成包 helper，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；两组分别预编译并交替六轮、每项 200ms，字段名与位图预建，返回值写入测试专用 sink。中位 ns/op：

| 字段数/加载状态 | 原实现→slices.Clone | 原实现→copy |
|---|---:|---:|
| 16/完整 | 164.2→176.35 | 167.35→166.65 |
| 16/稀疏一项 | 50.905→52.43 | 52.76→56.68 |
| 16/空 | 5.163→5.414 | 5.2585→5.114 |
| 130/完整 | 867.05→826.65 | 893→815.1 |
| 130/稀疏一项 | 180.35→173 | 172.3→207.7 |
| 130/空 | 6.3995→6.378 | 6.405→6.4985 |

完整加载两版均为 256B/1 次（16 字段）或 2304B/1 次（130 字段），稀疏均为 16B/1 次，空均为 0B/0 次。slices.Clone 使小结构完整加载慢约 7.4%；copy 虽使 130 字段完整加载快约 8.7%，但稀疏加载慢约 20.5%。两组基线各自交错测量，不跨组计算收益。

当前 tx、txgame/server、txgameslg/server 的非生成、非测试 Go 代码未发现 .LoadedFields() 调用；这不排除外部业务使用，但缺少当前热点依据。因此不为此查询增加分支，保留原实现，下一项先依据实际调用或 profile 筛选。

前后候选验证 0/1/64/65/130 字段、尾字高位忽略、声明顺序、空返回非 nil 和返回副本隔离通过。证据位于 /private/tmp/tx-loaded-full-20260911，含两个候选、overlay、bench-results.json、copy-results.json 和六轮原始数据。采集不进入生产路径。本轮生产代码与生成物净变化 0 行，提交建议累计 S81–S83；具体下一候选尚未确认。

本轮 txgen 全量测试、候选前后语义验证与六工程 diff 检查通过，生成物保持 S81 状态。后置 ponytail-review：Lean already. Ship. 无新增简化项。未重复两个 server 全量回归，未另跑真实集群或 race；全部变更未提交。


## S84：StoreView 捕获链路 profile 与后续依据（2026-09-11）

保留未提交 S81–S83。本轮没有试改生产代码，转向现有 BenchmarkGeneratedStoreDirtyRuntime 的生成物捕获链路：16 批、每批 64 项，包含业务对象创建、Store Set、捕获和 ClearChanges；叶子场景使用已有子 Store 修改路径。这是 fixture 负载，不能声称代表真实业务流量或线上占比。

Go 1.27.0 darwin/amd64、GOMAXPROCS=1，预编译测试二进制；不带 profile 的六轮、每项 200ms 中位基线：

| 既有场景 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| CaptureFrameOnly | 501876 | 282290 | 2273 |
| CaptureAndBuildFrameDocument | 513423 | 282770 | 2278 |
| CaptureLeafFrameOnly | 1032282 | 491806 | 5391 |

以上是本轮定位基线，不是前后对照或提速结论。另对 CaptureFrameOnly 采集 3 秒 CPU 与分配 profile；该采样运行约 533µs，未与无采样结果混算。Go 安装无预编译 pprof，使用本机 `go run cmd/pprof` 分析，未安装新依赖。

alloc_space 的主要平面归因：ItemsMapStore.Set 约 26.1%、copyGDRoleStoreViewPath 约 25.2%、基准中的业务对象创建约 17.0%、GDItem.Copy 约 16.9%、ItemsMapStore.recordOp 约 13.3%。alloc_objects 中，业务对象创建约 45.0%、GDItem.Copy 约 44.6%，recordOp 约 7.4%。这些是采样估计，包含基准的业务对象构造，不能把全部分配当成可删框架开销。recordOp 的分配主要在 map 写入扩容；不是每次方法调用都分配。

CPU 样本中 runtime.kevent 与 runtime.madvise 占比约 56.7% 和 9.7%，不据此给业务函数排序或推断调用耗时；本轮主要使用分配 profile 定位。原 profile、文本报告、无采样六轮数据/baseline.json 与验证日志仅位于 /private/tmp/tx-frame-profile-20260911，采集与生产路径隔离。

本轮保留两项可靠性边界：捕获副本保障之后修改业务对象不污染已捕获视图；S54 的操作表引用替换保障撤销恢复，不能把所有 ops=nil 机械改成 clear(map)。下一候选为集合操作表容量管理，需要同时评估有/无 UndoRecorder、历史引用与回滚、持续多帧分配和峰值后内存保留；尚未确认具体方案，不新增池、固定容量或配置项。本轮生产代码及生成物净变化 0 行，提交建议累计 S81–S84。

本轮 txgen 全量测试、六工程 diff 检查通过，182 个 Store 生成文件与 S81 状态一致。后置 ponytail-review：Lean already. Ship. 无新增简化项。未重复两个 server 全量回归，未另跑真实集群或 race；全部变更未提交。


## S85：操作表无限期复用未采用（2026-09-11）

保留未提交 S81–S84。本轮临时 overlay 仅改 GDRoleItemsMapStore.clearChanges：无 UndoRecorder 时 clear(s.ops)，有记录器仍置 nil；dirtyChildren 等路径不动。未改模板和生成物。候选只用于测量容量复用的代价，不宣称已完成所有回滚组合审查。

实际生成 helper，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；同一 Store 持续写操作表并 clearChanges，先预热一次，前后预编译交替六轮、每项 200ms，中位数：

| 每轮操作数 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| 1 | 103.45→35.285 | 128→0 | 2→0 |
| 64 | 4299→950.75 | 2424→0 | 11→0 |
| 1024 | 63441.5→15432 | 38184→0 | 22→0 |

微基准只覆盖操作记录与清除，不包含捕获复制、业务对象创建和数据库；不能直接当作帧吞吐收益。复用确实降低稳态成本，但会保留 map 的历史容量。

峰值后内存独立验证：64 个活 Store，每个操作表插入 4096 项，再清除并强制 GC，保留 Store 引用；业务 Items map 保持空，隔离操作表容量。每版独立进程重复三次，HeapAlloc 相对创建前 GC 基线增加均为：原实现 22448B，候选 4895664B，差额 4873216B（约 4.65MiB）。这不是泄漏判定，但说明只清空内容会让闲置 Store 保留峰值容量。

仅以 len(ops) 设置释放阈值不能准确约束容量：删除条目后 len 可能很小，而底层表仍大。引入额外容量记录、固定阈值或池需要真实集合峰值分布与驻留 Store 规模作为依据。本轮不引入这些机制，保持置 nil 释放引用；也不扩大到 UnTouch、detach 或撤销恢复路径。

临时验证操作清除、下一轮记录，以及有记录器路径保留旧 map 内容通过；证据在 /private/tmp/tx-ops-reuse-20260911，包含候选、overlay、六轮原始数据/bench-results.json、三次峰值内存日志与验证日志。采集不进入生产路径。本轮生产代码及生成物净变化 0 行，提交建议累计 S81–S85。下一步需真实负载依据，尚无已确认的新编码方案。

本轮 txgen 全量测试、临时候选语义测试与六工程 diff 检查通过，生成物保持 S81 状态。后置 ponytail-review：Lean already. Ship. 无新增简化项。未重复两个 server 全量回归，未另跑真实集群或 race；全部变更未提交。


## S86：单节点追加省去临时子路径（2026-09-11）

保留未提交 S81–S85。S84 CPU profile 的生成侧热点包含 appendStorePathNode，本轮只优化已有私有 helper：原来构造完整 child Path 再调用通用 JoinPathLimited，现在复制 prefix 的有效节点并直接写入一个节点。仍使用零值输出以清空尾部槽位，深度达到或超过上限时返回截断祖先和 false；不新增公共 API，不改 JoinPathLimited、appendStoreViewPath 或路径有效性契约。这是有实测依据的单节点特化，不将通用路径拼接逻辑复制到各类型。

临时逐值对照覆盖 uint8 全部深度、带非零尾槽的父路径和不同 key 节点，完整返回 Path 与 complete 均与 JoinPathLimited(prefix, 单节点child) 相同。现有深层子变更截断、祖先只访问一次和快照深度回退测试通过；例如父路径已达四层时仍返回该祖先整体替换，不继续追加。

实际生成 fixture 的既有帧基准，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；前后预编译交替六轮、每项 200ms，无 profile，16 批各 64 项，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| CaptureAndBuildFrameDocument | 519219.5→456174.5 | 282770→282770 | 2278→2278 |
| CaptureFrameOnly | 508544→459038 | 282290→282290 | 2273→2273 |
| CaptureLeafFrameOnly | 1012587→899730.5 | 491806→491806 | 5391→5391 |

耗时分别下降约 12.1%、9.7%、11.1%，分配不变。这是 fixture 捕获负载收益，不外推线上或数据库吞吐。最终 fixture 与实测候选格式化后字节一致（补充解释注释不改变执行代码）。证据目录 /private/tmp/tx-append-node-20260911 包含旧基础层、候选、对照测试、overlay、六轮数据/bench-results.json 和验证日志；采集不进入生产路径。

三套生成 type_registry.go 各净增 6 行，合计 18 行；模板净增 6 行，不新增运行时 helper。逐文件还原本函数与注释后和 HEAD 一致，S81 的其他生成改动保持不变。本批代码累计仍含 S81 的 49 处比较变化；提交建议累计 S81–S86。下一项评估 StoreView 路径追加的同类成本，尚需单独实测，不能把不同溢出返回语义直接合并。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；模板格式整理后重新生成 fixture，复跑 txgen 全量测试并核对实测候选一致性通过。六工程 diff 检查通过，后置 ponytail-review：Lean already. Ship. 无新增简化项。两个客户端无变化，未另跑真实集群或 race；全部变更未提交。


## S87：StoreView 单节点追加复用已有实现（2026-09-11）

保留未提交 S81–S86。appendStoreViewPath 原来构造完整 child Path 再调用 JoinPath，本轮先检查 prefix.Depth >= MaxPathDepth，超限直接返回空 Path/false；未超限时调用 S86 的 appendStorePathNode。复用已有节点追加实现，不新增公共 API、helper 或状态。不能直接无条件转发：StoreView 溢出返回空路径，Store 溢出返回截断祖先，两者契约不同。

临时逐值对照覆盖 uint8 全部深度、带非零尾槽的父路径以及不同 key 节点，完整 Path 与 bool 均和 JoinPath(prefix, 单节点child) 一致；例如三层父路径可追加为四层，四层父路径返回空路径/false。最终 fixture 与实测候选 gofmt 后字节一致。

实际 fixture 的既有 StoreView 基准，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；两版预编译交替六轮、每项 200ms，无 profile，帧参数 16 批各 64 项，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| CaptureMergeAndBuildFrameDocument | 813779.5→772985 | 439259→439259 | 3614→3614 |
| BuildCapturedFrameDocument | 562.6→503.4 | 480→480 | 5→5 |
| BuildCapturedLeafFrameDocument | 756936.5→615129 | 263044→263054 | 3765→3765 |

耗时分别下降约 5.0%、10.5%、18.7%，分配次数不变。叶子文档 B/op 中位差 10B，不视为固定分配变化；包含 map 构建的样本存在分配字节波动。普通帧样本最终是整体字段替换，叶子帧则逐路径构建文档，成本不能直接横向比较。这些结果限于 fixture，不外推线上或持久化吞吐。证据在 /private/tmp/tx-view-append-20260911，包含旧基础层、候选、overlay、逐值测试、六轮原始数据/bench-results.json 与回归日志，采集不进入生产路径。

三套基础层各替换一个已有方法，本轮生成物及模板行数均不变；累计 S86–S87 生成物净增 18 行，S81 的 49 处比较继续保留。提交建议累计 S81–S87；下一项具体候选尚需评估，不自动扩大路径 API。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过；候选逐值对照、最终生成物一致性和六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。两个客户端无变化，未另跑真实集群或 race；全部变更未提交。


## S88：文档路径按实际段数预留容量（2026-09-11）

S81–S87 已提交，本轮起始六工程干净。Path.FullPath 原来按 Depth*2 预留 DocumentPath 容量，普通字段节点也预留了不存在的 key 段。现在在原深度校验后统计非 KeyNone 的节点，以 Depth+keyCount 预分配；字段解析、key 转换、错误返回和最终段顺序保持不变。例如 Items.1001.Quantity 需要三段，原来按两层预留四段，现在预留三段，返回数据仍独立持有。

这是公共运行时容量调整，不新增 helper 或配置项；只遍历至多四个节点，不改变生成契约。未知 key 类型仍交由原转换路径拒绝，深度非法或字段解析失败仍返回 nil。既有后端字段名、嵌套字段和非法 key/路径测试保留。

实际 fixture 的既有文档构建基准，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；两版预编译交替六轮、每项 200ms，无 profile，帧参数 16 批各 64 项，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| CaptureMergeAndBuildFrameDocument | 767967.5→775739 | 439259→439243 | 3614→3614 |
| BuildCapturedFrameDocument | 489.5→481.25 | 480→464 | 5→5 |
| BuildCapturedLeafFrameDocument | 612734→617197.5 | 263043→246676 | 3765→3765 |

耗时变化约 +1.0%、-1.7%、+0.7%，不宣称提速，也不能据此确认绝对无耗时成本。收益是每个普通字段层少预留一个字符串槽：本样本叶子帧约少 16KiB（约 6.2%）分配字节；map 分配字节波动使表中差额不必恰好等于 16*1024。分配次数不变。普通帧最终整体替换只有一条输出路径，叶子帧逐路径构建，不能将两者耗时直接横向比较；不外推业务或持久化吞吐。

证据在 /private/tmp/tx-path-capacity-20260911，包含原实现、候选、overlay、六轮数据/bench-results.json 和验证日志；最终 path.go 与实测候选 gofmt 后字节一致，采集不进入生产路径。本轮公共 Go 实现净增 6 行，生成物变化 0 行，仅 tx 有变更。下一项具体候选尚需评估。

本轮 tx、txgen、txgame/server、txgameslg/server 全量测试通过，路径解析定向测试、实测候选一致性与六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。未另跑真实集群或 race；仅 tx 有未提交变更。


## S89：FullPath 解析分支外提未采用（2026-09-11）

保留未提交 S88。临时 overlay 将 PathFieldNameResolver 类型判断移到 FullPath 循环外，并将普通/嵌套字段名解析分支直接放入循环；保留 FieldNone 拒绝和公开 FieldNameAt。目的是减少每层调用中的重复检查，但会让两处维护相近的解析规则。候选未进入生产代码。

实际 fixture 的既有文档基准，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；两版预编译交替六轮、每项 200ms，无 profile，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| CaptureMergeAndBuildFrameDocument | 760359→766178.5 | 439243→439243 | 3614→3614 |
| BuildCapturedFrameDocument | 500.8→506.2 | 464→464 | 5→5 |
| BuildCapturedLeafFrameDocument | 601136→612527 | 246634→246664 | 3765→3765 |

耗时分别上浮约 0.8%、1.1%、1.9%，分配次数不变；叶子场景分配字节的微小变化不认定为固定变化。未显示收益，且增加解析逻辑重复，因此不采用；不把类型断言外提视为必然优化。本轮只验证性能取舍，未对未采用候选做完整行为回归。

证据在 /private/tmp/tx-path-resolver-20260911，包含 S88 起点 path.go、候选、overlay、六轮原始数据/bench-results.json 与原实现定向验证日志，采集不进入生产路径。本轮生产代码及生成物净变化 0 行，提交建议累计 S88–S89；下一项具体候选尚需评估。

本轮原实现路径/文档构建定向测试、S88 起点字节一致性和六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。仅补评估文档，未重复全量回归、真实集群或 race；保留 S88 全部未提交变更。


## S90：文档路径相等比较使用标准库（2026-09-11）

保留未提交 S88–S89。DocumentPath.Equal 的手写长度检查与逐项字符串比较，与本机标准库 slices.Equal 的算法一致，改为直接调用 slices.Equal；公开方法和分段相等契约保留。例如 nil 与空切片仍相等，而 {"a.b", "c"} 与 {"a", "b.c"} 虽然点连接结果相同，仍不相等。未改为字符串比较，未引入依赖。

实际公开方法前后编译对照，Go 1.27.0 darwin/amd64、GOMAXPROCS=1，交替六轮、每项 200ms，中位数：

| 场景 | ns/op 前→后 | B/op | allocs/op |
|---|---:|---:|---:|
| nil/空 | 1.2775→1.2845 | 均 0 | 均 0 |
| 三段相等 | 6.789→6.668 | 均 0 | 均 0 |
| 末段不同 | 5.4285→5.3995 | 均 0 | 均 0 |

差异约 0.12ns 以内，未发现明显退化，不宣称提速或业务收益；本项为标准库替代重复维护。临时语义检查覆盖 nil/空、相等、分段边界差异和长度差异，两版均通过。最终 types.go 与实测候选 gofmt 后字节一致。

证据位于 /private/tmp/tx-document-equal-20260911，含原实现、候选、overlay、前后测试、六轮原始数据/bench-results.json 与回归日志，采集不进入生产路径。本轮公共 Go 文件含导入调整净减 5 行；累计 S88–S90 公共实现净增 1 行，生成物无变化，仅 tx 有变更。下一项具体候选尚需评估。

本轮 tx、txgen、txgame/server、txgameslg/server 全量测试通过，前后语义测试、实测候选一致性与六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。未另跑真实集群或 race；仅 tx 有未提交变更，提交建议累计 S88–S90。


## S91：Mongo unset 表延迟分配未采用（2026-09-11）

保留未提交 S88–S90。临时 overlay 将 buildUpdateDocument 的 unsetDoc 改为 nil，首次 OpDelete 才 make；期望避免纯设置时创建未使用表。不修改 setDoc、ID 保护、unsafe key 回退或文档操作顺序。候选未进入生产代码。

实际 BuildUpdateDocument，Go 1.27.0 darwin/amd64、GOMAXPROCS=1；输入预建，前后预编译交替六轮、每项 200ms，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| 一项设置 | 1161.5→1143 | 1056→1016 | 10→10 |
| 一项删除 | 1147.5→1153.5 | 1056→1064 | 10→11 |
| 设置和删除混合 | 1631→1662.5 | 1360→1368 | 12→13 |

纯设置只少 40B，未减少分配次数；删除和混合场景增加 8B/1 次分配。候选 -m=2 逃逸分析明确报告 unsetDoc 被闭包按引用捕获、宽度 8，并 moved to heap；这解释了延迟创建 map 同时引入变量逃逸的额外成本。没有充分收益抵消新增分支和删除场景退化，保留预先创建表的原实现。不继续增加 holder、二次 map 查询或专用 helper 来规避这一成本。

六轮对照同时运行既有 TestMongoBuildUpdateDocument 系列，覆盖 BSON 字段名、嵌套清空类型、ID 保护、非法路径/操作、nil 值和 unsafe key 回退，两版均通过；这不是实际 Mongo I/O 测试。证据位于 /private/tmp/tx-mongo-unset-20260911，含原实现、候选、overlay、六轮数据/bench-results.json 和 escape-after.log。采集不进入生产路径。本轮生产代码与生成物净变化 0 行，提交建议累计 S88–S91，下一项具体候选尚需评估。

本轮原实现 Mongo 文档定向复测、候选前后测试、起点字节一致性及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。仅补评估文档，未重复全量回归、真实集群或 race；保留全部未提交变更。


## S92：GD 完整加载直接覆盖目标对象（2026-09-11）

保留未提交 S88–S91。GD.Load 在 store.Value != gd 时，原先通过 value 临时结构清空 state 后覆盖 gd；现在直接 `*gd = *store.Value`，再 `gd.state = nil`，随后按原顺序 gd.attachStore(store)、store.Value = gd。少一行临时赋值，保留原对象地址、Store 绑定和加载位图；不改变 LoadFields、缺失/错误返回或公开 API。

这只是现有同步加载块内的改写，没有新增并发保证，也不把源码赋值次数当作机器复制次数。临时前后测试验证原 Store 与原 GD 地址保持、字段值更新、已加载状态、无残留脏状态以及后续 Setter 能记录变更，两版通过。

实际 fixture 的 GD.Load 入口使用预建 recordingDocumentLoader，返回 RoleId/Name/Power 三个字段；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，前后预编译交替六轮、每项 200ms，中位 2907.5→2862ns/op，两版均 768B/20 次分配。约 -1.6% 的耗时变化不认定明确提速；该样本含现有加载解码链路，不含实际数据库 I/O，不能外推真实存储延迟。证据在 /private/tmp/tx-load-copy-20260911，含旧生成物、候选、overlay、前后测试、六轮数据/bench-results.json 与回归日志；最终 fixture 与实测候选字节一致，采集不进入生产路径。

三套生成物分别减少：fixture 3 行/3 文件、txgame 33 行/33 文件、txgameslg 4 行/4 文件，合计净减 40 行；模板净减 1 行。逐文件反向还原该赋值块后与 HEAD 一致。首次全量回归仅因旧生成文本断言要求 *gd=value 而失败，同步为新写法后重跑；保留原 Copy 的独立 state 清空断言。

本轮只改生成完整加载入口；S88–S90 公共运行时累计净增 1 行继续保留。提交建议累计 S88–S92，下一项具体候选尚需评估。

同步生成文本断言后，本轮 txgen、txgame/server、txgameslg/server 全量测试通过；前后加载语义、生成范围、候选一致性及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。未另跑真实集群或 race，两个客户端无变化；全部变更未提交。


## S93：投影加载删除只读字段列表副本（2026-09-11）

保留未提交 S88–S92。loadStoreFields/loadStoresFields 原来分别复制 requestedFields 和 loadFields；前者仅交给私有 finishLoadedStore，通过生成 InitFields 只读遍历，后者才传给 loader 并补齐 RecordKey。删除前者的复制，直接使用原 fields；保留 loadFields 的独立副本。生成初始化逻辑不写字段列表，因此无需第二份数组；不删除 loader 入参隔离或主键补齐，不改变公开加载接口。

例如投影 Name、Items 时，仍只初始化请求的可初始化字段，loader 收到独立的 Name、Items、RoleId 列表；调用方切片及备用容量不被主键追加修改。单条与批量既有 DoesNotMutateProjectionSlice 测试在前后六轮中均通过，生成文本断言同步新参数名。

实际 fixture 的单条与两条批量投影入口，预建 recordingDocumentLoader 和两字段投影 Name/Items；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，前后预编译交替六轮、每项 200ms，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| 单条投影加载 | 1871.5→1786.5 | 736→704 | 13→12 |
| 两条批量投影加载 | 3503.5→3362 | 1368→1336 | 24→23 |

耗时分别约下降 4.5%、4.0%，两场景每次少 32B/1 次分配，符合删除两字符串副本；批量只减少每批的一份字段列表，不是每条记录各省一份。样本包含现有加载解码与初始化，不含真实数据库 I/O，不外推存储延迟。证据在 /private/tmp/tx-projection-copy-20260911，含旧基础层、候选、overlay、前后投影隔离测试、六轮原始数据/bench-results.json 和回归日志；最终 fixture 与实测候选字节一致，采集不进入生产路径。

三套 type_registry.go 各净减 2 行，共 6 行；模板净减 2 行。逐文件与 HEAD 核对仅为删除两处副本及替换传参。累计 S92–S93 生成物净减 46 行，S88–S90 公共运行时净增 1 行继续保留；提交建议累计 S88–S93，下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过，前后投影隔离测试、生成范围、候选一致性及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。两个客户端无变化，未另跑真实集群或 race；全部变更未提交。


## S94：局部加载复用已生成的加载位图（2026-09-11）

Store.LoadFields 的临时 Store 已经标记请求字段和补齐的主键。合并值后逐字 OR 其 loadedFields，删除重复的字段列表复制、主键追加和名称查找。保留 loader 的隔离列表、请求字段初始化、未请求字段和变更记录；失败或不存在仍直接返回，不改标记。按整个位图数组遍历，不引入 64 字段上限。

例如先加载 Name/Items，再加载 Power，最终标记仍为 RoleId/Name/Power/Items。既有加载跟踪测试增加这一次连续投影断言，防止误用位图覆盖。没有增加公共 helper 或运行时采集逻辑。

实际 fixture Store.LoadFields，预建 loader、目标 Store 和 Name/Items 投影；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，前后预编译交替六轮，每轮 200ms，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| 合并局部加载 | 2023.5→1798.5 | 808→712 | 15→13 |

样本耗时下降约 11.1%，少 96B/2 次分配；不含真实数据库 I/O，不外推数据库延迟。临时测试、overlay、前后源码、原始六轮数据与回归日志在 /private/tmp/tx-loaded-union-20260911；最终 GDRoleStore 与实测候选字节一致。

40 个生成 Store 各增加 2 行：fixture +6、txgame +66、txgameslg +8，共 +80 行；模板 +2 行，关键行为测试 +8 行。累计 S92–S94 生成物净增 34 行，S88–S90 公共运行时仍净增 1 行。接受这两行循环换取删除重复分配，不以删行数为目标。全部变更未提交，提交建议累计 S88–S94；下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过，gen/run.sh 已执行，生成 Store 差异逐文件核对及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。本轮未重复执行未修改的 tx 全量测试，未运行真实集群或 race；两个客户端无变化。


## S95：加载入口移除被覆盖的 GD 分配（2026-09-11）

私有 codec.newStore 通过 NewStore(nil) 创建空 GD，decodeDocument 随后又用 NewGD 创建解码对象并覆盖 Value，第一份 GD 无使用者。单条和批量加载直接 new(S)，删除 codec 构造回调及 40 个生成闭包。解码仍负责 NewGD、JSON 解码和 Value 赋值，随后主键补齐、初始化及加载标记顺序不变；公开 NewStore(nil) 契约保留。不存在及解码失败的返回边界不变，没有新增公共 SPI 或采集逻辑。

例如加载 Name/Items 时，从“空 GD + 解码 GD”变成只创建解码 GD；每条成功加载记录减少一个无效对象。

沿用实际 fixture 单条与两条批量投影场景，预建 recordingDocumentLoader 与 Name/Items 投影；Go 1.27.0 darwin/amd64、GOMAXPROCS=1，前后预编译交替六轮，每项 200ms，中位数：

| 场景 | ns/op 前→后 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|
| 单条投影加载 | 1714→1699 | 704→544 | 12→11 |
| 两条批量投影加载 | 3354.5→3271.5 | 1336→1016 | 23→21 |

耗时中位数分别低约 0.9%、2.5%，不据此宣称明显提速；每条记录减少 160B/1 次分配是主要收益。未包含真实数据库 I/O。证据位于 /private/tmp/tx-load-allocation-20260911，包含修改前后源码、overlay、临时基准、六轮原始数据与回归日志；候选经 gofmt 后与最终 fixture 字节一致。前后投影切片隔离测试随基准运行并通过。

本轮 40 个 Store 各减 1 行，三套基础层各减 1 行，共净减 43 行；两模板净减 2 行，删除一条过时构造闭包文本断言，复用现有完整/投影/批量加载和状态测试。累计 S92–S95 生成物净减 9 行，S88–S90 公共运行时净增 1 行保留。全部未提交，提交建议累计 S88–S95；下一项具体候选尚需评估。

本轮 txgen、txgame/server、txgameslg/server 全量测试通过，gen/run.sh 已执行，候选一致性、加载构造链路及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增简化项。未重跑未修改的 tx 全量测试，未运行真实集群或 race；两个客户端无变化。
