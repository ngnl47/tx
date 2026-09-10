# 六工程 Ponytail 瘦身审计与补充计划（2026-09-10）

本轮显式使用 Ponytail 4.9.0 的 `ponytail-audit`，串行扫描六工程。仅补充审计与计划文档，不实施下述代码候选，不提交 Git。原分层审计的完成项不重复计入本轮收益。

## 结论

保留四项代码候选和一项文档纠偏；没有确认可删除的依赖。建议首批只删除 txgen 的四个无调用私有函数，再处理标准库替代；低频压测文件处理复用分别实施。角色/世界压测循环、连接缓存、公开工具替换不进入本批。

Ponytail 负责发现复杂度，最终筛选仍服从工程边界。单实现接口、单产品工厂、测试桩及一行转发都不是独立删除理由；插件扩展和可测试性也是有效职责。正确性、认证漏洞和性能瓶颈不作为本次瘦身发现，遇到具体证据须另开正常审查。

## 当前基线与扫描范围

开始扫描时六个工作区均干净，上一轮 RouteKey 比较已提交。revision 是本轮静态证据的定位基线，未来实施前必须复核。

| 工程 | HEAD | 手写源文件 | 测试文件 | 生成文件 | 模板 |
| --- | --- | ---: | ---: | ---: | ---: |
| tx | a4e20ea | 197 | 168 | 9 | 0 |
| txgen | 9c2ea1e | 25 | 25 | 37 | 14 |
| txgame | 01233b1 | 121 | 48 | 1132 | 1 |
| txgameslg | e8c15df | 72 | 62 | 48 | 1 |
| txclient | 5514f2a | 25 | 19 | 3 | 0 |
| txclientslg | 5fd5641 | 30 | 23 | 3 | 0 |

共扫描 2063 个 Git 跟踪的 `.go/.ts/.vue/.tmpl` 文件：遍历全文进行标识符引用、重复函数候选和规模筛选，再人工追踪入选候选的调用链。生成目录和 protobuf 产物纳入清单及引用检索，发现问题追溯模板或输入，不手改产物。规模分类按目录与文件名，仅用于定位；例如项目维护的 `server/gen/config` 也归入生成目录统计，不表示 txgen 已接管其生成。

同时检查六工程 AGENTS.md、相关 README、tx/txgen 当前设计和实施计划、已有模板评估报告，以及 Go/npm 依赖声明和调用。不是逐行人工认证全部代码，不以文本命中数证明运行期行为，也不宣称发现了全部死代码。

| 工程 | 本轮深查边界 | 结果 |
| --- | --- | --- |
| tx | Bee/Ant 能力门面、RPC/MsgQ 连接生命周期、存储接口、工具、Redis 测试 | 保留能力与生命周期边界；S03 可用标准库替代 |
| txgen | 模型构建、标签校验、模板引用、生成计划与测试隔离 | S01；不重复处理已完成的模板专项 |
| txgame | 组合根、Handler 注册、路由验证、CLI/验收及依赖 | 无新增确认代码候选；独立示例继续保留 |
| txgameslg | 世界加载/写回、Login SPI、角色/世界性能汇总 | S04、S05 |
| txclient | 协议、传输、CLI、界面及依赖 | 无新增确认代码候选；不为凑工程覆盖数制造问题 |
| txclientslg | 协议、传输、两类压测流程、注入测试与默认文件 I/O | S02、S05；测量循环暂缓 |

## 入选清单（按预计代码删减规模排列）

预计数字只计当前实现、必要替代代码和 import，不包含未来新增测试、报告或格式调整。无代码已删除；抽取项的精确净值在实施后复算。

| ID | Ponytail 标签与建议 | 预计净减少 | 风险 |
| --- | --- | ---: | --- |
| S01 | `delete:` 删除四个无调用私有函数；无需替代 | 37 行 | 低 |
| S02 | `shrink:` 同一客户端两套压测共用文件屏障与 JSON 写入 | 23–25 行 | 低至中 |
| S03 | `stdlib:` 两个测试 slice 比较函数换成 `slices.Equal` | 23 行 | 低 |
| S04 | `shrink:` 两类性能汇总共用原子 JSON 写入过程 | 20–23 行 | 低至中 |
| S05 | `delete:` 生效文档移除旧 WAL/checkpoint 验收描述 | 不计代码 | 低 |

### S01：txgen 私有残留函数

位置：

- [model.go:230](../../../txgen/gen/model.go#L230)：`buildDataStructModel`。
- [model.go:349](../../../txgen/gen/model.go#L349)：`buildStoreModel`。
- [validate.go:584](../../../txgen/gen/validate.go#L584)：`serializationTagNames`。
- [validate.go:637](../../../txgen/gen/validate.go#L637)：`serializationTagNameKeys`。

当前精确标识符检索只命中各自声明，模板与测试也没有引用。前两个是向 `WithKnownTypes` 入口传 nil 的三行包装；真正渲染走 `render_data.go:112`、`render_store.go:4`。后两个已不参与标签校验；当前 `serializationTagKeys`、`namedBackendTagKeys` 和 `serializationTagName` 仍有职责，必须保留。

这四个函数是包私有实现，不是 SPI、注册回调、插件入口或测试桩。按函数声明 3+3+18+9 行及分隔空行计，预计删除 37 行。不连带重命名有调用入口，不因名称相近删除当前校验。

例：普通 GD 生成仍直接携带 `knownDataTypes` 构建模型，删除未被使用的 nil 包装不会改变生成条件。

实施验证：先生成再全量测试 `go run . && go test ./... -count=1`；确认固定产物无差异，保留标签冲突和生成前失败检查。无需为“函数已删除”新增字符串禁令测试。没有当前运行调用，不承诺运行时或生成吞吐提升。

### S02：txclientslg 压测文件处理复用

位置：[cluster-perf.ts:289](../../../txclientslg/src/cli/cluster-perf.ts#L289)、[cluster-world-perf.ts:305](../../../txclientslg/src/cli/cluster-world-perf.ts#L305)。两文件均维护相同的 `waitForFile`、`writeJSONAtomic`、`isMissingFile`，共两套文件轮询和临时文件 rename 逻辑。

建议仅在本工程 CLI 内共用这三个文件处理函数；没有合适现有文件时使用一个局部 `perf-files.ts`，不建设通用 I/O 框架，不让 txclient 跨仓引用，不移动到 AgentClient 或生产协议路径。`asError` 的两行转换不值得单独公共化。

必须保留 `login/createTransport/now/writeReady/waitForStart/writeResult` 测试注入接口。它们用于控制时序和隔离真实网络，属于测试接缝，不是应删的“单实现接口”。角色和世界的结果类型、最终状态、业务响应校验保持各自独立。

例：角色与世界负载仍各自等待 `.measure`；复用的仅是“文件不存在时继续等待，其他文件错误直接返回”的机制。10ms 轮询、超时错误、JSON 换行和 rename 行为不在这次整理中改变。

实施验证：`npm test`、`npm run build`；保留两类负载的失败统计、单在途、屏障及响应拒绝测试。现有测试多数注入文件函数，不足以覆盖默认磁盘处理；新增最小临时目录检查默认写入/读取及错误传播，避免复制两份同构 I/O 测试。确认普通 TCP/登录工具不依赖此压测文件模块。收益是维护面减少，不承诺测量吞吐变化。

### S03：tx 测试比较复用标准库

位置：[storage_redis_adapter_test.go:2030](../../tests/storage_redis_adapter_test.go#L2030)、[2042](../../tests/storage_redis_adapter_test.go#L2042)：`sameStrings`、`sameBools` 各 11 行，仅按长度和逐元素相等比较。

同包调用改为 `slices.Equal`，删除两函数并新增标准库 import。预计净减少 23 行。已对照本机 Go 1.27 标准库实现：nil 与空 slice 均按长度零视为相等，顺序敏感，与当前 helper 一致。`hasMGetCall` 的外层“是否存在某次调用”职责仍保留，只替换其内部比较。

例：批量读取的存在掩码 `[true,false]` 仍与 `[false,true]` 不同。删除的是比较实现，不是 Redis key 顺序、批量 I/O 次数、路由隔离或缺失项断言，也不是删除记录调用的假客户端。

实施验证：tx 全量测试，重点保留 Redis Adapter/Cluster 相关断言；无需测试 Go 标准库本身。本项仅影响测试代码，生产性能无变化。

### S04：txgameslg 原子汇总写入复用

位置：[perf_model.go:266](../../../txgameslg/server/internal/clustere2e/perf_model.go#L266)、[world_perf_model.go:264](../../../txgameslg/server/internal/clustere2e/world_perf_model.go#L264)。`WritePerfSummary` 与 `WriteWorldPerfSummary` 的 26 行函数体流程相同，仅输入类型不同。

保留两个强类型入口及 `perfRuntime/worldPerfRuntime` 的注入点，在同包共用一个私有泛型写入函数。可放在现有 `perf_model.go`，无需新文件。不得让世界结果转成角色结果，不删除独立 schema，不合并两套业务验收编排器。

必须保留：同目录随机临时文件、JSON 缩进、Encode 错误、Sync、Close、Rename、失败后清理临时文件以及原错误返回。不能简化成直接 `os.WriteFile` 覆盖最终摘要。它们是 artifact 可读性与持久化边界，不是多余防御。

例：世界摘要仍写 `finalWorldUnits`，角色摘要仍写 `finalRoles`；二者共享的是文件写入过程，发布时读者不能看见半份摘要。

实施验证：已有两类摘要格式测试及失败统计回归、server 全量测试；针对共同文件写入过程补必要失败检查，不为两个薄包装复制同样的故障矩阵。抽取只位于 `internal/clustere2e`，不进入普通服务启动及存储热路径。无生产性能收益承诺。

### S05：清理仍生效的旧验收说明

位置：[txclientslg README:175](../../../txclientslg/README.md#L175)、[AGENTS.md:33](../../../txclientslg/AGENTS.md#L33)、[txgameslg README:810](../../../txgameslg/README.md#L810)。客户端手册仍描述故障前 WAL 与新 checkpoint；服务端世界性能拓扑仍称六进程。

当前 `world_perf.go:435` 检查 `DocumentVersion`，`statefulWorldPerfProcessSpecs` 包含 Login 共七进程。修正为当前 Mongo 最终状态、文档版本、槽位 assignment 与关闭证据；区分 Mongo 版本与执行者归属。历史 specs/plans 的旧记录不按当前待办重做，也不机械全库删除 WAL 字样。

此项是附带文档纠偏，不替换代码审计主线，不计入代码瘦身收益。只修改实际失效句子；不改变权限边界或承诺异常退出后的内存变更必不丢失。

## 过滤与暂缓记录

| 机械扫描信号 | 核对后的处置 |
| --- | --- |
| 单实现 Login 认证、授权、编号分配、Agent 选择接口 | 保留：项目定制 SPI；接口实现数量不决定插件扩展价值 |
| `worldBucketLoader`、`accountStorage`、`ResponsePublisher`、压测依赖对象 | 保留：隔离存储/网络、注入受控失败和测试时序；不删除测试桩 |
| Bee/Ant `contextView`、storage capability 包装 | 保留：防止取回宿主 Stop/Cancel/Flush 权限，不能用暴露具体类型换短代码 |
| 同名 PushSenderFactory、服务停止包装 | 保留：按选择的服务延迟创建资源，并负责卸载、关闭和错误处理 |
| 两工程 Actor 适配、两客户端 BON/Frame/Session 类似 | 保留独立示例边界，公共语义分别回归，不建立跨示例源码依赖 |
| 多处反射 typed-nil 检查 | 保留：用于可信配置/API 的 nil 边界；不为十余行重复创建所有层依赖的通用 util |
| RPC 与 MsgQ Sender 的连接缓存/Close 近似 | 暂缓：连接 SPI、错误类型和生命周期独立；抽取将扩大同步状态的共享范围，当前没有净简化证据 |
| `util.NewOnceValue` 可换 `sync.OnceValue` | 不直接替换：标准库在 factory panic 后每次重抛，当前实现后续调用返回零值；也涉及公开工具契约，非等价删除 |
| 角色/世界 `runWindow` 约 50 行流程近似 | 暂缓：失败统计、预热、测量与尾部清理有既定语义；新 callback/测量器抽象须先证明不改变测量成本。本轮不把它列入可删预算 |
| cmd 辅助产物在联合校验和输出时分别 prepare | 保留现阶段取舍：已有计划复用报告明确只合并模板展开，不为继续复用格式化结果新建状态层 |
| GD Inc/Dec 与 setter 重复 | 保留已确认直接实现，前轮候选有性能代价证据，不因换成完整 Ponytail 就重开同一建议 |
| Otter、singleflight、队列、uuid、Vue、TS/Vite 工具链 | 有实际调用和明确职责；本轮无成熟等价替代及迁移验证，不建议删除依赖 |

这些保留结论不是“未来永远不能改”，而是当前没有满足既定边界的删除依据。正常审查发现真实缺陷时另行处理，不用本次瘦身计划掩盖或解决。

## 分批实施顺序与完成条件

以下是待确认方案，不是自动执行授权；每批单独实施、验证并交付，串行推进。

1. **首批 S01：txgen 私有残留**。范围仅两个源文件；用生成无差异及完整生成器测试验证，完成后执行 ponytail-review。
2. **第二批 S03：测试 helper 标准库替代**。只改 tx 测试文件，不删任何现有测试函数或断言。
3. **第三批 S04：服务端低频 artifact 写入**。保留强类型入口、独立 schema 和原子写入，验证错误返回。
4. **第四批 S02：客户端压测文件处理**。先确认默认 I/O 测试覆盖，再抽取局部 helper，不改测量循环。
5. **S05 单独文档批次**，可在任一批后执行；不混入 runtime 改造。

每批完成后提供实际行数、测试结果、未验证项及分工程中文 Angular Commit Message，不执行 Git commit。涉及热点才做等条件 benchmark；没有数据不声称提速。采集代码留在压测工具或 `txbench` 构建，不新增生产采集钩子。

## 本轮验证与限制

以下均在本轮当前代码上运行，候选未实施：

- txgen：`GOCACHE=/private/tmp/tx-go-build go test ./gen -count=1` 通过。
- tx：`GOCACHE=/private/tmp/tx-go-build go test ./tests -run '^(TestRedisAdapter|TestRedisCluster)' -count=1` 通过。
- txgameslg/server：两项摘要格式测试 `TestWritePerfSummaryAtomicallyWritesMachineReadableResult`、`TestWorldPerfModelWritesIndependentSummary` 通过。
- txclientslg：`node --import tsx --test tests/cluster-perf.test.ts tests/cluster-world-perf.test.ts`，6 项通过、0 跳过。直接 tsx CLI 因沙箱禁止其 IPC socket 而启动失败，改用同一 tsx loader 的 Node 测试入口成功，未改测试内容。
- 精确符号检索及本机标准库源码核对用于确认候选及误报；未删除私有函数进行实验，未测试候选代码。
- 未运行全量生成、六工程全量测试、真实集群、race 或性能对照；上述定向通过不能替代后续改动的验收。
- 本轮只有此报告和主实施计划入口变更，执行 diff/链接检查与纯文档后置 review，不为审计文字新增测试。

## 规模口径

S01 和 S03 按静态删除与替代 import 可计 60 行；S02 和 S04 为局部抽取草案估算约 43–48 行。四项合计预计净减 103–108 行，不含未来新增的必要测试；S05 文档、暂缓项、已完成的 RouteKey 及 GD 模板收益不计。实施可能因合理测试和注释增量降低净值，不以达到该数字作为验收条件。

net: -103～108 lines, -0 deps possible.（静态候选估算，未实施、非性能收益。）
