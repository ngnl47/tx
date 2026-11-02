# SessionID 独立发号与运行身份收敛实施计划

> 执行说明：实施时按批使用 `superpowers:executing-plans` 或 `superpowers:subagent-driven-development` 推进，逐项记录验证。本文记录已确认范围及逐批验证；未经用户要求不提交 Git。

**状态：2026-11-02，四批已完成本地验证，未提交。实测与边界见 [验收报告](session-id-allocation-acceptance.md)。**

**目标：** 新 Session 通过 etcd 单次写入取得编号，删除 work slot 发号身份层，保留 Actor 逻辑槽位、排水和归属交接。

**架构：** InstanceID 是每次启动独立生成的不透明身份；SessionID 使用成功写入的 etcd revision。两者不再相互编码。框架装配复用进程级 etcd 客户端，应用只使用原有发号接口。

**技术范围：** Go、既有 etcd v3 客户端、Discovery、CModSessionID、两组应用及客户端的既有本地验收入口，不新增第三方库或旁路服务。

**契约依据：** 本轮确认的发号收敛方向、[工程约定](engineering-conventions.md)、[现行 API](api-reference.md)、[Actor 归属计划](actor-routing-lifecycle-plan.md)。现行设计/API 已同步本轮实现；以下分批记录保留实施范围与验收依据。

## 1. 全局约束

1. 保持 `CModSessionID().AllocateSessionID(ctx) (int64, error)`；仅 Login 声明发号能力，Game、Agent 等不因此增加发号职责。
2. SessionID 必须满足 `0 < id <= 2^53-1`。它不是业务顺序号，不要求连续，也不保证并发调用的响应到达顺序与数值顺序一致。
3. 每次新 Session 独立写入一次，使用该写入成功响应的 revision；不能用 Get、失败 CAS 或多号合并事务的同一个 revision 冒充多个编号。不增加号段、本地预取或读改写计数器。
4. 超时、取消或结果未知返回错误，不返回臆测编号；允许已经写入但未返回的编号形成空号。不增加应用层自动重试或本地降级发号。
5. 保留启动成功后开放发号、关闭发号准入、身份失效禁止发号的边界。单次发号失败不等于 Discovery 已经失去租约。
6. 保留 Actor Identity、SlotID、owner、Generation、Placement、缓存、容量、正常释放、显式交接与异常处置规则。删除 work slot 不删除 `/slots`，不改变邮箱、Procedure、EntityCache、写回或 World 业务生命周期。
7. 保持既有 SessionID、票据及 TCP/WebSocket 字段类型和版本；不修改 UpSeq/Ack 语义，不为此重新生成 cmd 或改 txgen。
8. 只做本地验证，不租赁实例、不跑云压测；既有未提交变更继续保留。数据库/Redis/etcd 清理只覆盖本轮有 ownership 的隔离数据。

## 2. 目标模型与职责

```text
进程启动
  → 生成 AppID + UUID 形式的 InstanceID
  → 申请成员租约，注册 /instances/<InstanceID>
  → 初始化能力和子服务 → Running → 开放 Login 发号

新建 Session
  → Login 调用 CModSessionID().AllocateSessionID(ctx)
  → 核对准入和身份 → etcd 独立 Put → 取得响应 revision
  → 检查数字范围、取消和身份 → Login 按原流程签发票据

正常关闭
  → 关闭准入并取消在途发号 → 已接纳工作结束 → 必要写回及服务收尾
  → 条件释放本进程 Actor 逻辑槽位
  → 注销成员、撤销租约 → 关闭共享 etcd 客户端
```

同 Session 的断线恢复和普通上下行不新增发号请求；新登录仍建立新 Session。成员租约丢失仍按现行规则终止服务，不解释为旧 Actor 已可安全接管。

| 工程/包 | 目标职责 |
| --- | --- |
| `tx/etcd` | 补充返回写入 revision 的通用 Put 原语，不认识 Session、Login 或 Actor |
| `tx/sessionid` | 把单次成功写入转换为符合数字契约的 SessionID；借用写入器，不管理连接或成员租约 |
| `tx/modules/discovery` | 注册不透明 InstanceID、维护成员和 Actor 归属；删除 work slot，不引入 Session 发号方法 |
| `tx/internal/framework` | 持有并装配共享 etcd 客户端，绑定发号准入/取消/身份有效性，按现有顺序关闭 |
| `txgame`、`txgameslg` | 保持 Login 调用与业务流程；调整验收工具、隔离键清理和文档 |
| `txclient`、`txclientslg`、`txgen` | 保持接口与产物；通过现有接入回归确认无需修改 |

### 2.1 编号键与连接所有权

- 以规范化 Discovery 根 `R = TrimSuffix(Prefix, "/instances")` 派生发号键 `R + "-session-ids/issuer"`。例如目录根 `/txgame/local` 对应 `/txgame/local-session-ids/issuer`，位于 `/txgame/local/` 的 Watch/CAS 域之外。
- 同一环境所有 Login 使用同一个派生键；覆盖固定短值，不为每个 Session 创建键，不挂成员租约。路径由现有配置派生，不新增业务 YAML 项。
- revision 是整个 etcd 集群的递增修订号，不是键内计数值。删除/重建发号键不会归零；还原旧快照或重建整个 etcd 数据目录则可能回退。不能把“单键未删除”当成恢复安全证明。
- `etcd.Client.PutRevision(ctx, key, value, leaseID) (int64, error)` 返回成功响应的 revision；原 `Put(...) error` 保持接口，并共用写入实现。
- 新增 `sessionid.NewEtcdAllocator(writer RevisionWriter, key string) (*EtcdAllocator, error)`；`RevisionWriter` 只要求上述 Put 原语。分配器不暴露 Close，不取得连接所有权。
- 框架现有 `discoveryRuntime` 装配点持有一个 `*etcd.Client`；Discovery 和 SessionID 借用同一对象。增加 `discovery.NewRuntimeWithClient(ctx, client, config, appID, descriptor)` 的借用入口，现有 `NewRuntime` 保持自行创建和关闭客户端的便利入口。内部明确拥有/借用，避免两处同时负责关闭。
- 正常关闭和启动失败清理均先停止借用者，再由所有者关闭连接；不新增 CModEtcd、全局连接池或对业务开放的底层客户端入口。

### 2.2 首次切换与灾难恢复边界

- 首次由旧算法切换时采用统一版本切换：停止旧 Login 发号，结束旧会话并使旧票据失效后重新登录。不承诺旧算法与 revision 算法长期混部的编号域互斥，不新增双算法兼容层。
- 后续同一算法下 Login/服务正常重启不重置 revision，仍支持既有滚动发布。InstanceID 每次重启变化，同进程组合子服务共用一个身份。
- 保留有效旧 Session 的 etcd 恢复必须保证恢复后的 revision 高于此前已发号上界；无法证明时，应先使旧 Session/票据失效，不能直接恢复旧快照后继续发号。不在 tx 中自动恢复、回拨或重编号。
- 运维说明包含自动 compaction、容量告警与备份恢复要求。反复覆盖一个键仍产生 MVCC 历史；删除测试键不会回收全部历史版本。不借本次实施修改共享本机 etcd 的 compaction/defrag 配置。

## 3. 审查重点

| 易遗漏边界 | 必须得到的行为 | 验证归属 |
| --- | --- | --- |
| 多 Login、重复写相同值、响应并发乱序 | 各次成功发号唯一，不要求响应有序 | 第一、四批 |
| 取消/网络结果未知、写入完成后身份失效 | 不返回成功编号，不补发业务；空号允许 | 第一、二批 |
| 大量发号与槽位分配并行 | 发号键不进入成员/槽位比较域，不新增目录刷新 | 第一、四批 |
| 初始化失败、重复关闭、组合子服务 | 连接由唯一所有者清理，不提前关闭借用连接 | 第二批 |
| 原编号格式断言及新键清理遗漏 | 只验不透明身份与唯一性；清理精确隔离键 | 第三、四批 |

## 4. 分批实施

### 第一批：建立 etcd 发号实现，暂不切换运行身份

**文件：** 修改 `etcd/client.go`、`etcd/client_integration_test.go`；新增 `sessionid/etcd.go`、`sessionid/etcd_test.go`、`sessionid/etcd_integration_test.go`。暂保留旧 Generator，仅用于让这一批独立编译，不保留为最终可选算法。

- [x] 先定义并运行关键失败测试：单次写入取得编号、同值重复写仍唯一、`0`/负数/超过 `2^53-1` 拒绝、取消和写入错误不返回有效 ID。
- [x] 实现 PutRevision 和 EtcdAllocator；使用注入的最小写入接口测试异常，不靠关闭共享 etcd 制造故障。
- [x] 使用真实 etcd 随机隔离键验证多分配器并发、销毁重建分配器后不复用已返回编号；检查成功编号集合，不断言连续。
- [x] 执行 `TX_ETCD_ENDPOINT=http://127.0.0.1:2379 go test ./etcd ./sessionid`，记录未开启外部测试的 Skip，不能按通过真实依赖计数。

**可验收结果：** 新发号器可独立工作，原运行身份与应用行为尚未切换。

### 第二批：切换装配并删除 work slot

**文件：** `modules/discovery/{identity.go,etcd.go,runtime.go}` 及对应 identity/runtime 测试；`internal/framework/{discovery.go,assembly.go,cmod_discovery.go,cmod_session_id.go,session_id.go}` 及 `discovery_test.go`、`cmod_session_id_test.go`；`tests/discovery_etcd_integration_test.go`。删除旧 `sessionid/generator.go`、`generator_test.go`，把仍使用的 Allocator/数字上限声明收敛到 `sessionid/allocator.go`。

- [x] 先补/调整测试：重启取得不同 InstanceID，多子服务共用身份；服务启动钩子完成前及其失败后不发号；身份丢失和关闭取消在途发号；借用连接不被提前关闭，启动失败清理已取得资源。
- [x] 将框架 etcd 连接所有权提升到现有装配点，Discovery 与新分配器借用；依赖图仍保留 `CModSessionID → CModDiscovery`，不把发号器挂到 Discovery 公共能力上。
- [x] 原子切换内部装配到 EtcdAllocator，同时移除 `RuntimeIdentity.WorkSlot/ModRevision`、`WorkSlotCount`、槽位扫描、耗尽错误、时间戳拼接、时钟回拨和秒内额度等待。
- [x] 运行身份改为 `AppID + UUID`，成员键创建保持不覆盖既有登记的语义；业务不得解析 InstanceID。保留数据版本/目录变更用的 etcd ModRevision，不能按字段名全局删除。
- [x] 统一成员注销/租约撤销，不再根据“曾经 Running”延迟释放发号槽位。保留实例状态机、租约丢失退出和 Actor 条件释放顺序。
- [x] 运行 `TX_ETCD_ENDPOINT=http://127.0.0.1:2379 go test ./modules/discovery ./internal/framework ./tests`，重点复用正常释放、显式交接、Probe 和 World 指定归属测试，确认没有扩大变更边界。

**可验收结果：** 所有服务启动不再申请 work slot；Login 仍通过原接口发号；Actor 归属行为保持不变。

### 第三批：两组应用接入与文档收敛

**文件：** `txgame/server/cmd/internal/clustere2e/login.go`、两组 `server/cmd/internal/clusterkit/dependencies.go` 及相关清理测试；`txgame/server/cmd/internal/clusterkit/instance_test.go`、`txgame/docs/acceptance/README.md`；`txgameslg/README.md`、`txgameslg/docs/login_authorization.md`。同步 tx 的 `README.md`、`AGENTS.md`、`docs/design.md`、`docs/api-reference.md`、相关生命周期/放置文档和各工程实施计划。

- [x] 删除验收工具按三段解析 InstanceID、断言 WorkSlot 不同、输出 WorkSlots 的逻辑；保留双 Login、新建/重连、重启后不复用 SessionID 的业务契约。
- [x] 将派生发号键纳入专用验收环境的 ownership 与精确清理；先覆盖清理只影响本轮命名空间的测试。不得扩大为清理整个 etcd 或删除其他环境发号键。
- [x] 保持两组 Login 的 `CModSessionID` 接线与错误返回不变；核对两组客户端安全整数与票据契约，txgen 无改动需求时不修改。
- [x] 更新现行文档中的“运行身份、成员租约、发号、逻辑槽位”四者关系和简短流程图；文档说明新增派生键的 ACL/备份/压缩要求及首次切换边界。
- [x] 已有日期验收报告保留当时数据，标明旧算法或由新报告接续，不把旧 work slot 证据改写成新算法实测。清理现行说明中的过期限制，不追改所有无关示例字符串。
- [x] 两组 server 运行 `go test ./...`；两组客户端复用 `npm test`、`npm run check`。只记录实际执行的命令和结果。

**可验收结果：** 应用业务代码不感知编号算法；验收工具不依赖 InstanceID 内部结构；文档能区分当前结果与历史证据。

### 第四批：本地正式包与性能验收

**复用入口：** 两组 `cmd/cluster-e2e.sh`，依次运行，避免同时争用端口与专用依赖。txgame 复用双 Login/重启场景；SLG 复用 `server/cmd/internal/clustere2e/login_dual.go` 的并发及重启检查。使用入口支持的专用模式，不新建第二套部署工具。

- [x] 正式包验证：双 Login 并发取得唯一编号，Login 重启后不复用，Login 全停不影响已建立连接的数据通信；新 Session 与原 Session 恢复语义保持不变。
- [x] 核对组合/分离启动都不创建 `/work-slots`，注销成员不留下本轮租约记录；正常 Actor 排水和代次释放、World 停服后新 owner 认领与业务加载保持原行为。
- [x] 在真实 etcd 隔离环境并行发号与创建/释放逻辑槽位，验证前缀隔离及归属正确性。只验证本轮新增交互，不重测全部业务吞吐档位。
- [x] 复测完整框架发号入口与直接 Put 基线：同机同条件的 1/8/32 并发，每档短窗口，记录 QPS、P50/P99、失败数、重复编号和 DB 大小变化。每秒数千次作为可行性参考，不把固定并发或本机峰值写成生产 SLA。
- [x] tx 全量回归、受影响包定向 race；两组正式包验收失败时保留证据，修复属于本批的缺口后重跑对应场景。不将 Skip 或单节点 Docker 结果写成三节点生产容量。
- [x] 实施差异经 `ponytail-review` 检查，补齐结果索引并将本计划对应项标为完成；不提交 Git。

**完成标准：** 两组应用原调用/wire 不变；无 work slot 分配和回收；取消、错误和关闭不返回无效成功；新编号唯一且满足数字范围；Actor 放置/排水无回退；隔离数据与进程清理可核验。

## 5. 已有测量依据与未验证范围

2026-11-02：Intel i9-9880H Mac、32 GiB、Docker 单节点 etcd 3.5.15、一个客户端连接，各档 6 秒独立 Put：

| 并发 | 次/秒 | P99 | 请求错误/档内重复编号 |
| --- | --- | --- | --- |
| 1 | 1,012 | 2.23ms | 0/0 |
| 8 | 4,834 | 3.61ms | 0/0 |
| 32 | 10,602 | 6.45ms | 0/0 |
| 64 | 16,480 | 9.02ms | 0/0 |

脚本及 JSONL 位于各工程仓库之外的证据目录 `tx-artifacts/etcd-sessionid-evaluation-20261102/`；测试键已清理。此数据是实施前的原语基线；本轮完整 CMod 路径已另行测量，见 [验收报告](session-id-allocation-acceptance.md)。两者均不证明生产三节点容量。

资料：[revision 契约](https://etcd.io/docs/v3.6/learning/api_guarantees/)、[性能约束](https://etcd.io/docs/v3.6/op-guide/performance/)、[历史压缩](https://etcd.io/docs/v3.6/op-guide/maintenance/)、[快照恢复与 revision bump](https://etcd.io/docs/v3.6/op-guide/recovery/)。生产故障域、持续高登录率和恢复演练另行验收，本计划不包含新增云端测试。
