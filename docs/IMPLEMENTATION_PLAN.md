# 当前状态与验收计划

本页只维护当前待办和证据索引。首次接入读 [README](../README.md)，模块职责读 [设计文档](design.md)，检查业务用法读 [工程约定](engineering-conventions.md)。已完成施工步骤由 Git 历史追溯，不重复列为待实现能力。

## 已完成：明确接入拒绝与完整持久化补验（2026-11-04）

Agent 新增 `HandshakeRejected` 空控制帧，只拒绝本次接入；`SessionEnded` 保留永久失效。两组客户端不再将网络错误或 Ready 前普通关闭当作应用拒绝，初始化取消与结果未知边界保留。正常上下行及不重投规则不变。

最终两组 Login/Agent/MsgQ 三窗恢复契约均通过，每组 MsgQ 的 20 个受影响连接含 10 个 WebSocket，全部明确拒绝后同角色新登录。SLG 工具先保持 Fusion 消费在线限时等待角色 Mongo 标记，再正常排水并再次读回，120/120 角色及 World (31,32)、版本 2 完整持久化通过。首轮过早停消费者的失败证据保留，未修改生产存储契约。

每窗仍为 180 秒、36,000 固定计划。txgame 最终完成数为 36,000 / 35,861 / 35,980；txgameslg 为 36,000 / 35,889 / 36,000。恢复与持久化通过不能替代 99.99% 完成率；两组 Agent 与 txgame MsgQ 仍未达标，工具按约定失败退出并保留专用数据。最终两轮各 11 次正常服务退出，无本轮进程或 etcd 成员残留。

tx 全量与会话 race、客户端 155/167 项测试及构建、两组工具定向回归与 SLG 持久化等待 race 均通过。详见[完整补验报告](session-rejection-acceptance-20261104.md)。本轮不提交、不跑云测试；下一步可审阅提交，发布交界的完成率缺口单独评审，不缩减分母或追加重投。

## 修复前：接入服务持续流量验收（2026-11-04）

两组本地普通正式包各执行 Login、Agent、MsgQ 三个 180 秒窗口，40 连接×5/s，固定计划分母 36,000，不重投请求、不跑云测试。两组 Login 均 36,000/36,000；Agent 分别 35,881/36,000、35,877/36,000，恢复通过但全窗口未达 99.99%；MsgQ 分别 28,510/36,000、28,490/36,000，均有 10 个 WebSocket 恢复传输错误，未放宽为应用拒绝。六窗对照组均为 18,000/18,000。

本轮修正三处真实运行发现的工具问题：txclient 不额外发送 Role_Enter、仅 MsgQ 窗口读取 Agent 日志、SLG World 完成通知按 BON 契约处理省略的零值网格。客户端全量 152/164 项测试及构建、两组工具定向普通及 race 回归通过；生产协议和恢复语义未变。

最终两轮各 10 次服务正常退出，无验收进程残留。SLG 前两窗 80 个角色最终标记已从 Mongo 核对，最后成功 World 坐标 (21,22)、版本 2 已保存；第三窗缺少最终标记，完整持久化仍失败。失败专用数据和所有运行证据保留，不宣称清库通过。见[完整报告](session-rollout-acceptance-20261104.md)。

以上为修复前记录；当前接入拒绝及持久化补验结果见上节。发布空窗与全计划 99.99% 的差距继续保留，不能靠改统计口径消除。

## 已完成：修复立即重连与旧接入关闭重叠（2026-11-04）

此前正式包收尾验收的七个 180 秒窗口均达到 99.99%，但 SLG 有限套件两次在立即重连时失败。本轮先定向复现，再修复共享流按 SessionID 提前拒绝新 ConnectionID 的问题；流目录改按完整接入身份登记。同步补齐旧 Terminate 与 Restore 的串行校验、旧控制句柄清理和迟到初始化保护，保留 MsgQ 统一接入协调，不改变 Login、协议或客户端重试策略。

两组有限验收补齐初始接入证据采集确认，再执行顶号，修复初始 SID 先被删除造成的工具漏采；普通断线重连仍立即执行。最终 tx 全量与定向 race、两组装配和工具回归、客户端 134/144 项测试及构建通过。两组普通正式包各 10 阶段、12 次正常退出通过，两组有限会话套件通过。最终四轮 52 份退出记录为 50 次正常退出与 2 次预期故障注入，无缺失或意外退出；专用存储与验收进程无残留。

运行时和工具差异分别完成独立 review，无待修项。本轮不提交、不跑云测试、不重复七个流量窗口，不把旧构建的流量数据改记为修复构建结果。修复与最终补验见[报告](reconnect-window-acceptance-20261104.md)和[机器记录](validation/reconnect-window-20261104.json)。下一步可审阅差异并提交，无需再扩大功能或补注释范围。

修复前的两次运行时失败及本轮工具漏采失败保留原始日志和结果，历史数据不覆盖。修复前流量与失败边界见[原报告](final-package-acceptance-20261104.md)及[原机器记录](validation/final-package-20261104.json)；原始证据分别位于 `tx-artifacts/final-package-20261104/` 和 `tx-artifacts/reconnect-fix-20261104/`。

## 已完成：Actor/RPC 职责导航与公开契约注释（2026-11-04）

补齐 Actor/RPC 包级说明及关键文件、类型、方法导航。纠正 Result 的旧上行接纳确认口径，明确准入包装只登记至接收回调返回；补充 CallFuture 创建 Context 与 Await 等待 Context 的关系、接收 SPI 的并发/顺序和注册要求，以及共享流分阶段取消与失败影响范围。保留既有 Controller、WaitReady 和类型卸载策略契约，不新增 API 或包装。

本批八个 Go 文件仅改注释；连同此前 Agent/MsgQ，工作区十四个 Go 文件与 HEAD 的非注释 token 完全一致。包级和公开入口 GoDoc、职责树路径与链接、gofmt、差异检查通过。当前 go doc 无法通过公开别名展开 actor.Future.Await，已核对实际定义处的方法展示，公开 Future 注释亦保留完整等待边界。按 ponytail-review 对本批差异作有限复核，无待修问题。未重复运行功能、集群或性能测试，本轮不提交。

下一步可汇总两批文档注释变更提交；若继续扩展样例，先只读检查 Procedure/EntityCache 的公开契约缺口，不自动扩大本批范围。

## 已完成：文档职责与 Agent/MsgQ 导航样例（2026-11-04）

统一 README、design、包级 GoDoc、公开契约和私有流程注释的维护边界，规则集中在审计标准，工程约定引用。Agent/MsgQ 的设计章节展开到文件、关键类型和方法，源码注释补齐借用与关闭权限、接纳与发送完成、并发调用及取消/错误语义；不穷举私有方法，不另建 API 手册。

本轮仅文档和注释。六个 Go 文件对照 HEAD 的非注释 token 完全一致；包与关键接口 GoDoc 展示、导航源码路径、gofmt 和差异检查通过。按 ponytail-review 做有限差异复核，纠正了接入 Context 取消不等于逻辑会话销毁的旧注释，并保持唯一包级说明。没有行为改动，未重复运行功能、集群或性能测试，不提交。下一步可按本样例检查 Actor/RPC 等公开入口的文档缺口，先列范围再补齐，不扩展本轮修改。

## 已完成：MailboxStream 职责整理与关键流程注释（2026-11-04）

沿用已确认的单发送协程、共享流复用 Session、接入/关闭确认及异常收尾边界，将 `MailboxStream` 的局部协作状态收敛到包内每条流一个的私有对象。主入口展示启动、分派和收尾；接入、下行转发与解除接入按职责组织，不新增公共 API、协议或逐消息包装。

按 L0～L4 的实际职责选择少量跨模块、跨生命周期的长流程入口，补充中文流程简图和必要的完成/取消约束；包含 Agent `serve`。简单契约、转发及注册适配不机械补图。通用规则写入审计标准，工程约定仅引用。

Agent/MsgQ、事务、缓存、Actor 宿主、框架与 Redis 适配回归，以及框架契约、关键会话 race、vet 均通过。六组 AB/BA 对照中，单 Session 下行、64 Session 下行、OPEN/CLOSE 的分配次数分别保持 46、37、124 次/操作；耗时未见稳定退化，不作提速结论。当前私有对象实现已独立测量，不以此前临时拆分替代验证。

独立差异 review 未发现实现回归，修正 serve 注释中的本地传输关闭与远端确认先后关系；GoDoc 展示、格式与差异检查通过。具体范围和数据见[本轮验收](local-acceptance-matrix.md#2026-11-04-mailboxstream-职责整理与关键流程注释)。未改验收工具、生成产物或示例工程，未跑真实集群或云压测，本轮不提交。下一步可审阅整理后的入口与流程注释，再汇总提交；不继续机械拆分其他长方法。

## 已完成：Agent 单连接生命周期封装（2026-11-04）

`serve` 收敛为验票、离线退出分支、远端接入与连接运行的编排入口。沿用 `activeSession` 持有单连接的上下文、邮箱流、写入器及完成信号；`session_connection.go` 集中 Ready、收发、主动退出和统一收尾，`session_registry.go` 保留本地初始化登记及精确连接控制。合并初始化终止补偿的重复分支，不新增公共 API 或通用状态机。

保持原有接入与关闭契约：Ready 等待远端接入，初始化期间可终止；普通断线等待解除绑定确认并保留邮箱；Logout 等待逻辑终止与在途下行写完。补充普通断线等待远端 Close、传播关闭错误且不调用 Terminate 的回归，原实现基线及整理后的回归均通过。

Agent/MsgQ 与框架契约测试、关键会话路径 race、两组应用 Agent/MsgQ 启动装配回归及 Agent vet 通过；独立 ponytail-review 无待修项。具体范围见[本轮验收](local-acceptance-matrix.md#2026-11-04-agent-单连接生命周期封装)。本轮仅整理 tx 私有实现和文档，不改协议、生成产物或示例业务，不跑真实集群或云测试，不作性能收益结论，不提交。下一步可汇总现有未提交变更，不自动扩大重构范围。

## 已完成：独占接入与 MailboxStream 协调（2026-11-03）

当前实施以[独占接入计划](exclusive-session-plan.md)为准：Login 恢复独立认证、授权与签票；签票不占位，Agent 验票后才取得角色 owner MsgQ。顶号由 owner 经旧 MailboxStream 的 Revoke/Detached 协调，远端 Init 与撤销绑定完成后才返回 Ready。在线、离线退出均由 Agent 验票并精确终止，不经过 Login，也不在 Agent 间广播。

两组应用仍统一选择独占策略，非强制占用拒绝、本人新登录及经授权的强制借号允许替换。TicketOrder 与策略随票据签名，接纳后淘汰较早票据；逻辑退出保留水位。Ack、Renew、下行数据不增加逐项成功回执，不扩展通用四语义 API。

后续文档核对已补齐 Ready 后的业务初始化链路：接入完成与角色业务登录完成分开，txgame 与 txgameslg 分别按其现有协议说明建角/进入角色及下行响应；重连不自动重发初始化请求。各工程 README 和登录设计同步维护，修正 Login 会话控制的旧口径；此项仅核对代码与文档、路径和差异，不新增运行行为或重复运行集群验收。

已完成框架与两组应用全包回归、两客户端测试/build、关键 race、真实 Redis CAS、有限跨 Agent/MsgQ 验收及独立差异 review。补修后两组正式包生命周期各 10 阶段、12 次正常停止通过，接入与关闭确认、owner 释放和失败保留重试均已覆盖。证据填写在[验收矩阵](local-acceptance-matrix.md#2026-11-03-独占接入与-mailboxstream-协调)。本轮结果见[流内协调验收](exclusive-session-acceptance-20261103.md)，旧预留模型日志不作为当前通过证据。本轮不提交、不跑云测试。

## 已完成：SessionClient 权限与创建文档收敛（2026-11-03）

`SessionClientRouter.Acquire` 返回的 `SessionClient` 仅含会话操作；拨号器和连接缓存使用 `SessionConnection` 持有关闭权，路由器继续统一关闭连接。本地客户端移除无操作 Close，不改会话、连接复用或排水逻辑。txgen 写入参考统一为业务 `db.Create` 委托 `txruntime.Create`，`storage.Create` 保留为高级独立存储入口。

接口回归先复现旧 SessionClient 暴露 Close，修改后通过。针对性验证：

- tx：Agent、MsgQ、连接缓存及会话回归通过；Bee/Ant Agent/MsgQ 注册适配参与编译。
- `go test -race ./modules/msgq ./internal/instanceconn ./tests -run 'Test(SessionClient|FixedSessionClient|Cache|GRPCSharedStreamClose)' -count=1` 通过。
- txgame、txgameslg：各自 `go test ./services/agent/... ./services/msgq/... ./internal/app -count=1` 通过。

首次含网络测试受沙箱本机端口监听限制，允许本机临时端口后复跑通过。独立 ponytail-review 无待修项，差异检查通过。不启动真实集群或外部依赖专项，不跑云测试；生成产物和示例代码未修改，本轮不提交。

## 已完成：手写 Go 结构体组织整理（2026-11-03）

依据[结构体字段组织约定](architecture_audit_standard.md#结构体字段组织)，已整理 tx、txgen、txgame、txgameslg 的手写 Go 结构体。固定依赖、可变状态、原子操作、一次性关闭及单协程状态按实际职责分组；锁紧邻受保护字段，必要位置补充中文说明。简单数据结构保留原布局，生成产物和第三方代码未修改。

四仓共 143 个 Go 文件有组织调整，其中 tx 76、txgen 5、txgame 25、txgameslg 37。对照 HEAD 的语法树核验确认方法逻辑、字段集合、类型和标签不变；必要字段移动已检查位置字面量及布局/序列化依赖，不修改协议字段顺序。不为排版增加包装、接口或测试。

tx、txgen 与两组 server/gen 共六个 module 的完整本地测试，以及 tx vet 均通过；gofmt、差异检查及生成产物排除检查通过。独立 ponytail-review 已完成，未发现本轮引入的问题；证据见[本轮验收](local-acceptance-matrix.md#2026-11-03-手写结构体组织整理)。本轮不提交，不运行真实集群、外部依赖专项、云压测或性能基准；字段视觉分组不作为内存/性能优化结论。

## 已完成：L0～L4 审查后续收敛（2026-11-03）

本轮模块边界收敛保留以下现行结果，前轮验收工具变更独立维护：

1. MsgQ MailboxStream/PublishStream 在锁外共享建流；请求各自取消等待，连接所有者可以取消建流并回收迟到结果。
2. 为 Service Flush、CMod Drain 与 Service Stop 补齐配置期限，正常关闭和初始化回滚均区分尚未完成与已结束但失败；同步 Agent、Room、World、Sched 的返回契约，不以超时替代实际退出。
3. 统一实例连接缓存的退役回收；只依据有效成员目录，不在消息热路径扫描，不因 Watch 暂时失联误关连接。
4. 框架统一直接持久化的 Procedure 拒绝错误；两组应用的 db.Save 只转发，保留 SaveChange 必要保护。

这些收敛已完成。后置审查补齐初始化回滚的最终错误保留及状态；全量回归进一步发现 Agent 未完成关闭缺少标记，同步检查实际服务适配后修复。槽位刷新测试的真实计时假设改用虚拟时间，不修改生产查询周期。tx 与两组 server 完整本地回归、关键取消与关闭路径 race、tx vet 均通过，最终差异复核无待修项；证据见[本轮验收](local-acceptance-matrix.md#2026-11-03-模块边界后续修复)。未运行真实集群或云压测，未单独启用外部存储专项，也不把引用回收外推为已量化性能收益。本轮未提交，原有验收工具变更保留。

## 已完成：验收客户端失败退出清理（2026-11-03）

会话生命周期收尾核对发现实际工具缺口：SLG 的 npm 包装进程取消后，Node 探针仍存活；两组 `Terminate` 只发送信号，不等待客户端实际退出。本轮仅修本地验收工具，保留此前通过的正常路径证据，不修改 tx 运行时，不提交。

1. 用真实本地 Node 夹具先复现提前返回、取消残留及忽略 SIGTERM；不启动集群或外部依赖。
2. 两组直启 Node，复用 `exec.CommandContext` 的取消与有限强制终止，独立等待实际退出；正常结果仍保留原始退出错误，清理不消费结果。
3. 调用方汇总清理错误，移除重复等待；核对 txclient 现有多进程 CLI 的父取消与 worker 回收，不扩展通用进程组或生产恢复机制。
4. 完成命令工具本地回归、定向 race 和差异审查，记录实际范围；不重复整轮集群或云压测。

两组真实 Node 夹具均先失败后通过；非协作客户端在既有 2 秒等待后被回收，原始非零退出证据保留。txgame 命令工具回归与 vet、SLG 六个受影响包及工具 vet、两组定向三轮 race 均通过；txclient 新增父 SIGTERM 回归后全量 119 项及构建通过。独立差异审查无待修项，见[补验记录](session-service-lifecycle-acceptance-20261103.md#验收客户端失败退出补修)。本批未重新运行真实集群，不把此前正常验收改记为当前工具版本的复跑。

## 已完成：正式包会话服务生命周期验收（2026-11-03）

两组应用沿用现有打包、集群编排及客户端入口，补齐不带 `txbench` 的有限请求验收。先运行 txgame，再运行 txgameslg；程序从源码目录外启动，全部替换复用同一程序包。本轮不跑云测试、不提交，不增加框架恢复机制或生产探针接口。

```text
普通程序包 → 隔离集群 → 保持 TCP / WebSocket 会话
  → Login 逐台停止、替换：存活节点签票，旧连接继续工作
  → Agent 在线停止、替换：原 Session 恢复、新 ConnectionID、新登录
  → MsgQ 正常停止：旧 Session 恢复明确失败 → 同 Role 新登录
  → 全部正常排水退出 → 核对身份、目录、退出记录与专用数据清理
```

- Login：跨实例与重启后的 SessionID 不重复，替换期间已建立的连接不受影响。
- Agent：由实际 Stop 关闭在线 TCP/WS，恢复复用原 AgentClient 和 Session；原 MsgQ owner 不变，下行接续，上行不重放。另验证新登录可进入替换实例。
- MsgQ：明确停止的旧 owner，核对条件释放；旧 Session 恢复必须是明确握手拒绝，连接失败不能冒充该证明。同 Role 重新登录使用新 Session，存活 MsgQ 上的会话继续成功。
- SLG 保留 Role/World 进程、业务实例与槽位归属；本项只验证接入组件生命周期，不改为有状态业务在线发布。
- 有限步骤按契约逐项断言，记录恢复和退出耗时，不用小样本推算 99.99% 可用率。失败保留证据，清理遵守既有 ownership 边界。

两组普通包最终各 10 个阶段、12 次正常退出通过，探针各记录 11 个新 SessionID 且无重复；SLG 两个业务槽位在各阶段保持原 owner/generation。补齐终阶段退出与日志观察的工具竞态后已分别复跑；两组 Go 定向回归、客户端 118/129 项测试及构建均通过。独立核对专用数据与进程无残留，详见[本轮报告](session-service-lifecycle-acceptance-20261103.md)和[机器记录](validation/session-service-lifecycle-20261103.json)。当前批准的实施及本地验收已完成，可汇总提交，不自动扩大运行时改造。

## 已完成：当前构建本地正式包验收（2026-11-03）

上一批边界修复已提交。本轮固定 tx `84f095e`、txgame `3261fb8`、txgameslg `8753304`，复用当前正式打包与客户端入口，顺序运行：

1. txgame：50 连接、每连接 5 次/s，180 秒持续流量覆盖在线 A→B→A 配置发布与回滚。
2. SLG Role：4 并发，A→B→A 各 180 秒；版本之间全部服务停服后重启。
3. SLG World：4 并发、WorldID `490001,490002`，A→B→A 各 180 秒；版本之间全部服务停服后重启。
4. 复用 `TestOwnedBucketsMongoRoundTrip` 连接本机真实 Mongo，补充新写入前恢复旧坐标的断言；此项是运行时重建验证，不冒充正式包独立进程之间的只读恢复断言。

txgame 整段 180 秒、SLG 各 180 秒测量窗按 99.99% 完成率核验，txgame 的发布子窗口另行报告；SLG 停服间隔单列，不计为在线滚动成功。核对正式包脱离源码启动、程序摘要不变、配置版本、进程实际退出、槽位代次/owner 及入口已有的持久化回读。txgame 入口不等待正常 Fusion 600 秒最终 Mongo 延迟，不能据此声称其最终 Mongo 已同步。

复用与当前 schema 一致的既有完整 A/B 配置快照，正式加载器和配置打包入口已验证。完整版本与日志保存在 `/Users/a47/workspace/tx-artifacts/runtime-boundary-acceptance-20261103`（工作区目录，不纳入库）。成功按现有 ownership 保护清理专用数据，失败保留证据。本轮只做本地验收和记录，不跑云测试、不提交。

实测完成：txgame 44,997/45,000，99.993333%；SLG Role 三轮共 1,085,930 次、World 三轮共 3,295,181 次，均零失败。槽位稳定阶段 1→3→5、新 owner、SLG 最终 Mongo 回读、World 运行时存档重载及 47 份进程退出记录均已核对，专用数据无残留。保留 txgame 3 次交接期失败及独立业务探针的旧绑定拒绝日志，不将总体达标解释为每个短发布子窗达标或零错误日志。详见[本轮报告](runtime-boundary-rollout-20261103.md)与[验收矩阵](local-acceptance-matrix.md#2026-11-03-当前构建正式包验收)。当前批准的本地验收已完成，下一步可汇总提交记录，不自动扩大重构或云端压测。

## 已完成：剩余三项 P1 边界修复（2026-11-02）

1. 内建存储 CMod 的后台工作归属当前组件；启动失败回滚先等待工作实际退出，再关闭其依赖。
2. Agent 共享 RPC 流发送的排队与实际写入均响应操作取消；等待取消不影响其他项，已进入 gRPC 的阻塞发送必要时终止共享流，不重投未知结果，不增加 Tell 确认。
3. 根 Procedure 的回滚回调统一在恢复、解绑与解锁后执行；子事务保留 savepoint 语义，明确上下文与锁边界。

三项已先复现再修复，后置审查另补根回滚回调 panic 不二次解绑后续事务记录器。tx 与两组 server 的最终全量本地测试、受影响六包 race、补充事务及 RPC race、vet 均通过；独立差异 review 无待修项。128 字节本地 gRPC 发送基准由约 1.26～1.39µs 增至 1.48～1.60µs，分配保持 19 allocs/op；这是取消保护的可测成本，不推算真实集群吞吐。证据与限制见[本批验收](local-acceptance-matrix.md#2026-11-02-剩余三项-p1-边界)。本轮不提交，不跑云测试或整轮真实集群；可整理本批提交，不自动扩大实现。

## 已完成：业务本地 DelayQueue 封装修复（2026-11-02）

`util.DelayQueue` 保留为业务拥有的本地有序定时器，与 EventBus 同属可选工具。evo 战场已有实际使用依据，撤回仅因当前示例无调用而删除的建议；不接入 CMod，不替代 Fusion 或 sched。

已先复现周期间隔失效及大量到期任务下回调新增任务死锁，再收敛为单循环锁外串行执行，保留首次尽快执行并修复周期 TaskID 取消、停止等待及释放边界。完成顺序、周期、自取消、积压、批次内取消和并发停止回归；tx 全量本地测试、util 二十轮 race 与 vet 通过，独立差异 review 无待修项，见[本批证据](local-acceptance-matrix.md#2026-11-02-业务本地-delayqueue)。本轮不提交、不跑云测试或真实集群，不修改 evo 参考工程。

此前剩余审查的三个 P1 已按页首独立批次完成，不并入本次工具修改。

## 已完成：能力收窄与共享冷加载取消（2026-11-02）

1. EntityCache 业务视图移除创建/删除预留，保留 Runtime 对 WriteBack 的内部 SPI；Placement 工厂仅取得准入能力，不取得共享闸门管理权。
2. 先复现同键冷加载中首请求取消影响其他调用、后续等待者不能独立取消的问题；共享加载由宿主托管，等待取消不冒充加载退出，不扩大到创建查重路径。
3. 同步上下文、生命周期契约与示例文档残留，完成针对性并发回归、两组应用接入回归及热命中微基准对照，再做差异 review。

本轮不提交，不跑云测试或整轮集群。Bee/Ant 的独立门面政策、存储原生入口和既有事务/写回语义不变。

上述步骤已完成：两处越权入口及冷加载两类取消问题均先复现再修复。共享加载使用独立十秒期限并由宿主 Tasks 跟踪实际退出，屏蔽请求事务/实体使用期；低层独立装配须先 Start，正式应用原有接线不变。tx 与两组 server 全量回归、关键四包 race、tx vet 通过，热命中保持零分配，未见微基准回退；独立差异 review 无待修项。证据见[本轮验收](local-acceptance-matrix.md#2026-11-02-能力收窄与共享冷加载取消)。下一步继续 L0～L4 剩余边界审查，先给有实际调用依据的问题清单，不自动扩展重构。

## 已完成：RPC 调用命名统一（2026-11-02）

1. Agent 的单向直连能力统一为 `actor.Teller.Tell`，完整 `Messenger` 复用同一最小接口；两组应用接入同步，不添加第五种业务调用模式。
2. gRPC 发送侧 `SendStream` 改为 `TellStream`；原本等待接收回执的 `Send` / `Connection.Deliver` 改为 `TellAccepted`。保持线协议、接收端分派、长流复用、顺序和错误传播不变，不增加逐项确认或业务重投。
3. 生成业务方法保留业务名称，由现有 GoDoc 说明 Call/Tell。邮箱入队、Procedure Submit、可靠任务和 MsgQ 发布不属于另一种 RPC 模式，不机械改名。
4. 同步契约、示例和测试，验证单向发送不等待远端接纳，完成 tx 与两组应用本地回归和差异 review。本轮不提交、不跑云测试或整轮集群。

上述步骤已完成。单向流测试阻塞接收端首条消息的接纳，确认发送端仍完成三条 TellStream，解除阻塞后顺序不变且没有逐项回执。tx 与两组 server 全量本地回归、tx 全量 vet 和受影响六包 race 通过；独立差异 review 无待修项。txgen 及生成业务方法已核对，现有 GoDoc 已说明 Call/Tell，不需要改生成器或客户端。证据见[本轮验收](local-acceptance-matrix.md#2026-11-02-rpc-调用命名统一)。下一步可汇总当前未提交变更，再继续模块边界审查。

## 已完成：能力契约与懒装配收敛（2026-11-02）

按最新审计契约及用户确认实施，本轮不提交，保留已有文档变更：

1. 明确 Bee/Ant 门面政策与 L4 注册适配允许独立实现，不抽公共政策层，不改变底层资源所有权。
2. 先复现标准宿主首次共享 Create 的缓存绑定缺口，再复用既有 EntityCache 懒初始化入口；保留 Procedure、独占存储和低层 SPI 的显式装配边界。
3. Agent 直链接入统一依赖单向投递能力（现名 `Teller`），移除隐藏类型断言并同步两组应用；保留正常 Actor Call 及底层同步直连协议。
4. Actor 装配直接复用 Discovery 已持有的 SlotOwnership，不新增归属逻辑或接口。
5. 完成针对性回归、受影响并发验证和两组应用接入回归，再做差异 review。测试记录实际通过与跳过范围，不跑云测试或整轮集群，不修改生成器和客户端协议。

上述步骤已完成。正式 Ant Provider 首次 Create 的提交、回滚缺口已先复现再修复；仅初始化受管共享对象所需的缓存，低层显式装配保持不变。tx 与两组 server 全量本地测试、tx vet、受影响六包 race 和创建定向 race 通过；差异 review 发现并修正直连路由文档的旧轮询描述，无待修代码项。证据和环境限制见[本批验收](local-acceptance-matrix.md#2026-11-02-能力契约与懒装配收敛)。下一步可汇总未提交变更；不自动扩大到云压测或新的模块拆分。

## 已完成：收敛既有 vet 告警（2026-11-02）

保持上一批 Reader/元数据工作区变更，本轮不提交：

1. 复现全量 `go vet ./...`，区分 MsgQ 生产路径和测试夹具问题。
2. 发布队列不复制 protobuf 内部消息状态，保留路由字段及载荷独立性、异步准入和关闭语义。
3. 测试夹具改用指针接收器，槽位字面量使用具名字段；不新增公共 API 或配置。
4. 完成全量 vet、本地回归、相关并发/race 验证和差异 review，记录实际范围。不跑云测试或整轮集群。

5 条告警已消除，`go vet ./...` 和 tx 全量本地回归通过。MsgQ 整包 race、Sender 定向回归及真实本机 etcd/NATS 下的受影响槽位测试通过，独立差异 review 无待修项。发布队列的寻址字段和载荷隔离保留；128 字节本地准入微基准分配仍为 256 B/op、2 allocs/op，时延观测及限制见[本批验收](local-acceptance-matrix.md#2026-11-02-vet-告警收敛)。本批新增修改仅在 tx，上一轮四仓变更保留，未提交。

## 已完成：Reader 与类型元数据装配收敛（2026-11-02）

按已确认的审查结果实施，本轮不提交：

1. 删除生成包全局 Reader 和默认回退，Find/Snapshot 的默认资源统一从 ctx Provider 取得；保留显式 Load 和存储模式 Reader。
2. GeneratedData 只登记元数据，删除默认 Reader 绑定、空回调与启动/关闭清理链；通用 TypeRegistry 从 Fusion 移到 meta，不保留兼容别名。
3. 同步两组应用、生成产物及夹具。缓存/Procedure 用例绑定测试 Provider，不用显式 Load 绕过被测语义。
4. 补齐 L0～L4 的职责映射和真实依赖图；不按目录层级搬包，不新增 CMod 或能力包装。
5. 验证缺失 Provider、作用域隔离、显式 Load、元数据独立装配，完成框架/生成器/两组应用本地回归及差异 review。不跑云测试或整轮集群。

实施分工：tx 维护元数据契约与装配；txgen 维护查询输出与生成契约；应用只迁移接入和测试，业务存取语义不变。

上述步骤已完成。额外复现并修复删除回退后的热缓存准入缺口：`Descriptor.CheckAccess` 返回 error，缺失 Provider 的冷热查询、缓存管理及事务写入沿现有入口失败，不增加第二套检查包装；能力未声明的配置错误仍沿用既有失败契约。tx、txgen 与两组 server/gen 全量本地回归通过，关键错误路径及 SLG 创建/回滚/排水 race 通过，显式 Fusion Load 已用本机 Redis/Mongo 完成往返验证。四仓差异 review 无待修项，客户端无变更，未提交。证据与未验证范围见[本批验收](local-acceptance-matrix.md#2026-11-02-reader-与元数据装配收敛)。下一步可汇总本批提交；不自动重复云压测或整轮集群。

## 已完成：模块边界修复（2026-11-02）

本批已完成以下修复，不扩大到默认 Reader 双入口或整体目录重构，本轮不提交：

1. Agent 监听非预期退出报告给宿主；TCP/WebSocket 连接工作统一登记，取消后等待实际退出，关闭超时保留依赖供重试。先验证异常传播、阻塞初始化和正常关闭。
2. 收窄 EntityCache、Redis 命令入口和 gRPC 注册回调的能力视图，生命周期仍由宿主拥有；共享缓存的存储依赖检查不受冷热影响，覆盖查询及事务写入入口。
3. Procedure 排在已选存储能力之后启动，补齐 Redis 的弱顺序依赖，不强制启用新后端。
4. 同步生成器及两组应用的必要产物和文档，完成定向 race、框架/生成器/应用本地回归，再做差异 review。不跑云测试或整轮集群；既有无关 vet 告警单列。

tx、txgen、两组应用 server/gen 本地全量回归及相关包 race 通过，独立差异 review 未发现新的必修项。补齐启动回滚关闭监听器、删除授权失败释放写回预留的异常分支；缓存能力检查为纯内存调用，实测 61～81 ns/op、0 分配。冷热存储声明检查、接口调整、基准与未验证范围见[本批验收](local-acceptance-matrix.md#2026-11-02-模块边界修复回归)。后续 Reader 双入口和 L0～L4 文档映射已按页首独立批次完成。

## 已完成：MsgQ 并发回归与模块边界审查（2026-11-02）

1. **MsgQ 回归已完成。** 先复现 `TestSessionClientRouterConcurrentFreshSessionsShareRoleOwner` 的 race，再修正夹具的并发记录与每次拨号对象身份。并发冷拨号允许创建候选连接，最终复用同一连接并关闭其余候选；不再错误要求只拨号一次，未修改生产连接缓存。定向十轮 race、`./tests` 全包 race、MsgQ/连接缓存包 race 及 tx 全包普通测试通过，见 [夹具补验](actor-placement-acceptance.md#2026-11-02-msgq-并发夹具补验)。未额外启用外部依赖，不重复整轮集群或云测试。
2. **模块边界只读审查已完成。** 已核对 CMod 与 L0～L4 的实际依赖、装配和资源所有权，并结合两组应用与生成器检查。Agent 监听异常传播、连接工作退出等待、共享资源生命周期视图、缓存冷热一致的能力声明检查，以及 Procedure 对已选 Redis 的启动顺序，已按页首后续批准的范围修复。全局默认 Reader 双入口与分层文档映射也已按独立批次收敛。

该批额外 `go vet ./...` 曾未通过：MsgQ protobuf 消息值复制、测试夹具含锁值接收器及槽位测试无字段名结构字面量产生告警，日志为 `/private/tmp/tx-msgq-vet-20261102.log`。当时未修改这些文件；告警现已在页首后续批准的独立批次修复，全量 vet 通过。

本轮不提交。上一轮 Actor、SessionID 和压缩恢复改造已提交；历史报告中的“未提交”保留当时的验证语境。

## 已完成补验：etcd 历史压缩后的目录恢复（2026-11-02）

etcd 历史维护说明已补入 [使用文档](api-reference.md#etcd-使用与维护)。本次只补真实压缩下的框架接入证据，不把运维维护当作业务功能，不修改共享 etcd 配置，不跑云测试、不提交。

1. 使用本机已有镜像创建独立 etcd；成员和槽位 Watch 分别从已压缩 revision 启动，核对真实 `ErrCompacted`。
2. 验证目录失效、两份新快照恢复及后续增量继续生效，不增加生产测试钩子或业务重投。
3. 验证同一发号键在压缩后仍取得更大的合法 SessionID；复用现有包级和 race 入口，记录实际结果并清理本轮环境。

本地 etcd 3.5.15 独立容器验收通过：两类真实压缩场景及既有 Watch 轮次屏障完成三轮 race；etcd、SessionID、Discovery 三包启用真实依赖后回归通过。没有修改生产代码，测试键和独立容器已清理。本项不验证自动压缩的长期稳态容量，也不重复两组应用整轮集群验收，见 [SessionID 验收报告](session-id-allocation-acceptance.md#etcd-历史压缩恢复补验2026-11-02)。当前已批准的实现和本地补验已完成，下一步可汇总未提交变更；不自动扩大到云端容量或异常接管。

## 当前已完成：SessionID 独立发号与运行身份收敛（2026-11-02）

用户已确认采用 etcd 单次写入 revision 发号、删除数值 work slot 的方向，具体见 [专项实施计划](session-id-allocation-plan.md)。实现及本地验收已完成，证据与未验证范围见 [验收报告](session-id-allocation-acceptance.md)：

1. 补充通用 revision 写入原语与 SessionID 分配器，保持正数及 JavaScript 安全整数契约。
2. 复用进程级 etcd 客户端，切换 CModSessionID；InstanceID 改为每次启动独立唯一身份，移除 work slot 分配、拼接及延迟复用。
3. 同步两组应用验收工具、隔离发号键清理和文档，保持 Login API、票据和客户端 wire 不变。
4. 本地正式包验证双 Login、重启、原 Session 恢复及 Actor 归属不回退，再测完整发号入口开销。不跑云测试、不提交。

Actor 逻辑槽位、Placement、正常排水后条件释放与显式交接继续保留。首次新旧发号算法切换采用统一版本和旧会话失效边界，不引入混部兼容或自动恢复编号机制。现行 API/设计文档已同步；完成情况和实测证据在专项计划中归档。

## 已完成主线：Actor 寻址、归属与生命周期收敛（2026-10-31）

目标与分批约束统一见 [专项实施计划](actor-routing-lifecycle-plan.md)。已完成固定版本 Orleans/Akka 源码对比、目标确认及三批实现：

1. **第一批：路由修正闭环已完成。** 保留缓存命中直达，增加旧归属条件失效、按 SlotID 共享刷新和单向投递异常控制反馈；收敛传输与归属职责，不重投结果未知的请求。
2. **第二批：正常关闭释放已完成（2026-11-02）。** 排水、必要持久化与 Service.Stop 实际完成后，条件释放仍属于本实例的槽位，允许普通服务池后续重新分配。保留跨代版本、显式交接及 World 指定部署；不把失联自动视为可接管。
3. **第三批：目录增量更新已完成（2026-11-02）。** 对照 1,024 / 8,192 / 65,536 槽位及每批 1 / 32 / 256 变化，确认全量重建成本后改为私有增量发布；保留初始/重连完整快照、版本保护与旧轮次拒绝。tx 全量、核心七包 race 和真实 etcd 槽位定向 race 通过；补齐此前未开启的 Actor etcd 夹具成员登记。见 [第三批证据](actor-placement-acceptance.md#2026-11-02-目录更新测量与增量发布)。

第一批 tx 全量、两组示例接入回归及七包 race 通过，见 [第一批证据](actor-placement-acceptance.md#2026-10-31-条件路由修正与共享冷刷新)。第二批 tx 与两组 server 全量测试、核心包 race、真实 etcd 回归和两组正式包有限请求验收通过；全量 `tests -race` 暴露既有 MsgQ 拨号测试夹具竞争，保留失败记录，不标为全套 race 通过。正常释放及 World 重新认领见 [第二批证据](actor-placement-acceptance.md#2026-11-02-正常关闭释放与重新分配)。

**三批后的本地正式包复验已完成（2026-11-02）**：txgame 在线滚动 180 秒，44,999/45,000 完成，99.997778%；SLG Role / World 按当前停服重启策略各运行 A/B/A 三个 180 秒窗口，分别合计 1,028,624 / 3,179,502 次请求，均零失败。计划停服单列，不冒充在线发布成功率。槽位 1→3→5、新 owner、最终存储及 58 条进程退出记录已核对，见[联合验收](actor-routing-rollout-acceptance.md)。本轮只补验收记录，不增加运行时代码，不跑云测试、不提交。三批已批准步骤和本地验收完成，可汇总提交；剩余云端容量、长期稳态和异常接管仍按下述独立边界安排，不把控制面收益当作业务吞吐提升。

**整体审查后的收敛已完成（2026-11-02）**：权威槽位写入统一校验成员状态，缓存仅读；正常释放的 Probe 返回 Unloaded；新增本实例 Claim 并移除 World 自连控制面、代次拼装和 Watch 轮询；删除 tests-only 旧 Placement，保留现行分组放置。tx、两组 server 全量本地回归、相关六包 race 及正式包 World 启动/停服重启场景通过，见 [本轮证据](actor-boundary-fixes-20261102.md)。不重复云端或持续流量压测，本轮未提交。

## 当前能力与证据

| 能力 | 当前方案 | 验收边界 |
| --- | --- | --- |
| Actor 放置与生命周期 | 默认分区、命名分组、独立槽位；Ready 表示通信入口就绪；业务对象由应用管理；正常 Stop 条件释放，显式 PrepareHandoff 保留归属供受控晋升 | [放置](actor-placement-acceptance.md)、[Role/Room](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/actor-package.md)、[World](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/world-startup.md)；历史报告不替代本轮验证，不承诺运行中单实体热迁移 |
| GD 访问 | 无缓存显式存取；共享实体 Find 仅限有效 Procedure，普通 Snapshot；World 独占排除 | [本地回归与微基准](entity-access-acceptance.md)，不是云端容量 |
| EntityCache 管理 | Stats、List、SnapshotCached、TryRelease | [管理验收](entity-cache-management-acceptance.md)，无管理页面/全量导出 |
| Procedure | Lockey 与 Store 解耦；Call、子 Call、同步 CallAfterCommit；有界异步 Submit | [锁](procedure-locks.md)、[Submit 真实业务](https://github.com/ngnl47/txgameslg/blob/main/docs/procedure-submit-acceptance.md)；默认 64/8192，合成突发对照未覆盖 8192 档 |
| 正式包滚动发布 | txgame、SLG Role、独占 World 三分钟窗口均达到 99.99% | [2026-10-30 报告](procedure-rollout-acceptance.md)；Role 是同步 Call，不证明滚动期间大量 Submit 队列 |
| 应用合服 | GDServerMerge 定时关系；txgame 在线刷新，SLG 停服后冻结新快照 | [txgame](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[SLG](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/timed-merge-20261030.md)有限多进程通过；保留原服入口，无业务暂停/自动迁移 |

合服属于应用示例，tx 本轮不增加合服能力。初次合服验收仅核对 Fusion Redis 当前等级及 Mongo 账号投影、合服目录；2026-10-30 补验已等待正常回写并核对最终 Mongo 等级、出生服与重启读取，见下节。

## 大验收前的下一项

1. **文档整理已完成（2026-10-30）**：README 导览、按功能设计、API/配置参考、稳定契约与有日期报告分开；已收敛完成的施工步骤并校验跨仓链接、章节锚点和 Markdown 围栏。此项只做文档验证，没有重新生成或运行集群。
2. **SLG 合服后的 World 联合寻址已完成（2026-10-30）。** [八进程正式包验收](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/merge-world-20261030.md)覆盖 OriginServerID → MainServerID → Excel World 绑定 → WorldID。客户端直达与 Role→World 在停服重启后均改向主服的 World，新 owner、CAS generation 和 World 最终保存已核对；12/12 条最终通知属于有限功能证据，不是持续流量或云端容量结论。
3. **架构审查首批修复已完成（2026-10-30）**：服务后台任务关闭等待遵守期限并保留资源重试；共享放置查询与请求取消隔离；Procedure 仅暴露执行视图。冷查询采用 1024 软告警阈值、限频 Warn 并继续接纳，不以阈值拒绝批量业务。[本地回归与缓存命中对照](actor-placement-acceptance.md#2026-10-30-生命周期与冷查询边界修复)已通过，未跑集群或云测试。
4. **本地整体验收已完成（2026-10-30）：** [统一矩阵](local-acceptance-matrix.md)中的关闭重试、共享查询取消、冷寻址软告警通过三轮定向 race（36 次顶层用例）；真实依赖交接、txgame 与 SLG 多进程场景通过。修正 SLG 旧角色夹具、World 拒写注入范围及旧日志断言，保留失败证据并完整重跑；进程和专用数据清理已独立核对。有限请求不作为当前构建的持续流量完成率证据，未跑云测试。

5. **生命周期补修（2026-10-30）**：CMod、根任务和初始化回滚增加限时等待与重试保留；Placement/CMod 任务视图不暴露取消/等待所有权。正常 Stop 保留服务发送器关闭早于根传输入口取消的顺序，失败 CMod 不进入正常 Drain；验证记录见 [定向回归](local-acceptance-matrix.md#2026-10-30-生命周期补修定向回归)。本批不重复整轮集群。

6. **启动失败补修（2026-10-30）已完成**：启动期致命错误取消启动操作并拒绝成功收尾；RPC/控制面终止性异常传回 Server；可靠任务消费者静态校验移到 CMod 准备阶段，消费仍晚于业务就绪。三项失败回归、tx 全量与两组应用包级回归通过，定向 race 三轮通过，见[复审记录](audits/2026-10-30-start-stop-review.md)与[验收证据](local-acceptance-matrix.md#2026-10-30-启动失败定向回归)。

7. **正式包滚动复验（2026-10-30）已完成**：两组工程三轮 180 秒流量分别达到 99.993333%、99.997530%、99.999261%；Role/Room、World 指定启动与重载、持久化回读和 39 条进程退出记录均核对通过。见[本轮报告](lifecycle-rollout-acceptance.md)。旧 A/B 快照过期已在隔离目录按当前生成契约重新导出，未修改运行时代码或验收门槛。

生命周期修复与验收记录已提交，相关工程已快进合入本地主干；合并前回归通过。

## 合服后最终 Mongo 回写与重启读取：已完成（2026-10-30）

1. 复用两组工程 `cmd/cluster-e2e.sh --server-merge`，保留各工程 GDRole 的正式 Fusion 延迟（txgame 600 秒、txgameslg 1 秒），不增加强制写回或缩短配置。
2. 保持回写工作进程运行，等待 Mongo 等级与最后一次客户端结果精确相等，并核对 RoleID、OriginServerID 和账号投影。
3. 正常排水后重启，SLG 复用受控槽位晋升；客户端仅读取、不再次升级，核对等级和原服入口。保留 Redis，重启读取按正常 Fusion 路径执行，不冒充 Mongo-only 冷加载验证。
4. 最终回读 Redis/Mongo，确认进程退出后仅清理 ownership 匹配的专用数据。失败保留证据。

两组 `--server-merge --timeout 20m` 已通过：

| 工程 | 最终 Redis/Mongo 等级 | OriginServerID | 实际等待 Mongo 追平 | 正常退出 |
| --- | --- | --- | --- | --- |
| txgame | 两角色均为 5 | 1、2，不变 | 600.094 秒 | 12/12 |
| txgameslg | 两角色均为 4 | 1、2，不变 | 2.003 秒 | 24/24 |

正式客户端重启读取一致，宿主精确匹配本轮目标新实例。两组 server 全量本地回归和集群工具定向 race 通过；差异审查发现的新宿主断言缺口已修正并对原始结果独立复核。进程退出、Redis DB12/13、专用 Mongo 库及 etcd 清理分别核对。证据见[txgame 补验](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[SLG 补验](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/timed-merge-20261030.md)。

该轮未跑云测试、未提交；tx 当时仅更新计划，不增加框架能力或业务合服协调。上述 Mongo 补验步骤已完成，当前新主线见页首；容量或生产部署验收仍按下述独立边界安排。

此前“CMod.Start 提交 Submit 后失败”为既有 P0 的判断已撤回：当前内置组件及两组应用没有该实际调用路径，不作为阻塞验收项，不为此增加启动回滚机制。启动错误必须终止本次启动；失败清理只释放已取得资源，不恢复或维持部分服务运行。

两组应用的具体待办：[txgame](https://github.com/ngnl47/txgame/blob/master/docs/IMPLEMENTATION_PLAN.md)、[txgameslg](https://github.com/ngnl47/txgameslg/blob/main/docs/IMPLEMENTATION_PLAN.md)。生成器没有待完成的已批准新功能，扩展以 [txgen 计划](https://github.com/ngnl47/txgen/blob/main/docs/IMPLEMENTATION_PLAN.md) 为准。

## 需要专用环境或独立授权的验收

- **当前构建云端回测与长期稳态**：复用正式服务和客户端，记录完成率、P99、CPU/内存、队列及客户端计划发起延迟。[七机 20/50 报告](audits/2026-09-28-seven-host-2050-acceptance.md)只证明历史构建三次 180 秒窗口。不自动租赁云实例。
- **JetStream 生产部署**：按 [部署说明](deployment/jetstream-tasks.md)确认磁盘同步、副本与物理故障域，同机容器结果不替代跨宿主容灾。
- **会话积压容量**：当前示例 1024 条/8 MiB。取得实际慢客户端、离线时长、消息大小后再评估，不放开为无界缓存。
- **持续 Submit 滚动验收**：真实业务大量使用 Submit 时单独验证；当前同步 Role 与独占 World 的发布结果不覆盖此场景。

## 需要新需求才能实施的方向

- 配置中心/进程内切换：当前外部 JSON 整套校验、滚动重启；主动拉取、在途快照及双端协调另行设计。
- 控制面认证、TLS/mTLS 与证书轮换。
- 异常接管、自动扩容与单实体热迁移；不把成员过期当唯一执行者证明。
- 新存储后端、自动 getter 加载与更深生成器扩展。
- 历史榜单合并、奖励结算、赛季和 World 数据迁移；均是应用业务，不为合服目录闭环提前实现。

## 文档与验证规则

设计描述现行实现，计划描述未完成工作，报告描述指定日期与构建条件。链接调整不改写历史数据，不把跳过的外部测试算通过；文档审查不能替代运行时测试。轻量差异审查使用 [tx-contract-review](../.agents/skills/tx-contract-review/SKILL.md)，规则只维护于工程约定与审计标准。

## 已完成：通信 Actor 与业务对象生命周期分离（2026-10-31）

- 框架已补齐 `WithIdlePassivation(0)` 显式禁用；未指定时保留原配置回退，指定类型策略时优先。ActorHost/公开模块及全库本地测试、Actor 定向 race 已通过。
- 应用按类型声明 Role 10 秒、Room 70 秒、World 禁用空闲卸载，移除重复 YAML。Room/World 由模块管理业务对象、Tick 和收尾，通信工厂仅绑定协议。
- 本地验证包含通信 Actor 卸载重激活不丢业务状态、World Tick 与最终保存、Room 通知与排水。Room 多房间慢通知隔离已通过先失败后修复的回归及 race 检查。
- SLG 显式启动已实施：通用 `WithExplicitAssignment` 约束普通寻址及 Ensure/Retry，默认 Role/Room 行为不变。按 --world-id 登记后直接加载 World，通信 Actor 不前置激活。初版匹配旧 InstanceID 晋升的本地回归及正式包证据见 [指定启动记录](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/world-explicit-startup.md)。2026-11-02 正常 Stop 已改为释放，后续启动直接认领空闲代次；旧身份仅用于仍保留归属的显式处置，见页首第二批记录。
