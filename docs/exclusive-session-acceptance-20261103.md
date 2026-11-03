# 独占接入与 MailboxStream 协调验收

日期：2026-11-03。范围：tx、txgame、txgameslg、txclient、txclientslg。本轮不提交、不跑云测试。当前边界见[实施计划](exclusive-session-plan.md)，接口见[Agent API](api-reference.md#agent-玩家接入)。

## 职责与时序

```text
Client → Login：认证、选角色/借号授权、选 Agent、分配 SID、签票
Client → Agent：携票接入
Agent → 角色 owner MsgQ：Init + MailboxStream 控制绑定
MsgQ → 旧 Agent：原共享流 REVOKE
旧 Agent：停止新上行准入、关闭 Socket → MsgQ：DETACHED
MsgQ：终止旧邮箱、完成新接入 → Agent：OPENED
Agent → 新 Client：Ready
```

签票不占位；非强制占用在接入时拒绝，且不改变旧票据水位。只有接纳的 TicketOrder 推进水位；已退出或被替代的旧票据不能重新 fresh/restore。应用负责认证、强制借号授权与策略，tx 不识别 GM。顶号不撤回已经进入 Actor 的业务。

```text
普通断线：关闭本地接入 → 精确解除绑定并确认 → 保留下行邮箱
主动退出：Client → Agent → MsgQ 精确终止 → LoggedOut → 关闭 Socket
离线退出：携原票据连接 Agent 发送 Logout → 定位已有归属 → 精确终止
MsgQ 正常关闭：关接入 → 有界并发撤销并确认 → 全部成功后条件释放 owner
```

没有 owner 的退出不分配 MsgQ；该入口终止已存在的逻辑会话，不承诺撤销从未接纳的票据。普通流断开、发送入队、等待超时均不等于远端关闭确认。控制操作不扩展为通用四语义 API；Ack、Renew、下行推送保留单向热路径。

## 本地回归

证据根目录：`/private/tmp/tx-mailbox-takeover-20261103/`。这里仅记录本轮新模型；旧 `/private/tmp/tx-exclusive-session-20261103/` 的 Login 预留/Agent 广播模型已撤回，不能证明当前方案。

| 范围 | 已执行的验证 | 日志 |
| --- | --- | --- |
| tx | 最终全包测试通过；Agent/MsgQ 定向并发测试通过 | `tx-final-full.log`、`agent-race.log`、`msgq-race-final.log` |
| 真实 Redis | 单角色 CAS 竞争只有一个成功、退出水位与未确认接入不因 Session TTL 消失；只清理本轮唯一前缀 | `msgq-redis-final.log` |
| txgame/server | 全包测试，Login/Agent/session race，本地真实 MsgQ 握手 | `txgame-final-full.log`、`txgame-app-race.log`、`txgame-local-msgq.log` |
| txgameslg/server | 全包测试，同组 race，本地 Agent→Role/World→MsgQ 实际集成 | `txgameslg-final-full.log`、`txgameslg-app-race.log`、`txgameslg-agent-route.log` |
| txclient | 133 项测试与构建通过 | `txclient-npm-test.log`、`txclient-build.log` |
| txclientslg | 143 项测试与构建通过 | `txclientslg-npm-test.log`、`txclientslg-build.log` |

针对回归包括：Ready 等远端接入；非强制拒绝不踢人；旧票据失效；旧 CID 不能退出重连后的新 CID；等待撤销不阻塞其他角色；重建共享流上精确解除旧绑定；失败候选清理与恢复失败保留下行；离线退出不 Acquire；正常 Stop 未确认时保留 owner。

## 有限多进程证据

复用两工程 `cmd/cluster-e2e.sh`，正常运行后由现有工具等待进程实际退出、核验 ownership 并清理专用数据。没有放宽断言或绕过清理门禁。

| 工程/入口 | 结果 | 本轮目录 |
| --- | --- | --- |
| txgame 正式包 `--session-lifecycle` | 10 阶段通过、12 次退出为 0；Login 替换不扰动连接、Agent 重连恢复、MsgQ 关闭释放归属并拒绝旧 SID | `txgame-lifecycle/20261103-223847.097067000/` |
| txgameslg 正式包 `--session-lifecycle` | 10 阶段通过、12 次正常停止；保留 Role/World 生命周期及归属边界 | `txgameslg-lifecycle/20261103-223936.503249000/` |
| txgame 默认有限套件 | 跨不同 Agent 顶号；`signedWithoutTakeover`、旧 fresh/restore 拒绝、恢复及离线退出全部通过 | `txgame-exclusive/20261103-224333.353624000/` |
| txgameslg 默认有限套件 | 相同独占场景、World 业务链路；202 个不同身份及 20 个同身份并发签票全部成功 | `txgameslg-exclusive/20261103-224456.099541000/` |

默认套件沿用已有诊断构建，不能记作普通正式包验证。真实 GM 授权由应用 SPI 测试覆盖；多进程验收使用本人重新登录共用的替换链路，不冒充生产 GM 认证端到端验证。

## 差异审查与收尾

按 ponytail-review 及当前工程约定检查真实调用链，本轮发现并修复：

1. 主动 Logout 取消邮箱后可能提前关闭 Socket。退出分支停止自动断开回调，并有期限地等待在途下行写完成后回写 LoggedOut。旧行为的重叠回归失败，修复后通过；不以取消 WebSocket 写入代替退出确认。
2. gRPC OPENED 明确拒绝时，错误清理不应抢先关闭 Agent Socket。补真实 gRPC fresh/restore 回归，旧行为 overlay 失败，修复后 SessionEnded 可送达并通过 race。
3. 失败 Open 的精确清理不应遗留无人消费的新邮箱；恢复失败则必须保留原未确认下行。覆盖最终准入写响应丢失后的再次恢复。
4. MsgQ Stop 不应逐会话串行累积网络往返；改为固定上限并发确认，所有确认完成后才释放 owner。
5. 外层 MsgQ Service 的关闭结果必须与内层保留状态/重试契约一致，不能先清掉 mailbox 引用或把未完成当作最终关闭。

6. SLG 全量并行回归实测发现测试夹具会读到刚创建、尚未写完的空 PID 文件。仅将夹具就绪文件改为写临时文件后原子 rename；全包复跑通过，不修改生产进程管理。

最后补修后，tx 与两组 server 的完整 `go test ./... -count=1` 通过，MsgQ 全包 race、Agent 定向 race、真实 Redis CAS 通过。非强制并发接入另以 20 轮 race 验证只有一个成功。独立 Agent/客户端及 MsgQ 审查记录无剩余待修项。

两组正式包生命周期在上述关闭补修后重新执行，各 10 阶段、12 次正常停止全部通过：

| 工程 | 最终产物目录 | 实际二进制 SHA-256 |
| --- | --- | --- |
| txgame | `txgame-lifecycle-final/20261103-225302.619210000/` | `8776f1b5b4ff256ae0166fa8f699fb818fda0a3afc74b548e62317541f097aa5` |
| txgameslg | `txgameslg-lifecycle-final/20261103-225349.219699000/` | `b4c2c011d3e40d5784f51772c5578944f07ae58eec804d1f7673addc28284d92` |

默认跨 Agent 独占套件没有在最后失败清理/关闭补修后整套重跑；其构建证据仍按上表原目录记录，新增关闭与失败边界由专项和最终正式包验证承接。

固定每个回执延迟 1ms 的本地 Stop 微基准：1 会话约 1.13ms，64 会话约 1.62ms，说明确认等待没有串行累计 64 次往返。该值是模拟控制延迟，不是生产网络吞吐或万连关闭时间。

审查与关键复现：`agent-client-contract-review.md`、`msgq-independent-review.md`、`agent-admission-review-red.log`、`logout-red.log`、`logout-downlink-red.log`、`logout-final.log`、`msgq-stop-benchmark.log`。没有云端容量、吞吐/延迟改善或网络分区无损接管结论。
