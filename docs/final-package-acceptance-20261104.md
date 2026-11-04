# 当前版本本地正式包收尾验收

本报告保留修复前构建的原始结果。其立即重连问题已在后续修复并完成会话补验，见[独立修复报告](reconnect-window-acceptance-20261104.md)；本页流量与失败记录不改记为后续构建结果。

本轮复用现有入口，核验会话链路、180 秒持续流量、正常关闭和专用数据清理。不修改运行时或客户端实现，不增加注释，不跑云测试，不提交。首次失败与后续结果分别保留，不能用通过的重复运行覆盖失败。

本轮结论：七个 180 秒流量窗口均达到 99.99%，普通正式包会话生命周期两组均通过；但 SLG 默认有限套件两次在立即重连阶段失败，因此整体会话收尾验收未通过。下一步先定向复现并修复重连与旧接入关闭重叠的窗口，再复跑相关场景，不需要重复扩大流量档位。

## 版本与构建

| 工程 | 验收版本 |
| --- | --- |
| tx | `8f9ae0bf965b6af09727b15944a39f034bca95ba` |
| txgen | `e245df6ad0ccfbfa94a57f58b99485aa21197467` |
| txgame | `488629a1ca9c4687b497ca0eecc43c29d8b0eb17` |
| txgameslg | `66c49744d88e64db59b3620b50100e5be4de303d` |
| txclient | `08180917dd2edecdfd984de483bd13bea8166b31` |
| txclientslg | `e5590c7a3fce6a056be50578c1e5658b31b96d20` |
| txconfig | `6592808750bac6f05d433ef5eb4b29cb6f6bccbb` |

环境为本机 Intel macOS、Go 1.27.0、Node 26.0.0，本地 Docker 提供 etcd、Redis、MongoDB 和 NATS。两组专用 Redis DB 为 12/13，Mongo 为 `txgame_cluster_e2e` / `txgameslg_cluster_e2e`，etcd 分别限定在 `/tx/e2e/txgame/` 与 `/tx/e2e/txgameslg/`。首次预检均为空；后续入口保留原有 ownership 校验。

正式包会话与流量均从源码外目录启动。SLG 发布入口原先固定启用诊断构建，本轮使用证据目录内的临时 Go overlay，仅将 `PackageProgram(..., true)` 改为 `false`；产品源码、包内容生成规则和验收断言不变。默认有限集群套件继续使用原有 `txbench` 构建，补验跨 Agent 顶号等边界，其结果与普通正式包分开统计。

## 会话与退出

| 场景 | 本轮结果 |
| --- | --- |
| txgame 普通正式包会话生命周期 | 通过，10 个阶段，12 次正常退出 |
| txgameslg 普通正式包会话生命周期 | 通过，10 个阶段，12 次正常退出 |
| txgame 默认有限诊断套件 | 通过，包含跨 Agent 顶号、签票不占位、旧 fresh/restore 票据拒绝、重连及退出后拒绝恢复 |
| txgameslg 默认有限诊断套件首次运行 | 失败，Login 停止后第二个角色立即重连被 `msgq: session has changed` 拒绝；8 个服务进程清理退出码均为 0 |
| txgameslg 原样复测 | 再次失败，第一个角色立即重连被相同错误拒绝；8 个服务进程清理退出码均为 0 |

普通正式包会话覆盖 Login 停止不影响已有连接、Agent 重启后的新登录与旧恢复拒绝、MsgQ 停止后的 owner 释放以及其他邮箱不受影响。Ready 后有业务请求验证，不把接入确认当作业务完成。本轮未新增生产 GM 认证入口，不将本人顶号场景解释为完整的 GM 授权端到端验收。

第二次在 `12:22:12.889` 复现相同错误，角色为 `480101`。两次失败均早于套件后续的跨 Agent 顶号步骤，因此该步骤本轮在 SLG 上尚未完成；不能借用历史通过结果补记。

SLG 首次失败中，角色 `480102` 的新连接拒绝和旧连接关闭均发生在 `11:57:15.441`。共享 MailboxStream 的客户端以 SessionID 登记接入；旧接入 Close 收到远端确认后才移除，新 OPEN 遇到仍登记的 Session 会返回 `ErrSessionChanged`。这与日志吻合，是待定向验证的竞争点；目前日志不足以唯一确定本次错误来自哪个返回分支，不能写成已证明根因。客户端 `TCPTransport.close()` 只等待本地 socket close；Agent 在本地 disconnect 后仍需等待远端 CLOSED，因此客户端 close 完成不构成 MsgQ 收尾屏障。未增加延时、重投或放宽断言。失败日志及专用库快照保存在原证据目录，后续独立场景由现有 ownership 入口重建专用数据。

## 持续流量

txgame 使用 50 个连接、每连接 5 次/秒，180 秒窗口包含 A→B→A 滚动发布；按计划数核验完成率。SLG Role 与 World 使用 4 个闭环并发连接，各运行 A/B/A 三个 180 秒窗口；按实际尝试数核验，不包含停服重启间隔，也不视为固定速率容量测试。

| 场景 | 分母 | 完成 | 失败 | 完成率 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| txgame 连续发布 180 秒 | 45,000 | 44,997 | 3 | 99.993333% | 1.323 ms |
| SLG Role A / 180 秒 | 275,705 | 275,705 | 0 | 100% | 4.452 ms |
| SLG Role B / 180 秒 | 316,662 | 316,662 | 0 | 100% | 3.536 ms |
| SLG Role A 回滚 / 180 秒 | 317,913 | 317,913 | 0 | 100% | 3.556 ms |
| SLG World A / 180 秒 | 918,521 | 918,521 | 0 | 100% | 1.471 ms |
| SLG World B / 180 秒 | 889,888 | 889,888 | 0 | 100% | 1.525 ms |
| SLG World A 回滚 / 180 秒 | 895,985 | 895,985 | 0 | 100% | 1.513 ms |

七个窗口均达到 99.99%。World 三轮共 2,704,394 次，全部完成。Role 三轮共 910,280 次，全部完成；每轮 4 个角色的 Fusion/Mongo 最终值均通过。txgame 的 3 次失败全部计入，无未发送请求；未因发布边界而剔除失败。

## 持久化与残留边界

持久化断言复用现有入口：SLG Role 每轮核对 Fusion 与 Mongo 最终角色值，World 最终停止后核对 4 个桶、4 个单位，均通过。txgame 本次配置流量核验背包值，但不等待 600 秒 Fusion 周期，不宣称完成其最终 Mongo 回写专项。World 发布后先运行 warmup 再发测量请求，不能将最终落盘核对等同于修改前冷启动读取证明。

独立收尾检查结果：

- 100 份服务进程退出文件均存在；98 份为 0，另 2 份是 txgame `wrong-schema`、`missing-file` 的预期启动拒绝。两次有限套件的失败是客户端/验收进程非零退出，不与服务端退出记录混算。
- 未发现本轮服务、验收入口或客户端探针进程残留；两组 etcd 实例注册均已消失。
- txgame Redis DB 12 为 0 键、专用 Mongo 无集合、etcd 专用前缀无键。
- SLG Role/World 成功入口均执行其 ownership 清理。最后一次失败按既定规则保留现场：Redis DB 13 有 11 键，Mongo 有 8 个集合（含 ownership），etcd 有 4 键（owner、发号键、两个空 owner 槽位）。这不是仍在运行的实例或未结束任务，不据此宣称专用数据已全部清空。
- 5 份普通正式包均核对元数据 diagnostics=false、程序摘要一致、构建信息无 txbench；两组会话包和流量包使用相同工程版本。SLG 复用的三个场景程序摘要一致。

## 证据与复现

原始证据位于 tx 同级目录 `tx-artifacts/final-package-20261104/`：

```text
tx-artifacts/final-package-20261104/
├── environment.json                  # 七仓版本与初始工作区状态
├── preflight.json                    # 专用数据预检
├── acceptance-ledger.md               # 执行进度与范围决定
├── configs/{a,b}/                    # 现有配置快照及加载校验
├── harness-overlay/                  # SLG 工具的普通包临时构建覆写
├── txgame-session/                   # 普通正式包会话生命周期
├── txgameslg-session/                # 普通正式包会话生命周期
├── txgame-exclusive/                 # 诊断构建有限会话补验
├── txgameslg-exclusive/              # 首次失败及 failure-state.json
├── txgameslg-exclusive-repeat/       # 原样复测失败及现场快照
├── txgame-config-release-*/           # 普通正式包持续发布流量
├── slg-role/                         # 普通正式包 Role A/B/A
├── slg-world/                        # 普通正式包 World A/B/A
├── postflight.json                   # 独立退出与数据残留核对
└── evidence.json                     # 从原始结果提取的核验数据
```

会话复用各应用根目录 `sh cmd/cluster-e2e.sh --session-lifecycle --timeout 8m --artifact-root <证据目录>`；默认补充套件去掉 `--session-lifecycle`。txgame 配置流量复用 `TXGAME_CONFIG_RELEASE_A/B`、`TXGAME_CONFIG_ARTIFACT_ROOT`、`TXGAME_CONFIG_TRAFFIC=1` 与 `go test ./cmd/internal/deploytest -run '^TestConfigReleaseRollback$' -count=1 -v -timeout=10m`。

SLG 在 server module 运行 `go run -overlay=<overlay.json> ./cmd/cluster-perf --workload <role-rename|world-move-unit> --concurrency 4 --release-a <A> --release-b <B> --warmup 2s --duration 3m --timeout 15m --artifact-root <证据目录>`；World 额外指定 `--world-ids 490001,490002`。两组依次执行，不并行占用同一验收资源。

紧凑机器记录见 [本轮数据](validation/final-package-20261104.json)。失败现场快照只保存在本机证据目录，不提交测试账号资料或票据。

本轮有限 ponytail-review 已独立核对五份普通包、七个流量窗口、两次失败日志、退出文件、残留记录与七仓差异，未发现报告事实错误或过度结论。此结论只表示验收记录可交付，不表示运行时问题已修复或整体验收通过。文档链接、JSON 记录与 `git diff --check` 均已核验。
