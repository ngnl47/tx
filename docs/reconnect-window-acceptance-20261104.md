# 立即重连窗口修复与会话补验

本轮承接[正式包收尾验收](final-package-acceptance-20261104.md)中的两次 SLG 立即重连失败。只修复接入与关闭重叠及其验收观察窗口，不改变 Login 签票职责、MsgQ 归属、上行重放规则或客户端重试策略，不跑云测试、不提交。

## 原因与修复

客户端关闭 socket，只能证明本地连接结束；旧 Agent 接入仍可能等待 MsgQ 的 CLOSED。此时相同 SessionID、新 ConnectionID 的恢复请求合法到达，但 MailboxStream 客户端原先只按 SessionID 登记，会在请求抵达 MsgQ 前返回 `ErrSessionChanged`。真实 gRPC 回归稳定复现了这一分支。

```text
客户端：关闭旧连接 → 立即携原票据恢复
Agent ：旧 Close 尚未确认 ── 新 OPEN 到达
流目录：按 RoleID + SessionID + ConnectionID 分开登记新旧接入
MsgQ ：串行协调角色接入 → 撤销旧入口 → 确认新入口
Agent ：新连接 Ready；旧 Close 只移除旧接入
```

沿用服务端现有完整接入身份与角色准入，补齐三个相关边界：

- 旧连接的 Terminate 在取得角色准入后校验完整身份，避免等待期间 Restore 切换 ConnectionID 后仍终止新连接。
- 已解除接入的旧句柄发送控制请求后移除临时目录项，拒绝结果不留下旧登记。
- Agent 已取消的初始化不得发布为活动连接，避免迟到结果覆盖恢复连接。

生产代码未增加公共 API、协议字段、自动重试或 Agent 间控制通信。普通 Close 仍只解除物理接入，Logout 才终止逻辑邮箱。

## 验收观察窗口

最终框架构建的 SLG 客户端已完成立即重连、跨 Agent 顶号、旧票据拒绝和 Logout，但一次有限套件仍等待初始 Session 的 Redis 证据。该 Session 在建立约 69ms 后被顶号替换，早于工具的 100ms 文件轮询，导致已删除的观察目标永远不会再次出现。本次主动取消等待，保留失败记录，不将客户端成功等同于整个套件成功。

两组有限验收复用文件确认屏障：先采集初始接入证据，再允许客户端执行顶号。确认只位于验收 CLI，立即重连步骤不加入固定延时，生产客户端及持续流量路径不变。

## 验证范围

本轮修复与相关会话补验通过。最终程序包含全部运行时修复，两个普通包均从源码外目录启动，元数据为 `diagnostics=false`，程序 SHA256 与包声明一致，构建信息无 `txbench`。有限套件沿用诊断构建，结果分别记录。

| 验证项 | 最终结果 |
| --- | --- |
| tx 完整回归 | `go test ./...` 通过 |
| 会话并发 | Agent、MsgQ 和框架会话定向 `-race` 通过；新增窗口均先复现失败再修复 |
| 两组应用 | Agent/MsgQ/启动装配及有限验收工具定向 Go 回归通过 |
| 两客户端 | txclient 134/134、txclientslg 144/144，完整测试与构建通过 |
| txgame 普通正式包 | 10 阶段、12 次正常退出；Login、Agent、MsgQ 生命周期及恢复通过 |
| txgameslg 普通正式包 | 10 阶段、12 次正常退出；保留 Role/World 业务进程及归属边界 |
| txgame 有限套件 | 签票不占位、跨 Agent 顶号、旧 fresh/restore 拒绝、恢复及退出后拒绝通过 |
| txgameslg 有限套件 | 相同会话边界通过；既有 MsgQ 重启、World 回写及多 World 归属断言通过 |
| 最终四轮退出记录 | 共 52 份：50 份为 0；SLG 故意终止的 `slgworld-a`、`msgq-a` 为预期 `signal: killed`；无缺失或意外退出 |
| 收尾 | 无验收进程残留；Redis DB 12/13 均 0 键，两组专用 Mongo 无集合，etcd 专用前缀无键 |

运行时修复及工具补修分别完成限定范围独立 review，无待修项；格式和差异检查通过。正常退出、预期故障注入与主动取消的工具失败分别统计，没有互相替代。

有限套件使用应用已有的独占登录策略，框架回归覆盖非强制占用拒绝、强制替换与精确退出；不把这些场景扩张为生产 GM 授权系统验收。有限请求逐项核验契约，不用其小样本计算 99.99% 可用率。

原始证据目录为 tx 同级 `tx-artifacts/reconnect-fix-20261104/`。此前七个 180 秒窗口仍对应原始构建，本轮不重复执行，也不将其改记为修复构建的性能数据。两次原始运行时失败与本轮工具观察失败均保留。

```text
tx-artifacts/reconnect-fix-20261104/
├── red.log、stale-control-red.log     # 修复前稳定复现
├── tx-full-final.log、session-race-final.log
├── evidence-gate-*.log               # 工具采集屏障的红绿及客户端回归
├── txgame-evidence-gate-verification.log
├── txgame-session/                  # 最终普通正式包生命周期
├── txgameslg-session/
├── txgame-exclusive/                # 最终有限套件
├── txgameslg-exclusive-gated/
├── txgameslg-exclusive/              # 中间框架构建的首次通过，非最终证明
├── txgameslg-exclusive-final/        # 保留工具观察失败与主动取消记录
├── runtime-source.json              # 基础提交及未提交运行时源码摘要
├── postflight.json                  # 独立存储与进程收尾检查
└── evidence.json                    # 最终四轮摘要与工具源码摘要
```

可复用各应用根目录的 `sh cmd/cluster-e2e.sh --timeout 8m --artifact-root <目录>` 运行有限套件；普通正式包追加 `--session-lifecycle`。最终紧凑记录见[机器记录](validation/reconnect-window-20261104.json)，源码基于记录中的提交加本轮未提交差异，不冒充已提交版本。
