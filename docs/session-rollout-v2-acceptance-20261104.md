# 原地址发布与分阶段统计验收（2026-11-04）

两组本地普通正式包串行运行 Login、Agent、MsgQ 三个 180 秒窗口，共六窗。使用现有 `cmd/cluster-e2e.sh --session-rollout --timeout 15m`；本轮未修改实现，不重投、不补发、不跑云测试、不提交。构建基于 HEAD 加当前未提交变更，原始差异和二进制摘要均已归档。

## 结果与统计边界

每窗 40 连接 × 5 次/秒 × 180 秒，TCP/WS 各半，完整计划分母均为 36,000。正常阶段总计及 affected/control 两组分别判定 99.99%；发布阶段单列，原始全窗口判定保留。

| 工程 | 发布节点 | 正常完成 / 计划 | 全窗口完成 / 36,000 | 全窗口完成率 | 未发送 | 在途未知 | 连接中断范围 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| txgame | login | 36,000 / 36,000 | 36,000 | 100.000000% | 0 | 0 | 未断开 |
| txgame | agent | 35,880 / 35,880 | 35,881 | 99.669444% | 119 | 0 | 1.177～1.184s |
| txgame | msgq | 36,000 / 36,000 | 36,000 | 100.000000% | 0 | 0 | 0.096～0.115s |
| txgameslg | login | 36,000 / 36,000 | 36,000 | 100.000000% | 0 | 0 | 未断开 |
| txgameslg | agent | 35,870 / 35,870 | 35,880 | 99.666667% | 120 | 0 | 1.177～1.185s |
| txgameslg | msgq | 36,000 / 36,000 | 36,000 | 100.000000% | 0 | 0 | 0.084～0.100s |

六窗正常阶段均为 100%，恢复、客户端清理及服务端证据核验全部通过，两套入口退出码均为 0。两个 Agent 窗口的完整完成率仍未达到 99.99%，保留 `completion=failed`；不能据此宣称发布全过程 99.99% 达标。

- 六窗对照组均为 18,000/18,000；超时、迟到、重复及不匹配响应均为 0。
- Agent 发布阶段：txgame 为 120 个计划（完成 1、未发送 119）；SLG 为 130 个计划（完成 10、未发送 120）。成功样本同样按发布边界划入，没有只剔除失败样本。
- 两个 MsgQ 窗口确实断连并恢复，但中断未跨过本轮计划发送时刻，发布阶段计划数为 0（not-measured），不能解释成没有中断或以后都不丢请求。
- 独立按总计及分组逐字段验证 `normal + publication = raw total`。完整分母合计 216,000，未发送 239，在途未知 0，完成 215,761。
- 中断按客户端首次观察断连至恢复可发流计算，包含离线通知、恢复探针与工具编排；约 1.18 秒不是纯 Agent 进程启动耗时。没有新增发布中断 SLA，也不将此次低负载结果扩展为容量结论。

## 恢复与持久化证据

```text
停服前发布标记确认
  → 旧 Agent 正常退出 → 原地址启动新 Agent
  → 原 SID Restore → 离线通知与业务探针完成
  → 新 CID、原 MsgQ owner → 恢复正常阶段发流

MsgQ 正常退出并释放 owner
  → 旧 SID 恢复获明确 HandshakeRejectedError
  → 同角色新登录、新 SID → 新邮箱业务请求成功

SLG 三窗结束
  → 最终标记确认 → 保持 Fusion 消费在线，等待 Mongo 追上
  → 全服务排水退出 → Mongo 最终读回 → ownership 清理
```

两组 Agent 均核对 20 个受影响连接的 SID 不变、CID 更换、MsgQ owner 不变、离线通知接续及 `upstreamReplayed=false`；替换 Agent 新登录成功。两组 MsgQ 均核对 20 个连接（含 10 个 WebSocket）的明确拒绝与新 SID，未盲重投。Login 发布期间备用与替换 Login 均能签票接入。

SLG 各窗客户端直达 World 与 Role→World 的完成通知均来自预期宿主；Role/World 槽位 owner 与 generation 保持不变。最终持久化报告通过：120/120 角色的最终名称、RoleID 与 OriginServerID=1001 一致；World 1 / Bucket 7 / Unit 1001 坐标 (31,32)，文档版本 2。这里引用工具在清理前的 Mongo 读回证据，未声称清库后还能重复读取这些业务记录。

## 正常退出与清理

两组各 11 份进程退出记录全部 observed=true、exitCode=0，没有排水或退出错误。工具成功后按 ownership 清理专用范围；独立只读检查确认 Redis DB12/13 均为 0 键，两个专用 Mongo 库无集合，etcd 专用目录及精确发号键为空，无本轮服务或客户端进程残留。其他依赖容器和数据范围未修改。

## 原始证据

```text
tx-artifacts/session-rollout-v2-20261104/
├── txgame/20261104-211906.650701000/
│   ├── installed/relocated/            # 普通正式包
│   ├── rollout/{login,agent,msgq}/     # 原始客户端结果及服务端身份取证
│   └── processes/                     # 日志与 11 份实际退出证据
├── txgameslg/20261104-212942.316840000/
│   ├── rollout/{login,agent,msgq}/     # 三窗及最终业务结果
│   └── session-rollout-persistence.json # 排水后的 Mongo 读回
└── evidence/
    ├── source.json / *-worktree.diff  # 构建时仓库身份与未提交差异
    ├── results.json                  # 原始 JSON 的只读汇总
    ├── postflight.json               # 独立残留检查
    └── verification-notes.md          # 统计闭合和恢复边界复核
```

机器摘要见 [验收数据](validation/session-rollout-v2-20261104.json)，当前口径见 [验收计划](session-rollout-acceptance-plan.md#3-正常运行发布中断与完整分母)。历史 schema v1 报告和机器记录保持不变。

本轮仅新增验收记录并更新计划索引，没有发现需要修改实现的新问题。下一步可审阅并提交现有统计与文档变更；无需为本轮通过而扩大功能或继续调节发布窗口。
