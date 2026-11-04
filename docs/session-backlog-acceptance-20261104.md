# 本地正式包会话积压验收（2026-11-04）

## 结论与范围

两组工程均通过本地正式包验收，未修改生产会话实现。复用 `cmd/cluster-e2e.sh --session-lifecycle`、既有通知协议、AgentClient 和 TCP/WebSocket 传输。每会话仍为 **1024 条 / 8 MiB**；验收模板的离线期限从 30 秒对齐示例的 180 秒，未搜索最优容量、未运行云测试。

慢消费通过暂停业务回调、延迟 Ack 实现，socket 仍继续读取。它验证未确认邮箱的积压、裁剪和恢复，不代表物理慢网络或带宽背压，也不能用于估算 8c16g 的生产容量。

## 固定场景与证据

| 场景 | 输入与判定 | 结果 |
| --- | --- | --- |
| 慢消费 | TCP、WS 各 500 条 × 1 KiB 文本，暂停业务消费至少 10 秒；记录实际编码字节与 Ack 游标，恢复后逐条验证数量及顺序 | 两组通过 |
| Agent 带积压关闭 | 两个会话各保留 256 条 × 16 KiB 文本；正常关闭被测 Agent，不等客户端消费或 Ack | 两组通过 |
| 原 Session 离线恢复 | Agent 停止后各追加 128 条 × 16 KiB 文本，等待 10 秒；原地址重启后保留 SID、更新 CID，接续全部离线通知，不重投上行 | 两组通过 |
| MsgQ 带积压关闭 | 两个会话各保留 256 条 × 16 KiB 文本，关闭其中一个 owner；旧 Session Restore 明确拒绝，新登录建立新 SID，另一 owner 的会话继续工作 | 两组通过 |
| 条数边界 | 独立 TCP 会话保留 1024 条，每条完整载荷 1024 字节；追加一条后淘汰 | 两组通过 |
| 字节边界 | 独立 WS 会话保留 512 条，每条完整载荷 16384 字节，合计 8388608 字节；追加一条后淘汰 | 两组通过 |
| 淘汰与隔离 | 不仅检查断线，还要求明确 Restore 拒绝或 SessionEnded；健康连接继续 Ping，新票据创建独立 Session | 两组通过 |
| 收尾 | 正式进程正常退出、owner/Role lease 取证、专用测试范围 ownership 清理；事后检查无本轮服务/客户端进程残留 | 两组通过 |

文本大小不同于邮箱载荷大小。精确容量场景使用实际响应字段和共享 BON 编码器校准，包括命令哈希的 4 字节及 BON 长度前缀；以真实收到的编码字节再次核验。发布响应只是 Tell 已提交，不是远端 MsgQ 入队凭证。满额场景要求全部下行已到达且消费/Ack 未推进；溢出另核对 MsgQ 的 `backlog_overflow` 日志。

Agent 关闭后，客户端先处理本地已经收到的旧数据，再恢复；离线追加通知用于证明真正的服务端邮箱续接。MsgQ 关闭后的新登录不迁移旧邮箱。带积压关闭不是“等待所有客户端 Ack 后才关闭”。

## 正式包结果

### txgame

- artifact：`txgame/server/artifacts/session-backlog/20261104-225718.331316000/`，汇总文件 `session-lifecycle-summary.json`；实际可执行程序由正式包解压后在源码外目录启动，`diagnostics=false`。
- 二进制 SHA256：`8ca54d7a5a9174993fbc8707eee3059b9a08f4baaa62c2345ba63e05eebc93c4`。
- 慢消费阶段 21257 ms；原 Session 恢复阶段（包含下一阶段积压准备）5693 ms；容量阶段 16921 ms。
- 12 次正常 Stop 全部完成。带积压被测 Agent / MsgQ 的 Stop 等待分别为 23 / 19 ms；这是本地工具等待耗时，不是生产停服时延指标。
- TCP 满额实际保留 1048576 字节；WS 满额实际保留 8388608 字节。对应淘汰 SID 为 756460 / 756462，已与 MsgQ 溢出日志核对。

### txgameslg

- artifact：`txgameslg/server/artifacts/session-backlog/20261104-225904.554048000/`，汇总文件 `session-lifecycle-summary.json`；实际可执行程序由正式包解压后在源码外目录启动，`diagnostics=false`。
- 二进制 SHA256：`0d6eaff9929d277ae6389cad691076288e06dbfef046d75237dd610c57687fdf`。
- 慢消费阶段 21022 ms；原 Session 恢复阶段（包含下一阶段积压准备）5722 ms；容量阶段 17009 ms。
- 12 次正常 Stop 全部完成。带积压被测 Agent / MsgQ 的 Stop 等待分别为 21 / 20 ms；这是本地工具等待耗时，不是生产停服时延指标。
- TCP 满额实际保留 1048576 字节；WS 满额实际保留 8388608 字节。对应淘汰 SID 为 756570 / 756572，已与 MsgQ 溢出日志核对。

## 回归与失败记录

- 两组客户端完整 `npm test`、`npm run build` 通过；新增积压探针测试再次定向执行通过，覆盖延迟 Ack、数量/顺序、准确容量、恢复拒绝与新登录。
- 两组服务端 `go test ./cmd/internal/clustere2e ./cmd/internal/clusterkit` 通过。
- tx 的 `TestMsgQConfiguredBacklogLimits` 及既有 MsgQ 容量/恢复定向 race 回归通过。框架测试补充满额后 Ack 释放额度及再次追加的精确验证。
- 初次 txgame 正式包 `20261104-225018.984636000` 失败：关闭前约 4 MiB 未确认载荷与离线追加约 4 MiB 叠加，正文以外的编码字节使总量超过 8 MiB，两个邮箱正确记录 `backlog_overflow`。修正夹具离线追加为 128 条，未放宽容量。中间回归 `20261104-225243.208715000` 通过慢消费/恢复/关闭，但尚不含最终精确容量场景。
- 初次 SLG 正式包 `20261104-225512.724608000` 在容量阶段失败：探针错误要求溢出先收到 SessionEnded。现有契约为断开接入后明确拒绝 Restore；按真实契约修正断言，不把单纯传输断开当作成功。
- 校准单测发现 16 KiB 边界处 BON 长度前缀发生变化；修正按实际字段二次编码校准，最终满额字节通过真实链路复核。

失败 artifact 保留，没有将失败覆盖成通过。运行时没有新增恢复机制、缓冲层或生产协议。生产容量、全实例内存预算、物理网络背压和高并发长稳态不在本轮结论内。

## 复现

在 txgame 或 txgameslg 工程根目录，准备既有本地依赖后运行：

```sh
./cmd/cluster-e2e.sh --session-lifecycle --timeout 8m --artifact-root artifacts/session-backlog
```

编排器自动传入客户端 `--backlog true`，不额外运行持续流量六窗口；本轮为有限功能验收，不套用吞吐完成率统计。
