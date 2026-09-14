# 多世界实例池与双入口路由验收

起点：tx `8ff9bbb`、txgameslg `93e734f`、txclientslg `9f2d571`，六工程工作区干净。本轮复用既有真实集群编排和协议驱动，不增加业务协议、生产采集或自动运维控制器。

## 场景

- 原有世界 490001 完成三代正常交接，归属 slgworld-a-r2。
- 加入独立 slgworld-c；两个实例均配置 assignment 数量上限 1，不在启动时绑定世界身份。
- 只准备世界 490002 的 Mongo 数据，不创建它的归属；首次客户端请求触发 tx 选择有容量的 slgworld-c。
- 新世界访问完成后，490001 的 owner 与代次必须保持不变，490002 必须为 slgworld-c/第一代。
- 独立测试角色通过同一会话先访问 490002，再访问 490001。每个世界分别执行 Role→World 的 Core NATS 调用和 Agent→World 的 gRPC 直连，客户端通知事件携带 worldID/sourceInstance，由编排器对照实际归属，四组证据缺一或 owner 不匹配都失败。
- 最终两个世界分别以 `(49,85)` 与 `(61,92)` 写回 Mongo，检查原世界未被第二世界写入覆盖。摘要保存 worldPoolAssignments。

CLI 的可选 `--second-world` 只复用已有会话执行第二次世界调用，原 Session、Seq/Ack、断线恢复实现和生成协议不变。此场景验证跨世界寻址，不新增侦查玩法；两条路径分别由两个固定角色会话执行，各自会话跨越两个世界。

## 夹具修正

首轮复用前一阶段角色重新登录，旧离线 Session 仍参与 PubRole 扇出，客户端原有“单邮箱投递数量”断言失败。保留该正确会话行为，改为独立角色，并在同一会话里访问两个世界；没有删除旧 Session、缩减扇出范围或放宽断言。

失败日志保留：`/Users/a47/workspace/tx-artifacts/world-pool/20260914-101835.641146000`。

## 本轮验证

真实集群重跑通过，artifacts：`/Users/a47/workspace/tx-artifacts/world-pool/20260914-102314.543544000`。

| 世界 | 权威实例 | 代次 | NATS / gRPC 通知来源 |
| --- | --- | ---: | --- |
| 490001 | txgameslg-692-33719 | 3 | 两者均匹配 |
| 490002 | txgameslg-3293-33728 | 1 | 两者均匹配 |

- txgameslg/server 全量 `go test ./... -count=1` 通过，编排包 `go test -race ./internal/clustere2e -count=1` 通过。
- txclientslg `npm test` 全部 101 项通过，`npm run build` 通过；扩展既有测试验证同会话按顺序访问两个世界，新增 Go 证据校验覆盖错误 owner、缺少路径和业务拒绝。
- 后置 ponytail-review：Lean already. Ship. 复用现有驱动和世界夹具，无新增生产包装或传输实现；`git diff --check` 通过。
- 原始测试日志及 SHA256 位于上级 world-pool 目录。首轮 npm test 因 sandbox 禁止 tsx IPC socket 而未能执行，授权环境重跑通过。
- 本轮未重复 tx/txgame 回归和 txclient 构建，相关源码未修改；不引用历史结果冒充本轮验证。

## 边界与后续

自动等待新 Pod、选择目标并续作滚动交接的运维编排按用户决定后续完善。当前保留显式 PrepareHandoff/CAS 控制原语；新成员加入不能自动搬走已有归属。该事项不阻塞当前 NATS 默认后端迁移验证。

本轮不修改冻结 I2 基线或性能负载；没有 benchmark，不作性能收益声明。下一项为默认后端迁移清单收口，核对生产依赖与遗留入口，再评估 JetStream 的首个实施节点。
