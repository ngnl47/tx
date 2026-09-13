# 完整上行对照夹具

此目录由 `build.py` 映入 `actortrial` 的测试包，复用既有 `TestRPCComparisonProcess` 和采样驱动。普通 `go test ./...`、正式构建和仓库 `go.mod` 均不依赖 `txgame-server`。临时 modfile 只为读取相邻 txgame 的原始生成包；不编辑或复制生成物，不让 tx 生产代码反向依赖示例。

I2 数据采集后，用户已因转发流量成本排除上行也经 NATS。后续仅运行 `uplink-proto` 和 `uplink-hybrid`；`uplink-nats` 代码及数据只保留为历史证据，不纳入后续验证矩阵。

```bash
python3 internal/actortrial/testdata/uplink/build.py /private/tmp/tx-uplink/build
python3 internal/actortrial/testdata/uplink/build.py /private/tmp/tx-uplink/wire --wire
```

默认生成 linux/amd64 无 CGO 测试二进制；本机验证可加 `--goos darwin`，race 构建另加 `--race`。`--wire` 只通过临时 Go overlay 统计 TCP Read/Write/Writev 字节，不改工具链文件或模块缓存；禁止用该二进制测吞吐或判断性能退化。

每个业务角色都是独立进程，通过 `TX_RPC_COMPARISON` JSON 指定：

- `Backend`：`uplink-proto`、`uplink-hybrid`、`uplink-nats`。hybrid 的 Agent 上行使用既有 `rpc.Sender`，Actor 间调用 NATS；nats 的两段都走 NATS。
- `Mode`：依次启动 `msgq`、`world`、`game`、`agent`、`client`；进程输出 `Ready` 后再组装下游。
- `Namespace`：本次专属、各进程一致的命名空间。`URL`、`Etcd`、`Redis` 指向编排器拥有的专用依赖。
- `MsgQ`、`World`、`Target`：填写前一阶段的 Ready；game 需要 MsgQ/World，agent 需要 MsgQ/Target(game)，client 需要 Target(agent) 和 Sessions。
- `Sessions`：测试客户端总连接数，编号相邻的两个 Session 属于同一角色。采样命令 `Entities` 必须等于 Sessions，`Payload` 是 BON 内 padding 大小，至少 128 字节；不把它称为整个线上信封大小。

采样命令沿用 `warm/run/gc/snapshot/samples/stop`；game 额外接受 `{"Action":"verify","Requests":累计完成数}`，执行 Flush、通过生成 Load 回读每个根及四个子字段，并核对累计事务数。先完成资源快照，再取 samples 和 verify。`--wire` 构建额外接受 `{"Action":"wire"}`，输出每个本地端点→远端端点的 `[读字节, 写字节]` 累计量。当前 gRPC 的 RawConn 读取绕过 net.Read，因此 gRPC 链路取两端 Write/Writev；不可把读计数 0 当成无流量。etcd 控制面不在字节合计内。

链路为 TCP/BON → Agent Session/MsgQ Rec → Actor → Procedure/生成 GD → 按 Session 错开相位、每十条上行一次 world RPC → MsgQ PubSession/FetchSome → TCP 下行与 ACK。客户端收到精确 ACK 及当前请求对应的下行才计完成，下一次上行捎带下行确认。每个 Session 同时最多一个请求；跨 Session 的同角色请求由 Actor 邮箱串行。均匀身份只是一种负载，不覆盖热点、重连或多会话广播容量。

事务使用 txgame 现有 Redis 类型 `ResRankItem`，预置 16 个 `ResourceRank` 子项；每次修改一个根字段及四个子项。writeback 每 100ms 写 Redis，完成 ACK 对应内存提交和下行入队，最终 Redis 核验另外计量。Redis AOF、Mongo/Fusion 和大世界计算不在此夹具范围内。

成员与 assignment 是固定的测试快照，使用现有 SlotResolver；不把此性能布局当作正式 framework 接线或真实租约验收。后者由 I1 及正式接线计划覆盖。初始化握手、首次建档、最终 Flush/回读均排除在稳态吞吐窗口之外。

本次隔离编排脚本、版本/hash、原始样本、独立汇总和失败日志见 [I2 报告](../../../../docs/audits/2026-09-13-uplink-business-comparison.md)。依赖由编排器创建并用随机 label 标明 ownership；不得用共享开发 Redis 执行本夹具建档、清空或销毁依赖。
