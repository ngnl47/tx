# 本地可靠任务 demo

仅用于单机开发：Core NATS + JetStream、Redis、Mongo、etcd 使用专用 Compose 项目 `tx-task-demo`、端口与数据卷，不连接默认调试依赖。R1 + `sync_interval=always` 是可复现的演示配置，不是生产容量或高可用承诺。需要 Docker Compose、Python 3 与工程要求的 Go 版本。镜像首次启动可能需要下载；版本标签固定，严格镜像归档可另行固定 digest。

以下前三步在 **tx 根目录** 执行；示例目录可以是任意位置，不要求互相 import。

## 1. 启动依赖并预创建资源

```sh
docker compose -f deploy/task-demo/compose.yml up -d --wait --wait-timeout 90
go run ./cmd/task-resources
```

健康检查失败或端口冲突必须先处理，不跳过。端口固定为 NATS 14222、监控 18222、Redis 16379、Mongo 17017、etcd 12379，全部只绑定本机。不要用这套无认证演示配置暴露公网。

`tasks.json` 为 Game、SLG 分别定义三条文件流和两个 pull durable。先创建全部源流、死信流、耗尽通知流，再创建恢复消费者和任务消费者。任务最多投递 5 次，重试等待序列为 30s/1m/5m/15m/30m；恢复消费者无限重试。`workers` 是 demo durable 名称，与部署说明中的命名示例可以不同。

资源命令直接使用已有 NATS Go SDK，不依赖额外 NATS CLI。相同配置可重复执行；已有配置不同则报错，不调用 Update，不自动删除或回滚已创建资源。修正输入后可以重新执行。它是独立部署命令，不由 Bee/Ant 启动调用，不是生产业务 API。

## 2. 生成独立配置副本

```sh
python3 deploy/task-demo/prepare.py ../txgame /tmp/tx-demo-game-assets
python3 deploy/task-demo/prepare.py ../txgameslg /tmp/tx-demo-slg-assets
```

脚本读取各工程现有 `server/assets/debug`，复制资源并替换依赖端口、启用对应 taskQueue 配置，不修改原工程资产。输出目录必须不存在；输入配置不再是默认 `taskQueue: null` 时会拒绝处理。需要重新生成时换一个新目录，旧目录由使用者自行保留或删除。

## 3. 启动实际业务服务

分别在两个终端中运行（目录按本地 checkout 调整）：

```sh
cd ../txgame/server
env -u TXGAME_NATS_URL TXGAME_RPC_LISTEN=127.0.0.1:0 \
  go run ./cmd/server --assets-root /tmp/tx-demo-game-assets --services game
```

```sh
cd ../txgameslg/server
env -u TXGAMESLG_NATS_URL TXGAMESLG_RPC_LISTEN=127.0.0.1:0 \
  go run ./cmd/server --assets-root /tmp/tx-demo-slg-assets --services slgworld
```

这两个最小选择用于确认任务资源绑定；不会自动投递奖励，也不启动客户端 Login/Agent/MsgQ 全链路。业务在声明 CModTaskQueue 的工厂取得 Publisher，按项目生成命令协议投递。Game 和 SLG 使用不同源流与主题，不能互换生成命令。Ctrl+C 触发正常排水；若收尾失败保留依赖及日志，先排查，不立即删卷。

## 4. 向实际 Game 投递业务任务

Game 的配置副本会额外启用 `storageProbe` 的 Redis/Mongo 路由；原 debug 资产不变。上一版已生成的副本不会自动升级，请使用新目录重新生成。

在 txgame/server 执行，使用与目标 Game 相同的 demo 配置：

```sh
env -u TXGAME_NATS_URL go run ./cmd/task-demo \
  --config /tmp/tx-demo-game-assets/debug/app.yml \
  --role-id 930001 --name queued-demo
```

此命令临时启动一个只发布的 Bee 服务，取得正式 CModTaskQueue.Publisher，投递现有生成协议 System_StorageProbe 后退出，不注册 Role、不启动消费者、不手工拼接 tx 内部任务信封。可以先投递再启动 Game，任务持久等待消费。输出“已持久发布”只代表 PubAck，不代表业务成功；连接或发布失败不自动重试，避免把结果不确定的任务重复投递。

Game 启动后，等待 Fusion 存盘，再从 tx 根目录查询：

```sh
docker compose -f deploy/task-demo/compose.yml exec mongo \
  mongosh txgame --quiet --eval 'db.storage_probes.findOne({_id:NumberLong("930001")})'
```

结果中的 `n` 应为 `queued-demo`。`role-id` 必须在 Fusion JSON 安全整数范围 1..2^53-1。该命令只演示探针，不是奖励系统或通用任务管理工具；SLG 继续使用已有世界任务验收，不复制一个相同 CLI。

同一 Game 运行期间可重复演示，但任务没有去重保证。保留数据卷并重启 Game 后，旧稳定归属不会自动改派；需要按正式交接流程处理，或在确认 demo 数据可丢弃后按末节清理专用卷、从头启动。不要删除共享集群的归属来绕过校验。

## 5. 复用依赖运行关键验收

在对应 `server` 目录执行。测试使用随机流、namespace、etcd prefix；以下数据库专用于验收，不使用 demo 正式流。它们验证的是实际框架与业务路径，不向正在运行的 demo 实例注入任务。

Game：

```sh
TX_TASK_NATS_URL=nats://127.0.0.1:14222 \
TX_TASK_REDIS_URI='redis://127.0.0.1:16379?db=0' \
TX_TASK_MONGO_URI=mongodb://127.0.0.1:17017/tx_demo_game_tests \
TX_ETCD_ENDPOINT=127.0.0.1:12379 \
go test ./services/internal/actorcmd ./tests/integration \
  -run 'TestQueuedGeneratedCommandRetriesUntilHandlerCompletes|TestQueuedStorageProbeWritesFusion' -race -count=1 -v
```

SLG：

```sh
TX_TASK_NATS_URL=nats://127.0.0.1:14222 \
TX_TASK_REDIS_URI='redis://127.0.0.1:16379?db=0' \
TX_TASK_MONGO_URI=mongodb://127.0.0.1:17017/tx_demo_world_tests \
TX_ETCD_ENDPOINT=127.0.0.1:12379 \
go test ./tests/integration -run '^TestQueuedWorldMoveAcknowledgesBeforeMongoFlush$' -race -count=1 -v
```

断言保留：业务完成前不 ACK、临时错误重投；Fusion Redis 回读与 Mongo 存盘；世界 Procedure ACK 早于最终写回、Stop 后保存最新状态。解码、业务和持久化失败不能当成验收成功。Game 的公共准备逻辑仅在该工程 `internal/testsupport/taskqueue`，SLG 保持独立；两个工程均不依赖对方。

## 6. 日志与清理

失败时先保留现场，以下只读命令在 tx 根目录执行：

```sh
docker compose -f deploy/task-demo/compose.yml ps
docker compose -f deploy/task-demo/compose.yml logs --no-color > /tmp/tx-task-demo.log
```

停止依赖但保留数据：

```sh
docker compose -f deploy/task-demo/compose.yml stop
```

确认本次 demo 数据可以丢弃后，再清理该专用项目（不要在业务服务仍运行时执行）：

```sh
docker compose -f deploy/task-demo/compose.yml down --volumes
```

测试不跨工程统一清理：命令重投用例成功后校验 owner 再清理自身范围；GD 用例保留数据供检查。统一的 Compose 生命周期避免散落临时容器管理代码，成功后可整体回收此专用环境；失败不自动销毁。此前其他名称的测试容器和共享服务不属于本项目。
