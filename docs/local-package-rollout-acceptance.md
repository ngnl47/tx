# 当前存储版本的本地正式包与滚动验收

日期：2026-10-29，Mac Intel，Go 1.26.5。使用本机依赖，不提交，不跑云压测。

## 结论

三轮持续流量均达到 99.99%，持久化、配置更新/回滚和正常排水分别核验通过。完成率不是无损承诺；交接窗口仍有少量失败及延迟尖峰。

| 场景 | 连接数 | 测量窗口 | 请求数 | 完成数 | 失败数 | 完成率 | 成功请求 P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| txgame Game 发布/回滚 | 50 | 180s | 45,000 | 44,999 | 1 | 99.997778% | 1.383ms |
| txgameslg Role 发布/回滚 | 4 | 180s | 330,939 | 330,931 | 8 | 99.997583% | 3.120ms |
| txgameslg World 发布/回滚 | 4 | 180s | 1,169,019 | 1,169,011 | 8 | 99.999316% | 1.148ms |

txgame 使用每连接 5 次/s 的计划负载，未发送为 0；SLG 使用既有闭环角色改名和世界移动客户端，完成率以实际尝试数为分母。三者不作为同负载性能比较。所有超时均保留为失败，不重投、不以迟到结果抵消；成功请求分位数不包含超时。

程序通过既有 package.py 构建、解包到源码外目录，删除原打包目录并从独立 cwd 启动。每轮发布过程中二进制摘要不变，实际加载版本为 A→B→A。业务进程 GOMAXPROCS=2，驱动上限为 4；SLG 保留原夹具的 Debug 日志和进度输出，不用本轮数据推断容量或优化收益。

## 业务状态与生命周期证据

- txgame Role/Room 正式包：Role Ready→Unloaded→Ready，背包仍为 1；Room 运行 60 秒、收到 Ended(frame=60, actions=1) 后 Unloaded。五个业务进程正常排水退出。
- txgame 四次 Game 更新/回滚：分别耗时 1.776s、1.835s、2.386s、1.797s；每步核验 50 个角色的新归属。配置切换后、重启前仍使用旧快照；最终背包为道具 1×6、道具 2×3。这里证明跨进程重载后的业务存储连续性，不宣称最后一次 Fusion 更新已同步落 Mongo。
- 缺 JSON 文件和错误结构版本两个进程均拒绝启动，旧服务仍可领奖；正常进程退出 0，两例故意错误配置退出非零。
- SLG Role：主服放置槽位代次 1→2→3，重启取得新 InstanceID；4 个最终名称分别从 Redis/Fusion 与 Mongo 回读核对。两次交接后均观察到新增完成请求。
- SLG World：490001、490002 两个槽位均为 1→2→3；最终先排水保存，再核对 4 个测试单位的 Mongo 坐标和文档版本，4 个分区均通过，版本合计 16。全部 8 次正常进程启动均退出 0。
- World 交接后的只读 Mongo 快照包含两个世界各 4 个 Monster，位置均在各自网格中心半径 10 内，保存版本已推进；该快照是持久化观察，不是对运行内存的无锁读取。
- 指定 World 冷启动：World 1 Ready、重复 owner 非零拒绝且不改变归属、代次 1→2；交接前后两服两路径共 8 条完成通知都来自对应 owner，原 GDWorld 保持唯一且内容不变。无映射服务器 9999 在两阶段均被明确拒绝。
- 精确的 Monster 坐标恢复由真实 Mongo 的 `TestOwnedBucketsMongoRoundTrip` 核验；周期任务进邮箱、最终保存后重新加载保留位置。保存失败保留脏对象、拒绝替换激活、重试后释放由 `TestFinalSaveFailureRetainsOwnedWorldAndRetry` 核验，其失败注入为可控 Persistence 实现，不冒充真实数据库故障。

## 本轮发现与修复

SLG 首轮角色发布中，改名成功但排行榜共享资料更新持续报 `Redis store "rank" is not configured`。原因是旧集群模板没有跟上正式业务依赖。

主动中断首轮并保留现场，不计为通过；`TestClusterTemplateProvidesRankProfileStorage` 先复现 rank 为空，再将模板 rank 指向本轮专用 Redis DB13。修复后完整重跑角色与 World，未出现该错误。未修改框架、业务语义、超时或通过阈值。

## 隔离与输入

- 原本机 txgame_cluster_e2e Mongo 库含历史集合，没有修改；本轮新增 `tx-local-acceptance-mongo-20261029`，仅绑定 127.0.0.1:27029。
- 复用 Redis DB12/13 与既定 etcd 验收前缀，入口核验 ownership 标记。成功场景清理自身范围；中断首轮的日志和摘要保留，后续重跑由既有入口重置自身持有标记的数据。
- A/B 来源是当前已生成配置的临时副本，分别只启用道具 1/2；按现有规则重算文件摘要和整体数据版本，经过正式打包与加载器完整校验。策划 Excel、工程 JSON 和生成代码不变；本轮不包含 Excel/Luban 导出验证。

## 本地回归与清理

- txgameslg/server 全量 `go test ./... -count=1 -p=2 -json` 通过：386 个测试通过、9 个依赖开关用例跳过；跳过名称逐项列在机器记录，不计为通过。设置 `TX_MONGO_URI=mongodb://127.0.0.1:27029`，真实网格 Mongo 往返测试实际执行。
- `go test -race ./services/slgworld/modules/internal/world ./cmd/internal/clusterkit -count=1 -json` 通过：37 个测试、无跳过，覆盖网格持久化/恢复、保存失败保留与重试、排水和模板修复。均使用 Go 1.26.5。
- 收尾按本轮目录检查无残留进程；Redis DB12/13 均为空，etcd `/tx/e2e/` 下无键。临时 Mongo 容器已停止，保留容器数据供复查，没有删除原本机数据库或其他容器。

## 复现入口

根证据目录为 `/private/tmp/tx-local-acceptance-20261029`，配置副本分别位于 `configs/txgame/{a,b}` 和 `configs/txgameslg/{a,b}`。本地依赖需要先就绪。

```sh
# txgame/server；两个测试顺序运行，共用相同隔离端口。
GOTOOLCHAIN=go1.26.5 GOMAXPROCS=4 \
TXGAME_E2E_MONGO_URI=mongodb://127.0.0.1:27029/txgame_cluster_e2e \
TXGAME_ACTOR_LIFECYCLE=1 go test ./cmd/internal/deploytest \
  -run '^TestPackagedActorLifecycle$' -count=1 -v -timeout=6m

GOTOOLCHAIN=go1.26.5 GOMAXPROCS=4 TXGAME_CONFIG_TRAFFIC=1 \
TXGAME_E2E_MONGO_URI=mongodb://127.0.0.1:27029/txgame_cluster_e2e \
TXGAME_CONFIG_RELEASE_A=/private/tmp/tx-local-acceptance-20261029/configs/txgame/a \
TXGAME_CONFIG_RELEASE_B=/private/tmp/tx-local-acceptance-20261029/configs/txgame/b \
go test ./cmd/internal/deploytest -run '^TestConfigReleaseRollback$' -count=1 -v -timeout=10m

# txgameslg 根目录；角色与 World 分别运行，不能并行占用相同验收端口。
export GOTOOLCHAIN=go1.26.5 GOMAXPROCS=4
export TXGAMESLG_E2E_MONGO_URI=mongodb://127.0.0.1:27029/txgameslg_cluster_e2e
sh cmd/cluster-perf.sh --workload role-rename --concurrency 4 \
  --release-a /private/tmp/tx-local-acceptance-20261029/configs/txgameslg/a \
  --release-b /private/tmp/tx-local-acceptance-20261029/configs/txgameslg/b \
  --warmup 2s --duration 3m --timeout 10m
sh cmd/cluster-perf.sh --workload world-move-unit --world-ids 490001,490002 --concurrency 4 \
  --release-a /private/tmp/tx-local-acceptance-20261029/configs/txgameslg/a \
  --release-b /private/tmp/tx-local-acceptance-20261029/configs/txgameslg/b \
  --warmup 2s --duration 3m --timeout 10m
sh cmd/cluster-e2e.sh --world-startup --timeout 5m
```

## 验证边界与证据

当前版本及各轮原始摘要、进程退出码、数据版本和二进制摘要收录在 [机器可读记录](validation/local-package-rollout-20261029.json)。临时目录中的详细进程日志可能被系统清理，文档和 JSON 保留主要证据。

本轮没有云端容量、长期稳态、跨物理机容灾、意外杀进程恢复或全部子服务滚动升级验收。业务持续流量只滚动 Game/SLGGame/SLGWorld；Login、Agent、MsgQ 验证启动与最终排水。本地小规模完成率通过不更新历史万连 20/50 的容量结论。
