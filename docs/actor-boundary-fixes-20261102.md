# Actor 归属边界收敛回归（2026-11-02）

本轮在此前三批未提交变更上继续修复，范围以[专项计划第 8 节](actor-routing-lifecycle-plan.md#8-整体审查后收敛2026-11-02)为准。未提交 Git，未跑云测试或持续流量压测。

## 实现与保留边界

| 位置 | 本轮变化 |
| --- | --- |
| `modules/discovery/slot_etcd.go` | 权威 Create/Promote 原子复核目标成员 Starting/Running、归属和容量；包装存储不再改变校验行为 |
| `modules/discovery/slot_directory.go`、Runtime/Advanced | 缓存只暴露 SlotReader；写入使用权威存储或受限认领能力 |
| `internal/actorrpc/ownership.go` | Probe 对缺失、正常释放返回 Unloaded，不触发分配或激活；不可用等错误仍保留 |
| `modules/discovery/local_assignments.go`、Ant 能力视图 | Claim 绑定本次 InstanceID；只允许首次、本机幂等、正常释放或显式确认旧 owner 后认领；未知写入结果不重试 |
| SLG `services/slgworld/bootstrap.go` | 应用仅映射 WorldID、认领本机槽位、加载业务 World；移除自连控制面、手算 generation 和 Watch 轮询 |
| `internal/actorhost/placement.go` | 删除 tests-only 成员哈希 Placement；现行 WithPlacement、GroupResolver、Identity→SlotID 缓存完整保留 |

```text
WorldID → 应用映射 SlotID
  → CModLocalAssignments.Claim（绑定本实例）
      → 权威读取 → 校验旧 owner → Create/Promote CAS
      → 带 revision 读取 → 合并本机缓存 → 核对 assignment
  → World.Load → 业务启动完成
```

Claim 不证明旧 owner 已停止，不提供基于失联的自动抢占。Watch 目录不可用时仍失败关闭；回填仅同步本机，不要求全体进程同时确认。World/Room 业务生命周期仍独立于通信 Actor。

## 复现与回归

修复前在本地 etcd 复现两项失败：

- `TestEtcdSlotWritesValidateMemberThroughWrapper`：成员缓存仍显示 Running，权威成员已 Draining，经包装后的存储仍错误地允许创建。
- `TestProbeReleasedSlotIsUnloaded`：正常释放被当作 `ErrSlotReleased` 返回，而非 Unloaded。

修复后上述测试通过；认领测试覆盖首次、幂等、正常释放、旧 owner 精确确认、冲突、缺失旧记录和写入结果未知。认领测试刻意不启动 Watch，确认 Claim 返回前已正确回填缓存。

本机既有 etcd、NATS、Redis、MongoDB 可达。命令统一使用 `GOCACHE=/private/tmp/tx-codex-go-build`：

```sh
# tx：启用现有 etcd/NATS/Redis 集成场景
export GOCACHE=/private/tmp/tx-codex-go-build
export TX_ETCD_ENDPOINT=127.0.0.1:2379
export TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222
export TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379
go test ./... -count=1 -timeout=3m

# 同组依赖环境下的相关六包竞态回归
go test -race ./modules/discovery ./internal/framework ./internal/actorrpc \
  ./internal/actorhost ./internal/actorplacement ./ant -count=1 -timeout=3m

# 分别在 txgame/server、txgameslg/server
go test ./... -count=1 -timeout=4m
```

结果：三组全包测试、相关六包 race 均通过。SLG 的 `TestWorldStartupLoadsPersistentIdentity` 不再设置控制端口，覆盖正式装配加载、通信 Actor 保持 Unloaded、Mongo 四格初始化及 tick 后的最终保存。旧 Placement 测试中有效的排水、旧句柄拒绝、清理期间不得重建语义由现行生命周期用例保留；NATS 场景改用真实槽位与 BeginDrain 路径。

## 正式包有限请求验收

复用 `txgameslg/cmd/cluster-e2e.sh --world-startup --timeout 5m`，程序脱离源码目录启动。

首轮业务启动和客户端请求正常，重复认领进程正确非零退出；夹具仍匹配旧业务报错文本，因此失败。仅将断言更新为 `discovery.ErrSlotConflict.Error()`，保留非零退出与权威归属未变检查，随后重跑通过。

- 指定 World 1：旧 owner generation 1 → 正常释放 generation 2 → 新 owner generation 3。
- 重复 owner 拒绝；启动冲突未改变原 assignment。
- 重启前后客户端直达 World、Role→World 共 8 条最终通知均通过，缺失 World 映射各拒绝一次。
- GDWorld 身份文档前后一致；正常关闭与专用数据清理完成。
- 10 个正常进程退出码 0，1 个预期冲突进程退出码 1；独立进程检查未发现本轮残留服务。

此场景不是持续流量或吞吐结论，也不替代百万级 World 恢复验收。

## 缓存命中微基准

Intel i9-9880H、darwin/amd64，`-benchtime=500ms -benchmem -count=3`：

| 路径 | ns/op 范围 | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SlotResolver 缓存读取 | 137.3–138.4 | 0 | 0 |
| EntityRouter 共享缓存寻址 | 249.8–254.6 | 8 | 1 |

这是当前构建的本机读路径测量，不是前后版本严格对照，不据此宣称吞吐提升。缓存读取不新增权威查询；首次认领和写入校验属于冷路径。

## 证据与审查

本机原始日志及首轮失败证据保留于 `/Users/a47/workspace/tx-artifacts/actor-boundary-fixes-20261102/`：

- `tx-regression.log`、`tx-race.log`、两组应用 `*-regression.log`。
- `cache-benchmark.log`、`world-startup.log`、`world-startup-rerun.log`。
- 成功正式包目录：`world-startup/20261102-124056.573385000/`。

本轮经过独立 ponytail-review，包含接口调用者、权限边界、版本合并、异常启动及旧测试等价覆盖；修正验收断言后追加检查未发现需修复项。未重新评审此前全部历史变更，未宣称全仓 race 或云端容量验收通过。
