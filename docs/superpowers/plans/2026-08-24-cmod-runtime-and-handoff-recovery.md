# CMod Runtime And Handoff Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 用 L3 `CMod` 依赖图替换能力位图和启用开关，按选中 L4 服务精确装配 L1/L2 资源，并用带 token fencing 的 Redis lease 协调 Fusion handoff 启动恢复。

**Architecture:** L0 保留纯算法和契约，L1 提供 Redis/Mongo/gRPC/etcd adapter，L2 提供 Fusion、RPC Host、Transaction Runtime 等组件，L3 `CMod` 封装这些组件并由 `server.Server` 做拓扑装配，L4 服务只声明 `CModID`。所有 CMod 先完成无副作用校验，再按稳定拓扑启动；停止时先排水 CMod，再取消根 `WaitGroupContext`，最后反向释放物理资源。

**Tech Stack:** Go 1.26、Redis Lua、go-redis/v9、Mongo Go Driver、gRPC、etcd、现有 `lifecycle.WaitGroupContext`。

**Spec:** `docs/superpowers/specs/2026-08-24-module-runtime-and-handoff-recovery-design.md`

## Global Constraints

- 不保留 `Capabilities`、`Capability*`、`backgroundWorkers` 或旧 YAML 路径兼容层。
- 公开架构术语统一为“能力模组（Capability Module，CMod）”；公开类型统一使用 `CMod*`。
- CMod 属于 L3；Fusion、RPC Host、Scheduler、Transaction Runtime 等实现属于 L2。
- `--services` 只选择 L4 服务；YAML 只提供参数，不决定服务或 CMod 是否启用。
- 未选中服务及其 CMod 闭包不得校验语义、建立连接、监听端口或启动 goroutine。
- 所有新增或修改的 Go 注释、GoDoc 和维护文档使用中文。
- 外部 Redis、Mongo、etcd、gRPC 故障返回错误，不得因为服务波动 panic。
- 未知状态默认失败；任何 handoff 恢复失败都不得伪装成启动成功。
- 不执行 `git commit`；由用户统一审核和提交。
- 测试使用 `/Users/a47/sdk/go1.26.5/bin/go`，并设置 `GOCACHE=/private/tmp/tx-go-build`、`GOTMPDIR=/private/tmp`。

---

### Task 1: 建立 CMod 契约和稳定依赖图

**Files:**
- Create: `server/cmod.go`
- Create: `server/cmod_graph.go`
- Create: `server/cmod_graph_test.go`
- Modify: `server/service.go`
- Modify: `server/registry.go`
- Modify: `server/service_test.go`

**Interfaces:**
- Produces: `CModID`、`CModSpec`、`CMod`、`CModValidator`、`CModDrainer`、`CModStopper`、`CModContext`。
- Produces: `ServiceSelection` 和 `CModRequirementsProvider.RequiredCMods(ServiceSelection) ([]CModID, error)`。
- Produces: `resolveCModGraph([]CMod, []Service) (cmodGraph, error)`，供 Server 装配和启动日志复用。

- [x] **Step 1: 为公开契约和依赖图写失败测试**

```go
func TestResolveCModGraphReturnsStableDependencyOrder(t *testing.T) {
	started := []CMod{
		fakeCMod{id: "rpc"},
		fakeCMod{id: "redis"},
		fakeCMod{id: "mongo"},
		fakeCMod{id: "fusion", requires: []CModID{"redis", "mongo"}},
		fakeCMod{id: "txn", requires: []CModID{"fusion"}},
	}
	graph, err := resolveCModGraph(started, []Service{fakeService{spec: Spec{
		Name: "game", Requires: []CModID{"rpc", "txn"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := graph.IDs(), []CModID{"rpc", "redis", "mongo", "fusion", "txn"}; !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}
```

同时覆盖空 ID、重复 ID、缺失依赖、循环路径、共享依赖去重、条件依赖只对选中服务求值。

- [x] **Step 2: 运行定向测试并确认因类型和解析器不存在而失败**

Run:

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestResolveCModGraph|TestCMod' -count=1
```

Expected: FAIL，错误包含未定义的 `CModID` 或 `resolveCModGraph`。

- [x] **Step 3: 实现 CMod 契约和无副作用图解析器**

```go
type CModID string

type CModSpec struct {
	ID       CModID
	Requires []CModID
}

type CMod interface {
	Spec() CModSpec
	Start(CModContext) error
}

type CModContext interface {
	AppID() string
	Instance() discovery.Instance
	Tasks() lifecycle.Tasks
	Storage() Storage
	WAL() wal.Store
	RPC() rpc.Registrar
	Directory() discovery.Directory
	Actor() actor.Messenger
	Transaction() txn.Executor
	HasCMod(CModID) bool
	Advanced() Advanced
}

type CModValidator interface {
	Validate(CModContext) error
}

type CModDrainer interface {
	Drain(context.Context) error
}

type CModStopper interface {
	Stop(context.Context) error
}

type Spec struct {
	Name     string
	Requires []CModID
}

type ServiceSelection struct {
	names map[string]struct{}
}

func (selection ServiceSelection) Has(name string) bool
func (selection ServiceSelection) Names() []string

type CModRequirementsProvider interface {
	RequiredCMods(ServiceSelection) ([]CModID, error)
}
```

图解析使用 DFS 三色状态并携带当前路径；同级节点按 CMod 注册顺序稳定输出。错误必须包含完整路径，例如：

```text
server: CMod dependency cycle: actor -> discovery -> actor
server: CMod "transaction" requires missing CMod "fusion-storage"
```

- [x] **Step 4: 运行 server 定向测试**

Run:

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestResolveCModGraph|TestCMod|TestRegistry' -count=1
```

Expected: PASS。

### Task 2: 实现 CMod 校验、启动、排水和反向停止宿主

**Files:**
- Create: `server/cmod_host.go`
- Create: `server/cmod_host_test.go`
- Modify: `server/server.go`
- Modify: `server/runtime.go`
- Modify: `server/context.go`
- Modify: `server/context_access.go`
- Modify: `server/service_host.go`
- Modify: `server/shutdown.go`
- Modify: `server/internal/servertest/context.go`

**Interfaces:**
- Consumes: Task 1 的 `cmodGraph`。
- Produces: `WithCMod(mods ...CMod) Option`。
- Produces: `Context.HasCMod(CModID) bool`，替换 `Context.Has(Capabilities)`。
- Produces: `cmodHost.validate/start/drain/stop`，由 `Server` 独占生命周期所有权。

- [x] **Step 1: 写宿主生命周期失败测试**

```go
func TestCModHostValidatesAllModulesBeforeStartingAny(t *testing.T) {
	events := []string{}
	host := newCModHost(testCModContext(), []CMod{
		&recordingCMod{id: "redis", events: &events},
		&recordingCMod{id: "fusion", validateErr: errors.New("invalid fusion config"), events: &events},
	})
	err := host.start()
	if err == nil {
		t.Fatal("start error = nil")
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want no starts before validation succeeds", events)
	}
}
```

补充启动失败仅停止已启动前序 CMod、Drain 反序、Stop 反序且幂等、`HasCMod` 只在成功启动后为真。

- [x] **Step 2: 运行测试确认失败**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestCModHost|TestServiceContextExposesCMods' -count=1
```

Expected: FAIL，缺少 `cmodHost` 和 `HasCMod`。

- [x] **Step 3: 实现宿主并接入 Server 骨架**

`Server.Start` 固定执行：选择服务 -> 解析 CMod 图 -> 创建根 lifecycle/runtime shell -> 校验全部 CMod -> 启动 CMod -> setup -> 服务。`Server.Stop` 固定执行：Drain 服务 -> Stop 服务 -> Drain CMod -> Cancel/Wait 根 lifecycle -> Stop CMod。

```go
func WithCMod(mods ...CMod) Option {
	return func(server *Server) error {
		server.cmods = append(server.cmods, mods...)
		return nil
	}
}
```

runtime 用集合记录已启动 CMod：

```go
type runtime struct {
	cmods map[CModID]struct{}
}

func (service *serviceContext) HasCMod(id CModID) bool {
	_, ok := service.runtime.cmods[id]
	return ok
}
```

- [x] **Step 4: 运行 server 生命周期测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestCModHost|TestServiceContext|TestServiceHost|TestServer' -count=1
```

Expected: PASS。

### Task 3: 将存储装配拆成 Redis、Mongo、Fusion、WAL 和 Scheduler CMod

**Files:**
- Create: `server/cmod_builtin.go`
- Create: `server/cmod_storage_redis.go`
- Create: `server/cmod_storage_mongo.go`
- Create: `server/cmod_storage_fusion.go`
- Create: `server/cmod_wal.go`
- Create: `server/cmod_scheduler.go`
- Create: `server/cmod_storage_test.go`
- Modify: `server/storage.go`
- Modify: `server/storage_health.go`
- Modify: `server/storage_workers.go`
- Modify: `server/config.go`
- Modify: `server/storage_workers_test.go`
- Modify: `server/storage_health_test.go`

**Interfaces:**
- Produces built-in IDs: `CModRedisStorage`、`CModMongoStorage`、`CModFusionStorage`、`CModWAL`、`CModDelayScheduler`。
- Produces private `builtinCMods(*Server, RunOptions) []CMod`。
- Reuses: `storageRuntime` as the private aggregate reached by `Context.Storage()`; it must support partial assembly without exposing partial internals to business handlers.

- [x] **Step 1: 写按闭包精确装配的失败测试**

测试用注入式 backend factory 记录创建次数：

```go
func TestRedisOnlyCModDoesNotCreateMongoOrFusion(t *testing.T) {
	factory := &recordingStorageFactory{}
	server := newServerForCModTest(t, factory, CModRedisStorage)
	if err := server.Start(context.Background(), RunOptions{Services: "msgq"}); err != nil {
		t.Fatal(err)
	}
	if factory.redis != 1 || factory.mongo != 0 || factory.fusion != 0 {
		t.Fatalf("created redis=%d mongo=%d fusion=%d", factory.redis, factory.mongo, factory.fusion)
	}
}
```

覆盖 Fusion 依赖 Redis+Mongo、WAL 只复用 Mongo、Scheduler 共享 Fusion 且只启动一次、generated registry 仅在 Fusion 闭包缺失时失败。

- [x] **Step 2: 运行测试确认现有全量 `newStorage` 行为失败**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestRedisOnlyCMod|TestFusionCMod|TestWALCMod|TestSchedulerCMod' -count=1
```

Expected: FAIL。

- [x] **Step 3: 拆分无副作用校验和有副作用启动**

内置依赖固定为：

```go
const (
	CModRedisStorage   CModID = "redis-storage"
	CModMongoStorage   CModID = "mongo-storage"
	CModFusionStorage  CModID = "fusion-storage"
	CModDelayScheduler CModID = "delay-scheduler"
	CModWAL            CModID = "wal"
)
```

`CModFusionStorage` 只组合已启动 Redis/Mongo L2 组件并创建 handoff producer/recoverer；`CModDelayScheduler` 只启动消费循环；`CModWAL` 只在被依赖时创建 WAL backend。删除 `startBackgroundWorkers` 的双 worker 聚合入口。

- [x] **Step 4: 将健康探针按已装配 backend 启动**

Redis CMod 只探测 Redis，Mongo CMod 只探测 Mongo；Fusion 写闸门继续要求两者都健康。探针仍由根 `WaitGroupContext` 管理，不在业务热路径 Ping。

- [x] **Step 5: 运行存储和 server 测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server ./tests -run 'Storage|Fusion|WAL|Scheduler|CMod' -count=1
```

Expected: PASS。

### Task 4: 将 RPC、Discovery、Actor 和 Transaction 收敛为内置 CMod

**Files:**
- Create: `server/cmod_rpc.go`
- Create: `server/cmod_discovery.go`
- Create: `server/cmod_actor.go`
- Create: `server/cmod_transaction.go`
- Create: `server/cmod_runtime_test.go`
- Modify: `server/assembly.go`
- Modify: `server/server.go`
- Modify: `server/shutdown.go`
- Modify: `server/discovery_test.go`
- Modify: `server/context_test.go`

**Interfaces:**
- Produces built-in IDs: `CModRPC`、`CModDiscovery`、`CModActor`、`CModTransaction`。
- `CModActor` requires `CModRPC` and `CModDiscovery`。
- `CModTransaction` requires `CModFusionStorage`。

- [x] **Step 1: 写内置 CMod 依赖和关闭顺序失败测试**

```go
func TestBuiltInCModsExposeDeclaredDependencies(t *testing.T) {
	want := map[CModID][]CModID{
		CModActor:       {CModRPC, CModDiscovery},
		CModTransaction: {CModFusionStorage},
	}
	for id, requires := range want {
		if got := builtInSpec(id).Requires; !slices.Equal(got, requires) {
			t.Fatalf("%s requires %v, want %v", id, got, requires)
		}
	}
}
```

覆盖 RPC Host 每进程一个、未选 Discovery 不创建 etcd client、Transaction 排水时 flush 且物理连接最后关闭。

- [x] **Step 2: 运行测试确认失败**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run 'TestBuiltInCMods|TestRPC.*CMod|TestTransaction.*CMod' -count=1
```

- [x] **Step 3: 用私有 CMod adapter 替换 `startDiscovery/startActor/startTransaction` 分支**

每个文件只负责一个 CMod。CMod 使用 `server.Context` 的受控能力或捕获的私有 runtime shell，不向业务暴露 client、关闭权或 owner 权限。删除 `runtime.capabilities` 及全部位运算。

- [x] **Step 4: 运行 server 全包测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server/... -count=1
```

Expected: PASS。

### Task 5: 迁移 L4 服务声明和条件依赖

**Files:**
- Modify: `server/services/agent/service.go`
- Modify: `server/services/agent/service_test.go`
- Modify: `server/services/msgq/service.go`
- Modify: `server/services/msgq/service_test.go`
- Modify: `server/services/dependency_test.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/service.go`
- Modify: `/Users/a47/workspace/txgame/server/tests/integration/server_runtime_test.go`

**Interfaces:**
- Consumes: `Spec.Requires []CModID` and `CModRequirementsProvider`。
- Produces: agent local-msgq/remote-msgq condition requirements without starting resources.

- [x] **Step 1: 写服务依赖失败测试**

```go
func TestAgentRequiresDiscoveryOnlyWithoutSelectedLocalMsgQ(t *testing.T) {
	service := configuredAgentForTest(t)
	local, err := service.RequiredCMods(NewServiceSelection("agent", "msgq"))
	if err != nil {
		t.Fatal(err)
	}
	remote, err := service.RequiredCMods(NewServiceSelection("agent"))
	if err != nil {
		t.Fatal(err)
	}
	assertCModIDs(t, local, CModRPC, CModRedisStorage)
	assertCModIDs(t, remote, CModRPC, CModRedisStorage, CModDiscovery)
}
```

- [x] **Step 2: 将服务声明迁移到 CMod**

```go
func (Service) Spec() server.Spec {
	return server.Spec{
		Name: ServiceName,
		Requires: []server.CModID{
			server.CModRPC,
			server.CModFusionStorage,
			server.CModTransaction,
			server.CModDelayScheduler,
		},
	}
}
```

agent 的 `RequiredCMods` 可以解析当前服务配置，但不得监听、拨号或启动 goroutine；它依据 `ServiceSelection.Has("msgq")` 决定使用本地 mailbox 还是 Discovery 路由。

- [x] **Step 3: 运行 tx 和 txgame 服务测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server/services/... -count=1
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./services/... ./tests/... -count=1
```

第二条命令工作目录：`/Users/a47/workspace/txgame/server`。Expected: PASS。

### Task 6: 重构配置分层并删除全部内置 enabled 开关

**Files:**
- Modify: `server/config.go`
- Modify: `server/config_test.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/model.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/framework.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/services.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/loader_test.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/framework_test.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/services_test.go`
- Modify: `/Users/a47/workspace/txgame/server/config/local/app.yml`

**Interfaces:**
- Produces: `RuntimeConfig.HandoffRecovery HandoffRecoveryConfig`。
- Removes: `RuntimeConfig.BackgroundWorkers`、`ActorConfig.Enabled`、`DiscoveryConfig.Enabled`、`WALConfig.Enabled`。
- Moves: txgame `runtime.agent` -> `services.agent`，`runtime.msgq` -> `services.msgq`。

- [x] **Step 1: 写严格 YAML 与延迟语义校验测试**

覆盖：未知字段始终失败；未选 agent 时无效 agent 参数不影响 framework base config；选中 agent 时错误包含 `services.agent.*`；没有 `enabled` 仍可由 CMod 闭包启动；旧路径直接作为未知字段失败。

- [x] **Step 2: 运行配置测试确认失败**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./server -run Config -count=1
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./internal/config -count=1
```

- [x] **Step 3: 实现新配置结构**

```go
type HandoffRecoveryConfig struct {
	StartupLeaseTTL     time.Duration
	StartupWaitTimeout  time.Duration
	MaxPendingPerSlot   int
	RetryInitial        time.Duration
	RetryMultiplier     float64
	RetryMax            time.Duration
	RetryJitter         float64
}
```

`Config.Validate` 只校验全局无条件契约；各内置 CMod 的 `Validate` 校验本模块配置。txgame 继续使用 `yaml.Decoder.KnownFields(true)`，服务 options provider 继续在服务被选中后转换。

- [x] **Step 4: 更新本地 YAML**

```yaml
runtime:
  handoffRecovery:
    startupLeaseTTL: 30s
    startupWaitTimeout: 2m
    maxPendingPerSlot: 1024
    retryInitialDelay: 1s
    retryMultiplier: 2
    retryMaxDelay: 1m
    retryJitter: 0.2

services:
  agent:
    tcpListen: 127.0.0.1:9000
    webSocketListen: 127.0.0.1:9001
    maxFrameBytes: 1048576
    fetchBatch: 1
  msgq:
    routeType: fusion
    prefix: txgame:msgq
    ttl: 48h
    maxBacklogMessages: 1024
    maxBacklogBytes: 8388608
    offlineTimeout: 60s
    sweepInterval: 2s
```

- [x] **Step 5: 运行配置与 txgame app 测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./internal/config ./internal/app -count=1
```

Expected: PASS。

### Task 7: 实现 Redis token-fenced 启动 lease

**Files:**
- Create: `storage/task/redis/startup_lease.go`
- Create: `storage/task/redis/startup_lease_test.go`
- Create: `storage/task/redis/scripts/startup_lease.lua`
- Modify: `storage/task/redis/script.go`
- Modify: `storage/task/redis/backend.go`
- Modify: `storage/redis/goredis/client_test.go`

**Interfaces:**
- Produces: `StartupLease` with `Acquire`、`Renew`、`Complete`、`Release`。
- Produces: `StartupLeaseStatusMissing`、`StartupLeaseStatusAcquired`、`StartupLeaseStatusRunning`、`StartupLeaseStatusComplete`。
- Keeps: Redis command and key format hidden below Fusion runtime.

- [x] **Step 1: 写 token fencing 失败测试**

```go
func TestStartupLeaseRejectsStaleOwner(t *testing.T) {
	lease := newStartupLeaseForTest(t)
	status, err := lease.Acquire(context.Background(), "token-a", time.Second)
	if err != nil || status != StartupLeaseStatusAcquired {
		t.Fatalf("acquire = %v, %v", status, err)
	}
	if ok, err := lease.Renew(context.Background(), "token-b", time.Second); err != nil || ok {
		t.Fatalf("stale renew = %v, %v", ok, err)
	}
	if ok, err := lease.Complete(context.Background(), "token-b", time.Second); err != nil || ok {
		t.Fatalf("stale complete = %v, %v", ok, err)
	}
}
```

覆盖 running/complete/过期接管、旧 token 不能续租/完成/释放、未知 Lua 返回值失败。

- [x] **Step 2: 运行测试确认失败**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./storage/task/redis -run StartupLease -count=1
```

- [x] **Step 3: 用单个 Lua 状态机实现四种操作**

Redis value 只允许：

```text
running:<token>
complete:<token>
```

所有续租、完成和释放都必须比较完整 value；`Complete` 保留 TTL，不能写永久 key。key 从 delay task prefix 派生，不由业务传入裸 Redis key。

- [x] **Step 4: 运行 Redis adapter 测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./storage/task/redis ./storage/redis/goredis -count=1
```

Expected: PASS。

### Task 8: 将启动 lease 接入 HandoffRecoverer 和 Fusion CMod

**Files:**
- Modify: `storage/fusion/handoff_recovery.go`
- Modify: `tests/fusion_handoff_recovery_test.go`
- Modify: `server/cmod_storage_fusion.go`
- Modify: `server/config.go`
- Modify: `/Users/a47/workspace/txgame/server/tests/integration/fusion_handoff_test.go`

**Interfaces:**
- Consumes: Task 7 `StartupLease`。
- Produces: coordinated startup recovery; runtime targeted retry loop remains local and does not scan slots.

- [x] **Step 1: 写十实例竞争和接管失败测试**

```go
func TestHandoffRecovererStartupLeaseAllowsOneFullTraversal(t *testing.T) {
	lease := newMemoryStartupLease()
	store := &countingHandoffStore{}
	var group sync.WaitGroup
	for range 10 {
		group.Add(1)
		go func() {
			defer group.Done()
			recoverer := newRecovererWithLease(store, lease)
			if err := recoverer.Start(testLifecycle(t)); err != nil {
				t.Errorf("start: %v", err)
			}
		}()
	}
	group.Wait()
	if got := store.fullTraversals.Load(); got != 1 {
		t.Fatalf("full traversals = %d, want 1", got)
	}
}
```

覆盖持有者扫描失败不写 complete、续租 token 丢失取消扫描、等待超时失败、complete 观察者跳过扫描、运行期 `RunPendingOnce` 不触发 slot 遍历。

- [x] **Step 2: 实现协调恢复**

`Start` 流程：竞争 lease；获得者每 `TTL/3` 续租并执行 `RecoverStartup`；成功后 CAS complete；失败时仅释放自己的 running token；观察者等待 complete 或 lease 过期后重新竞争；总等待受 `startupWaitTimeout` 限制。

- [x] **Step 3: 运行 Fusion 定向测试和 race**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./tests -run HandoffRecoverer -count=1
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test -race ./tests -run HandoffRecoverer -count=1
```

- [x] **Step 4: 使用真实 Redis 6379 验证 fencing 和接管**

```bash
TX_REDIS_ENDPOINT=redis://127.0.0.1:6379 env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./tests -run HandoffRecovererStartupRecoveryIntegration -count=1
```

Expected: PASS；测试清理自己的唯一前缀。

### Task 9: 更新组合根、启动日志和使用文档

**Files:**
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app_test.go`
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `docs/architecture_audit_standard.md`
- Modify: `docs/implementation_plan.md`
- Modify: `/Users/a47/workspace/txgame/README.md`

**Interfaces:**
- Produces: `server.Start` 成功后仅输出一次解析后的服务集合和稳定 CMod 装配顺序。
- Produces: 控制面不暴露动态装载或远程依赖图查询。

- [x] **Step 1: 写启动日志失败测试**

```go
func TestStartLogsResolvedServicesAndCModsOnce(t *testing.T) {
	// Services 输入为 "*"，日志必须输出解析后的实际服务名和稳定 CMod 顺序，且只记录一次。
}
```

- [x] **Step 2: 实现启动期可观测性**

启动日志直接使用 Task 1 的图解析结果，不复制拓扑算法。CMod 不支持运行期动态装载，控制面不增加静态图查询接口。

- [x] **Step 3: 更新 README 新手示例**

至少包含：

```go
appServer, err := server.New(
	framework,
	server.WithGeneratedData(data.DefaultTypeRegistry(), data.RegisterStorageRuntime),
	server.WithService(gameService, msgqService, agentService),
)

err = appServer.Run(ctx, server.RunOptions{
	Services:   "game,msgq",
	InstanceID: "game-1",
})
```

解释 `--services=agent`、`msgq`、`game`、`game,msgq` 各自展开的 CMod 闭包和实际连接资源；说明 Handoff startup lease 与 Scheduler task lease 的区别。

- [x] **Step 4: 更新维护者设计文档**

固定 L0-L4、H0-H2、P0-P3 三套术语；删除旧 Capability、enabled 和 backgroundWorkers 说明。补 CMod 依赖图、校验/启动/排水/停止时序、配置延迟校验边界和 Redis lease fencing 时序。

- [x] **Step 5: 运行文档符号扫描和 CLI 测试**

```bash
rg -n 'Capability[A-Z]|Capabilities|backgroundWorkers|runtime\.agent|runtime\.msgq|\bModuleID\b|\bMod[A-Z]' README.md docs server /Users/a47/workspace/txgame/README.md /Users/a47/workspace/txgame/server
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./cmd/txctl -count=1
```

Expected: `rg` 只允许迁移说明中明确标注的旧名称；CLI tests PASS。

### Task 10: 全量回归、真实链路和瘦身审计

**Files:**
- Modify only if verification exposes defects: files owned by Tasks 1-9
- Delete: obsolete capability/background worker helpers and tests made unreachable by the new design

**Interfaces:**
- Verifies: no compatibility bridge or duplicate lifecycle remains.

- [x] **Step 1: 运行 tx 全量测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

Working directory: `/Users/a47/workspace/tx`。Expected: PASS。

- [x] **Step 2: 运行 txgame 全量测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

Working directory: `/Users/a47/workspace/txgame/server`。Expected: PASS。

- [x] **Step 3: 运行 txgen 回归测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

Working directory: `/Users/a47/workspace/txgen`。Expected: PASS。

- [x] **Step 4: 运行真实 Fusion 链路**

使用 Redis `127.0.0.1:6379` 和 Mongo `127.0.0.1:27017` 执行 txgame integration tests，确认 game 闭包装配 Fusion、handoff、Scheduler 和 Transaction；agent/msgq 单独启动不会连接 Mongo。

- [x] **Step 5: 运行关键 race 测试**

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test -race ./server/... ./tests -run 'CMod|Handoff|Server|Scheduler' -count=1
```

Expected: PASS，无数据竞争。

- [x] **Step 6: 做过渡层和命名瘦身审计**

删除 `requiredCapabilities`、`runtime.capabilities`、`startBackgroundWorkers`、旧 enabled 分支、旧测试 fake 字段和仅为迁移存在的 adapter。公开名称只保留 `CMod*`；私有局部实现可以使用 `graph`、`host`、`registry` 等职责名，不机械添加前缀。

- [x] **Step 7: 输出阶段报告和提交信息，不执行提交**

报告必须按功能点给出实际示例：不同 `--services` 的 CMod 展开、未选 Mongo 不连接、十实例启动只扫描一次 handoff、旧 lease owner 不能覆盖新 owner。建议提交信息：

```text
refactor(server): 引入 CMod 依赖图并协调 Fusion 启动恢复

以 L3 能力模组替换固定能力位图，按服务依赖闭包精确装配运行时资源。
使用 Redis token fencing lease 合并批量启动时的 handoff 全量恢复，并收敛配置与生命周期边界。
```
