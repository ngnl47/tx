# Bee、Ant 与 Core 框架边界迁移实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在单一 tx 仓库和 Go module 内建立稳定 Core、无状态 Bee、有状态 Ant 三层边界，并把 txgame、txgameslg 分别迁移到对称的框架门面。

**Architecture:** Core 提供中立 Component Host 和可复用运行时组件；Bee、Ant 分别拥有 CMod、Server、ServiceRegistration 和配置门面。L4 Service 先声明静态依赖，CMod 启动后再由 Factory 获取强类型能力并构造业务 Service；Ant 额外提供 `CModProcedure`，组合 Procedure、Cache 和可选 WAL。

**Tech Stack:** Go 1.26 module、Go 1.27.0 本地工具链、WaitGroupContext、Redis、MongoDB、etcd、gRPC、ProtoActor、txgen 生成代码。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-27-bee-ant-framework-boundary-design.md`

## Global Constraints

- 不拆分 Git 仓库或 Go module；不提交 Git，由用户在阶段检查点手动提交。
- Core 不得 import Bee/Ant；Bee 与 Ant 不得互相 import。
- txgame 目标只 import Bee；txgameslg 目标只 import Ant；生成代码不 import Bee/Ant。
- Bee 与 Ant 的共有公开 API 同名同义；Ant 只增加 `CModProcedure`、逻辑槽位等有状态能力。
- CMod 相关公开符号统一使用 `CMod` 前缀；`StateRuntime` 目标名称固定为 `CModProcedure`。
- 不引入反射依赖注入、服务定位器、运行期插件或热路径 CModID 查询。
- Service Factory 之外不得长期保存 Bee/Ant Context；Handler、Procedure 和领域代码只保存项目强类型依赖。
- 每阶段保持受影响工程完整测试通过；错误默认失败，backend 波动返回 error，不 panic、不静默成功。
- 新增或修改的代码注释、GoDoc 和维护文档统一使用中文。
- 遵循剃刀原则；共用算法下沉 Core，Bee/Ant 只保留运行模型相关组合策略。

---

## Phase 0：基线与迁移护栏

### Task 1: 固定测试、性能和依赖基线

**Files:**
- Create: `/Users/a47/workspace/tx/docs/audits/2026-08-27-bee-ant-migration-baseline.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Test: `/Users/a47/workspace/tx/tests/framework_boundary_test.go`

**Interfaces:**
- Produces: 迁移前测试、benchmark、公开 API 和 import 闭包基线。
- Produces: `TestFrameworkPackagesRespectDependencyDirection` 的可复用 `go list` 检查器。

- [x] **Step 1: 记录四工程干净状态和 Go 版本**

运行：

```bash
cd /Users/a47/workspace/tx && git status --short && go version
cd /Users/a47/workspace/txgen && git status --short
cd /Users/a47/workspace/txgame && git status --short
cd /Users/a47/workspace/txgameslg && git status --short
```

预期：四个 `git status --short` 均为空，Go module 最低版本为 1.26。

- [x] **Step 2: 运行迁移前完整测试**

运行：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgen && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgame/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgameslg/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

预期：全部 PASS；失败项必须先修复或记录为既有问题，不能带入结构迁移。

- [x] **Step 3: 记录热点 benchmark 基线**

运行并把 `ns/op`、`B/op`、`allocs/op` 写入审计文档：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'StoreView|Fusion|Transaction|Cache|WAL|Actor' -benchmem -count=5
```

审计文档必须记录 CPU、Go 版本、命令和原始输出位置，不只写结论。

- [x] **Step 4: 先写架构依赖测试辅助函数**

在 `framework_boundary_test.go` 中实现只检查当前已存在包的基线版本：

```go
func packageImports(t *testing.T, pattern string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", pattern)
	cmd.Dir = filepath.Clean("..")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pattern, err)
	}
	return strings.Fields(string(output))
}
```

后续任务在 Bee/Ant 创建后扩展具体断言；此时不能引用尚不存在的 package。

- [x] **Step 5: 把框架拆分加入当前 P0 主线**

在 `docs/IMPLEMENTATION_PLAN.md` 的“当前主线”增加本规格与本计划链接，只记录尚未完成目标，不复制设计正文。

- [x] **Step 6: 检查阶段差异**

运行：

```bash
cd /Users/a47/workspace/tx && git diff --check
```

阶段建议 Commit Message：

```text
docs(architecture): 规划 Bee 与 Ant 框架边界迁移
```

---

## Phase 1：Core Component Host

### Task 2: 提取中立依赖图

**Files:**
- Create: `/Users/a47/workspace/tx/component/context.go`
- Create: `/Users/a47/workspace/tx/component/spec.go`
- Create: `/Users/a47/workspace/tx/component/graph.go`
- Create: `/Users/a47/workspace/tx/component/graph_test.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_graph.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_graph_test.go`

**Interfaces:**
- Produces: `component.ID`、`component.Spec`、`component.Component`。
- Produces: `component.Resolve([]Component, []Root) (Graph, error)`，Root 保留服务到组件的完整诊断路径。
- Produces: `Graph.IDs() []ID`、`Graph.Components() []Component`。
- Preserves: 现有 `CModID`、`CModSpec` 和错误行为，旧 Server 仅通过私有适配器委托。

- [x] **Step 1: 写依赖图失败测试**

覆盖重复 ID、空 ID、缺失依赖、Requires 环、After 环、稳定拓扑顺序和 After 不隐式启用：

```go
func TestResolveRejectsDependencyCycleWithPath(t *testing.T) {
	_, err := Resolve([]Component{
		testComponent("a", "b"),
		testComponent("b", "c"),
		testComponent("c", "a"),
	}, []ID{"a"})
	if err == nil || !strings.Contains(err.Error(), "a -> b -> c -> a") {
		t.Fatalf("cycle error = %v", err)
	}
}
```

- [x] **Step 2: 运行测试并确认缺少 component package**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./component -count=1
```

预期：因 package 或符号尚不存在而 FAIL。

- [x] **Step 3: 实现中立契约和稳定拓扑解析**

目标公开契约：

```go
type ID string

type Spec struct {
	ID       ID
	Requires []ID
	After    []ID
}

type Component interface {
	Spec() Spec
	Start(Context) error
}
```

`Context` 在本任务先定义最小只读契约：

```go
type Context interface {
	Root() context.Context
	Tasks() lifecycle.Tasks
}
```

`Resolve` 必须复制输入切片，不能让调用方后续修改破坏 Graph；错误包含完整组件路径。

- [x] **Step 4: 让旧 CMod 图委托 Core Graph**

`server/cmod_graph.go` 只保留 `CModID <-> component.ID` 和 `CModSpec <-> component.Spec` 转换，删除重复 DFS/排序算法。

- [x] **Step 5: 运行 Core 与旧 Server 图测试**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./component ./server -run 'Graph|CMod' -count=1
```

预期：全部 PASS，旧错误路径仍可诊断。

### Task 3: 提取 Component 生命周期宿主

**Files:**
- Create: `/Users/a47/workspace/tx/component/host.go`
- Create: `/Users/a47/workspace/tx/component/host_test.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_host.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_host_test.go`

**Interfaces:**
- Consumes: `component.Graph`、`lifecycle.WaitGroupContext`。
- Produces: `Validator`、`Drainer`、`Stopper`、`Host`。
- Produces: `Host.Validate()`、`Host.Start()`、`Host.Drain(context.Context)`、`Host.Stop(context.Context)`。

- [x] **Step 1: 写生命周期顺序和失败回滚测试**

```go
func TestHostRollsBackFailedStartInReverseOrder(t *testing.T) {
	events := make([]string, 0, 4)
	host := newTestHost(
		recordingComponent("a", &events),
		failingComponent("b", &events),
	)
	err := host.Start()
	if err == nil {
		t.Fatal("Start succeeded")
	}
	want := []string{"validate:a", "validate:b", "start:a", "start:b", "stop:a"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}
```

同时覆盖：全部 Validate 先于任何 Start、每个组件独立 Tasks、Drain/Stop 逆序、并发 Stop 共享结果、Stop 幂等、
Start 部分失败时等待失败组件已登记任务。

- [x] **Step 2: 实现最小 Host**

公开生命周期契约：

```go
type Validator interface { Validate(Context) error }
type Drainer interface { Drain(context.Context) error }
type Stopper interface { Stop(context.Context) error }
```

`Component.Start(Context)` 是必选生命周期；Validate、Drain、Stop 只在组件需要相应阶段时实现。`Context` 只提供只读根上下文
与当前组件的 `lifecycle.Tasks`，不提供能力查询。

- [x] **Step 3: 将旧 CMod Host 改为 Component 适配器**

适配器闭包捕获旧 `CModContext`，Core Host 不 import `server`：

```go
type cmodComponent struct {
	mod     CMod
	context CModContext
}
```

旧 Server 的启动、回滚和关闭测试必须继续通过。

- [x] **Step 4: 运行生命周期 race 测试**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test -race ./component ./server -run 'Host|Lifecycle|Start|Drain|Stop' -count=1
```

- [x] **Step 5: 运行 tx 全量测试**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

阶段建议 Commit Message：

```text
refactor(component): 提取中立依赖图和生命周期宿主
```

---

## Phase 2：Core 运行时组件去 Server 化

### Task 4: 提取存储运行时组件

**Files:**
- Create: `/Users/a47/workspace/tx/storage/runtime/config.go`
- Create: `/Users/a47/workspace/tx/storage/runtime/runtime.go`
- Create: `/Users/a47/workspace/tx/storage/runtime/health.go`
- Create: `/Users/a47/workspace/tx/storage/runtime/workers.go`
- Create: `/Users/a47/workspace/tx/storage/runtime/wal.go`
- Create: `/Users/a47/workspace/tx/storage/runtime/runtime_test.go`
- Modify: `/Users/a47/workspace/tx/server/storage.go`
- Modify: `/Users/a47/workspace/tx/server/storage_health.go`
- Modify: `/Users/a47/workspace/tx/server/storage_workers.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_redis.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_mongo.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_fusion.go`

**Interfaces:**
- Produces: `storageruntime.Runtime`，实现生成数据绑定和业务 facade 所需的 Reader/Writer 契约。
- Produces: `StartRedis`、`StartMongo`、`StartFusion`、`DrainFusion`、`CloseRedis`、`CloseMongo`。
- Produces: `BuildMongoWAL`、健康闸门和 Delay Scheduler 生命周期。
- Preserves: Fusion 任意 backend 不可用即拒绝读写、延迟任务本地重试、关闭排空和索引初始化语义。

- [x] **Step 1: 把 server 存储测试复制为 package 级失败测试**

迁移并改写现有健康、索引、worker、WAL 和失败契约测试，使测试只 import `storage/runtime`，不 import `server`。

- [x] **Step 2: 运行新 package 测试并确认失败**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./storage/runtime -count=1
```

- [x] **Step 3: 移动实现并消除 server 错误前缀**

存储错误改用 `storage runtime:` 前缀；不改变公开存储错误的 `errors.Is` 关系。Redis/Mongo client 所有权、健康 generation、
Fusion Dispatcher 和 Scheduler 保持在同一个 Runtime，避免 Bee/Ant 各自维护一份。

- [x] **Step 4: 让旧 Server CMod 使用 storageruntime.Runtime**

`server` 只负责把现有 Config 转换为 `storageruntime.Config` 并管理 CMod 顺序，不保留 backend 构建算法。

- [x] **Step 5: 运行存储定向与真实链路测试**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./storage/... ./server -count=1
```

使用 Redis `127.0.0.1:6379` 和 Mongo `127.0.0.1:27017` 运行现有 integration 测试；服务不可用测试只能返回 error，不能 panic。

### Task 5: 提取 Discovery、控制面和进程公共组件

**Files:**
- Create: `/Users/a47/workspace/tx/modules/discovery/runtime.go`
- Create: `/Users/a47/workspace/tx/modules/discovery/runtime_test.go`
- Create: `/Users/a47/workspace/tx/modules/control/server.go`
- Create: `/Users/a47/workspace/tx/modules/control/server_test.go`
- Modify: `/Users/a47/workspace/tx/server/discovery.go`
- Modify: `/Users/a47/workspace/tx/server/control.go`
- Modify: `/Users/a47/workspace/tx/server/slot_promoter.go`
- Modify: `/Users/a47/workspace/tx/server/assembly.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_rpc.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_actor.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_discovery.go`

**Interfaces:**
- Produces: `discovery.Runtime`，持有 Registrar、Directory、SlotStore 和 Watch 生命周期。
- Produces: `control.Server`，只依赖生命周期与可选槽位控制 SPI。
- Reuses: 现有 `rpc.Host`、`actor.Runtime`，不增加第二层传输封装。

- [x] **Step 1: 写 Discovery Runtime 所有权测试**

覆盖启动快照、Watch 重试、状态切换、关闭 lease、LogicalSlot CAS 和旧 watch 不覆盖新状态。

- [x] **Step 2: 写控制面独立测试**

控制面只依赖生命周期任务契约，以及现有的生命周期和可选槽位控制 SPI：

```go
type LifecycleController interface {
	BeginDrain(context.Context) error
	WaitDrained(context.Context) error
	Status() Status
}
```

不得 import Bee、Ant、Storage 或 Procedure。

- [x] **Step 3: 移动实现并让旧 Server 适配**

旧 `server` 保留配置转换和 CMod 组合；Watcher、slot promoter 和 gRPC control 实现进入 Core 模块。

- [x] **Step 4: 运行 etcd 与 race 回归**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test -race ./modules/discovery ./modules/control ./server -count=1
```

使用 etcd `127.0.0.1:2379` 运行集成测试。

- [x] **Step 5: 比较热点 benchmark**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'Fusion|Actor|Directory' -benchmem -count=5
```

阶段建议 Commit Message：

```text
refactor(runtime): 下沉存储和集群公共组件
```

---

## Phase 3：Bee 门面与 txgame

### Task 6: 建立 Bee 对称 API 和无状态 CMod

**Files:**
- Create: `/Users/a47/workspace/tx/bee/doc.go`
- Create: `/Users/a47/workspace/tx/bee/config.go`
- Create: `/Users/a47/workspace/tx/bee/cmod.go`
- Create: `/Users/a47/workspace/tx/bee/cmod_builtin.go`
- Create: `/Users/a47/workspace/tx/bee/context.go`
- Create: `/Users/a47/workspace/tx/bee/service.go`
- Create: `/Users/a47/workspace/tx/bee/builder.go`
- Create: `/Users/a47/workspace/tx/bee/server.go`
- Create: `/Users/a47/workspace/tx/bee/server_test.go`
- Create: `/Users/a47/workspace/tx/bee/service_test.go`
- Create: `/Users/a47/workspace/tx/modules/agent/service/service.go`
- Create: `/Users/a47/workspace/tx/modules/agent/service/service_test.go`
- Create: `/Users/a47/workspace/tx/modules/msgq/service/service.go`
- Create: `/Users/a47/workspace/tx/modules/msgq/service/service_test.go`
- Create: `/Users/a47/workspace/tx/modules/msgq/msgqredis/adapter.go`
- Create: `/Users/a47/workspace/tx/modules/msgq/msgqredis/adapter_test.go`
- Create: `/Users/a47/workspace/tx/bee/services/agent/registration.go`
- Create: `/Users/a47/workspace/tx/bee/services/agent/registration_test.go`
- Create: `/Users/a47/workspace/tx/bee/services/msgq/registration.go`
- Create: `/Users/a47/workspace/tx/bee/services/msgq/registration_test.go`

**Interfaces:**
- Produces: `bee.New(Config) *Builder`、`Builder.CMods`、`Builder.Services`、`Builder.Build`。
- Produces: `bee.Server.Start/Run/Drain/Stop`。
- Produces: `bee.CMod*`、`bee.ServiceRegistration`、Factory-scoped `bee.Context`。
- Consumes: Core Component Host、Storage Runtime、RPC、Discovery 和 Actor Runtime。

- [x] **Step 1: 写 Bee 公共 API 编译测试**

```go
func TestBuilderConstructsStatelessServer(t *testing.T) {
	registration := ServiceRegistration{
		Spec: ServiceSpec{Name: "game", Requires: []CModID{CModActor}},
		Factory: func(ctx Context) (Service, error) {
			messenger, err := ctx.CModActor()
			if err != nil {
				return nil, err
			}
			return &testService{actor: messenger}, nil
		},
	}
	server, err := New(testConfig()).Services(registration).Build()
	if err != nil || server == nil {
		t.Fatalf("Build() = %v, %v", server, err)
	}
}
```

- [x] **Step 2: 写 Factory 依赖越权测试**

Service 未声明 `CModActor` 时调用 `Context.CModActor()` 必须返回包含 Service 名和已选 CMod 集合的 error。

- [x] **Step 3: 实现 Bee CMod 和 Server**

共有生命周期 API：

```go
func New(Config) *Builder
func (*Builder) CMods(...CMod) *Builder
func (*Builder) Services(...ServiceRegistration) *Builder
func (*Builder) Build() (*Server, error)

func (*Server) Start(context.Context, RunOptions) error
func (*Server) Run(context.Context, RunOptions) error
func (*Server) Drain(context.Context) error
func (*Server) Stop(context.Context) error
```

ServiceRegistration 的 Factory 只在 CMod 全部启动后调用；Factory 失败按逆序清理已启动能力。

- [x] **Step 4: 验证 Bee 不暴露有状态能力**

用 `go/types` 检查 Bee 的导出符号中不存在 `Transaction`、`Procedure`、`StateRuntime`、`Checkpoint`、`LogicalSlot`。

- [x] **Step 5: 运行 Bee 生命周期和真实 backend 测试**

先把标准 agent/msgq 的监听、邮箱和 Redis adapter 下沉到上述 Core 中立包；这些包只依赖显式 Runtime 接口，不 import Bee、
Ant 或旧 Server。`bee/services/agent` 与 `bee/services/msgq` 只负责把 Bee Context 转换为中立 Runtime 并返回 Registration。

随后运行：

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test -race ./bee -count=1
```

### Task 7: 迁移 txgame 到 Bee

**Files:**
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/app/options.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/framework.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/services.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/db/db.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/service.go`
- Modify: `/Users/a47/workspace/txgame/server/services/agent/handlers.go`
- Modify: `/Users/a47/workspace/txgame/server/services/agent/virtual_resolver.go`
- Modify: `/Users/a47/workspace/txgame/server/cmd/server/main.go`
- Modify: corresponding `_test.go` files under the same packages

**Interfaces:**
- Consumes: Bee Builder、ServiceRegistration 和 Bee CMod accessors。
- Produces: `app.New(...) (*bee.Server, error)`。
- Preserves: agent -> Actor -> generated Handler -> msgq outbound 链路。

- [x] **Step 1: 先把服务测试改为 Registration 契约**

game 示例目标：

```go
func Registration() bee.ServiceRegistration {
	kind := mustRoleActorKind()
	return bee.ServiceRegistration{
		Spec: bee.ServiceSpec{
			Name: ServiceName,
			ActorKinds: []actor.Kind{kind},
			Requires: []bee.CModID{bee.CModRPC, bee.CModFusionStorage, bee.CModActor},
		},
		Factory: func(runtime bee.Context) (bee.Service, error) {
			messenger, err := runtime.CModActor()
			if err != nil {
				return nil, err
			}
			return New(Dependencies{Actor: messenger}), nil
		},
	}
}
```

- [x] **Step 2: 迁移项目配置转换**

debug/test/release YAML 模型不改变；`FrameworkBase`/`Framework` 改为输出 `bee.Config` 和 Bee 选择上下文。删除无状态项目中
Transaction、Cache、WAL checkpoint 和 LogicalSlot 的框架配置分支。

- [x] **Step 3: 迁移组合根和 db facade 安装**

生成 TypeRegistry 和项目 `db` 通过 Bee Builder 的 setup/typed storage hook 安装；业务侧继续只调用 `db.Save(role)`。

- [x] **Step 4: 禁止 txgame 直接 import 旧 Server/Ant**

在 txgame 测试中检查应用源码的直接 import：

```go
for _, imported := range packageImports(t, "./...") {
	if imported == "github.com/ngnl47/tx/server" || imported == "github.com/ngnl47/tx/ant" {
		t.Fatalf("txgame imports forbidden package %s", imported)
	}
}
```

- [x] **Step 5: 运行 txgame 完整和真实链路测试**

```bash
cd /Users/a47/workspace/txgame/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

使用 Redis、Mongo、etcd 验证启动、命令上行、Actor、Fusion 保存和 msgq 下行。

阶段建议 Commit Message：

```text
feat(bee): 建立无状态框架门面并迁移 txgame
```

---

## Phase 4：Ant、CModProcedure 与 txgameslg

### Task 8: 建立 Ant 对称 API

**Files:**
- Create: `/Users/a47/workspace/tx/ant/doc.go`
- Create: `/Users/a47/workspace/tx/ant/config.go`
- Create: `/Users/a47/workspace/tx/ant/cmod.go`
- Create: `/Users/a47/workspace/tx/ant/cmod_builtin.go`
- Create: `/Users/a47/workspace/tx/ant/context.go`
- Create: `/Users/a47/workspace/tx/ant/service.go`
- Create: `/Users/a47/workspace/tx/ant/builder.go`
- Create: `/Users/a47/workspace/tx/ant/server.go`
- Create: `/Users/a47/workspace/tx/ant/server_test.go`
- Create: `/Users/a47/workspace/tx/ant/services/agent/registration.go`
- Create: `/Users/a47/workspace/tx/ant/services/agent/registration_test.go`
- Create: `/Users/a47/workspace/tx/ant/services/msgq/registration.go`
- Create: `/Users/a47/workspace/tx/ant/services/msgq/registration_test.go`
- Modify: `/Users/a47/workspace/tx/tests/framework_boundary_test.go`

**Interfaces:**
- Produces: 与 Bee 同构的 Builder、Server、CMod、Context、ServiceRegistration。
- Adds: Ant LogicalSlot 配置和 accessor。

- [x] **Step 1: 写 Bee/Ant API 对称性测试**

使用 `go/types` 读取两个 package 的导出 scope；比较以下共有符号的方法集合和参数形态：

```go
var symmetricNames = []string{
	"Builder", "Config", "Context", "RunOptions", "Server",
	"CMod", "CModContext", "CModID", "CModSpec",
	"Service", "ServiceFactory", "ServiceRegistration", "ServiceSpec",
}
```

Ant 允许额外符号，但共有符号签名差异必须报告具体名称。

- [x] **Step 2: 实现 Ant 共有门面**

从相同 Core Component 构造，不复制 Storage/Actor/Discovery 算法。`CModID` 是 Ant 自己的强类型，不能 alias Bee 或
`component.ID`。标准 agent/msgq Registration 复用 Task 6 下沉的中立服务实现；Ant 与 Bee 的 Registration 构造函数和
Options 公开形态保持一致。

- [x] **Step 3: 验证生命周期行为一致**

对同一组 recording CMods/Services 分别运行 Bee、Ant；Start、Factory、Drain、Stop 事件序列必须相同。

### Task 9: 将 StateRuntime 收敛为 CModProcedure

**Files:**
- Create: `/Users/a47/workspace/tx/ant/cmod_procedure.go`
- Create: `/Users/a47/workspace/tx/ant/cmod_procedure_test.go`
- Modify: `/Users/a47/workspace/tx/server/state_runtime.go` during migration
- Modify: `/Users/a47/workspace/tx/server/state_runtime_test.go` during migration
- Test: `/Users/a47/workspace/tx/tests/txn_runtime_test.go`
- Test: `/Users/a47/workspace/tx/tests/cache_manager_test.go`
- Test: `/Users/a47/workspace/tx/wal/wal_benchmark_test.go`

**Interfaces:**
- Produces: `ant.CModProcedure`、`CModProcedureBuilder`、`CModProcedureContext`。
- Produces: `CModProcedureCache`、`CModProcedureWAL`。
- Produces: Factory 可取得的窄 Procedure facade，至少包含 `txn.Executor`。
- Preserves: Cache CommitParticipant、可选 WAL、按服务选择 WAL 和关闭排空。

- [x] **Step 1: 写命名和组件拓扑失败测试**

```go
func TestCModProcedureBuilderRequiresCache(t *testing.T) {
	mod, err := NewCModProcedureBuilder().Build()
	if err == nil || mod != nil {
		t.Fatalf("Build() = %v, %v", mod, err)
	}
}
```

覆盖 typed nil Cache/WAL、WAL 未命中服务不创建 backend、Mongo WAL 缺失 Mongo CMod、重复提交参与者和失败启动回滚。

- [x] **Step 2: 实现 CModProcedure**

```go
procedure, err := ant.NewCModProcedureBuilder().
	Cache(cacheComponent).
	WAL(ant.NewCModProcedureMongoWAL("slgworld")).
	Build()
```

启动顺序固定为 WAL（启用时）-> Transaction Runtime -> Cache -> Bind CommitParticipant；排水从 Cache 开始，停止时清理
Cache 引用和 WAL 所有权。CMod 不隐式选择 Mongo/Fusion。

- [x] **Step 3: 删除目标 API 中的 StateRuntime 命名**

Ant 及 txgameslg 新代码中不得出现 `StateRuntime`；旧 `server/state_runtime.go` 仅在 txgameslg 尚未迁移时保留。

- [x] **Step 4: 运行 Procedure/Cache/WAL race 和 benchmark**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test -race ./ant ./modules/txn ./modules/cache ./wal/... -count=1
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -run '^$' -bench 'Transaction|Cache|WAL' -benchmem -count=5
```

### Task 10: 迁移 txgameslg 到 Ant

**Files:**
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/options.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/framework.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/cachemgr/component.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/cachemgr/facade.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/cache_runtime.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/agent/handlers.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/agent/slot_resolver.go`
- Modify: `/Users/a47/workspace/txgameslg/server/cmd/server/main.go`
- Modify: corresponding `_test.go` files under the same packages

**Interfaces:**
- Consumes: Ant Builder、CModProcedure 和 ServiceRegistration。
- Produces: `app.New(...) (*ant.Server, error)`。
- Preserves: slggame Fusion 刷盘和 slgworld WAL/checkpoint 两条真实链路。

- [x] **Step 1: 把 cachemgr 组件改为 CModProcedure SPI**

`cachemgr.Component` 实现 `ant.CModProcedureCache`；组件工厂不再反向修改尚未构造的 Service。slgworld 的缓存运行时
构造器改为独立强类型工厂，由组合根同时交给 Cache 组件和 slgworld Registration：

```go
worldCacheFactory := slgworld.NewCacheRuntimeFactory(checkpointConfig)
cacheComponent := cachemgr.NewComponent(
	config.Cache,
	cachemgr.WithFactory(worldCacheFactory.Build),
)
```

Factory 在 CModProcedure 启动时创建 Runtime；slgworld Service Factory 在 CMod 全部就绪后通过该强类型工厂的
`Runtime() (*CacheRuntime, error)` 获取同一实例。不得使用全局 `Get(type)`、反射或业务 Service 回指针。

- [x] **Step 2: 迁移 slggame Registration**

Factory 从 `ant.Context.CModActor()` 和 `CModProcedure()` 取得 Messenger 与 `txn.Executor`，构造 Role Handler 后返回 Service。

- [x] **Step 3: 迁移 slgworld Registration**

Factory 取得 Actor、Procedure 和强类型 `*CacheRuntime`，构造 World Handler；`Service.Start` 不再读取框架 Context。

- [x] **Step 4: 迁移 agent/msgq 和组合根**

agent/msgq 使用与 Bee 对称的 Ant Registration API，但不声明 CModProcedure。项目 YAML 转换为 `ant.Config`；LogicalSlot
继续来自部署参数，不编码进 InstanceID。

- [x] **Step 5: 禁止 txgameslg 直接 import 旧 Server/Bee**

```go
for _, imported := range packageImports(t, "./...") {
	if imported == "github.com/ngnl47/tx/server" || imported == "github.com/ngnl47/tx/bee" {
		t.Fatalf("txgameslg imports forbidden package %s", imported)
	}
}
```

- [x] **Step 6: 运行完整和真实链路测试**

```bash
cd /Users/a47/workspace/txgameslg/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

必须真实验证：

```text
slggame:  command -> RoleActor -> Procedure -> Cache -> Fusion -> Redis/Mongo
slgworld: command -> WorldActor -> Procedure -> Cache -> WAL -> checkpoint -> Mongo
```

阶段建议 Commit Message：

```text
feat(ant): 建立有状态框架门面并迁移 txgameslg
```

---

## Phase 5：删除旧入口与文档收敛

### Task 11: 删除 tx/server 和所有过渡结构

**Files:**
- Delete: `/Users/a47/workspace/tx/server/*.go`
- Delete: `/Users/a47/workspace/tx/server/services/agent/**`
- Delete: `/Users/a47/workspace/tx/server/services/msgq/**`
- Delete: `/Users/a47/workspace/tx/server/services/dependency_test.go`
- Delete: `/Users/a47/workspace/tx/server/adapters/msgqredis/**`
- Modify: `/Users/a47/workspace/tx/tests/framework_boundary_test.go`
- Modify: any remaining test/import discovered by `rg 'github.com/ngnl47/tx/server'`

**Interfaces:**
- Removes: `server.Server`、`server.Context`、`server.CMod*`、`StateRuntime*`。
- Preserves: agent/msgq 核心实现下沉在 `modules/*`；Bee/Ant 只保存 Registration/CMod 组合。

- [x] **Step 1: 搜索全部旧入口引用**

```bash
cd /Users/a47/workspace && rg -n 'github.com/ngnl47/tx/server|StateRuntime|CModStateRuntime' tx txgen txgame txgameslg
```

除本迁移规格、实施计划和技术取舍归档外，目标代码与生效文档必须无匹配。

- [x] **Step 2: 移动 agent/msgq 组合入口**

Core 算法保留在 `modules/agent`、`modules/msgq`；Bee/Ant 分别提供对称 Registration。不得让 Ant import Bee 的服务实现。

- [x] **Step 3: 删除旧 package 和适配器**

删除旧 Server 状态机、巨型 Context、CMod 图包装和迁移期测试。若某个辅助函数仍被 Bee/Ant 同时需要，应先下沉到
Core 的准确职责包，不能为了消除编译错误复制两份。

- [x] **Step 4: 扩展最终架构测试**

断言：

```text
Core !-> bee/ant
bee !-> ant
ant !-> bee
txgame -> bee, !-> ant/server
txgameslg -> ant, !-> bee/server
txgen/generated -> Core, !-> bee/ant/server
```

- [x] **Step 5: 运行四工程全量测试**

```bash
cd /Users/a47/workspace/tx && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgen && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgame/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
cd /Users/a47/workspace/txgameslg/server && env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp go test ./... -count=1
```

### Task 12: 更新最终文档并完成架构审计

**Files:**
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgen/README.md`
- Modify: `/Users/a47/workspace/txgen/docs/design.md`
- Modify: `/Users/a47/workspace/txgame/README.md`
- Modify: `/Users/a47/workspace/txgameslg/README.md`
- Create: `/Users/a47/workspace/tx/docs/audits/2026-08-27-bee-ant-migration-review.md`

**Interfaces:**
- Documents: 最终 Core/Bee/Ant 架构和使用方式。
- Removes: 旧 Server、StateRuntime 和迁移过程说明。

- [x] **Step 1: 更新使用文档**

tx README 提供 Bee/Ant 并列最小示例；txgame 只展示 Bee；txgameslg 只展示 Ant 和 CModProcedure。使用文档不叙述
“从 server 改为 Bee/Ant”之类迁移历史。

- [x] **Step 2: 更新维护者 design 文档**

把目标规格中的以下内容整理进 `docs/design.md`：

```text
Core/Component/CMod/Service 分层
Bee 与 Ant 依赖方向
共有 API 对称性
ServiceRegistration 与强类型依赖注入
CModProcedure 调度链
配置、生命周期、错误和测试边界
```

只描述最终实现与必要技术取舍，不写旧包结构、旧名称或阶段迁移过程。

- [x] **Step 3: 更新 txgen 设计边界**

明确生成 GD/Store 代码保持运行模型中立；`@transaction` 只依赖 Core txn 契约，不依赖 Ant CMod。

- [x] **Step 4: 执行 P0/P1 架构审计**

按 `architecture_audit_standard.md` 检查公开 API、包依赖、类型断言、锁边界、不可变数据、错误降级和热路径分配，结果写入
迁移审计文档。P0/P1 finding 必须在本阶段解决；P2/P3 只记录，不扩大本轮范围。

- [x] **Step 5: 比较迁移前后 benchmark**

使用 Phase 0 相同命令和 `count=5`，逐项比较 `ns/op`、`B/op`、`allocs/op`。启动路径允许因更完整校验产生一次性开销；
Store、Actor、Fusion、Transaction、Cache、WAL 热路径不得出现无解释退化。

- [x] **Step 6: 最终验证**

```bash
cd /Users/a47/workspace/tx && git diff --check
cd /Users/a47/workspace/txgen && git diff --check
cd /Users/a47/workspace/txgame && git diff --check
cd /Users/a47/workspace/txgameslg && git diff --check
```

阶段建议 Commit Message：

```text
refactor(framework): 完成 Core Bee Ant 边界收敛
```

---

## 阶段执行规则

1. Phase 0-5 依次执行，不跨阶段并行修改同一包。
2. 每个 Task 先写失败测试，再实现，再运行定向测试和受影响工程全量测试。
3. 每个 Phase 完成后停止代码扩展，先审查 diff、测试和公开 API，再由用户决定是否提交。
4. 某阶段发现目标 API 需要改变时，先更新规格和本计划并重新确认，不用临时适配器掩盖设计问题。
5. 最终删除所有迁移期包装、旧测试和旧说明；Git 历史与本规格负责保存技术选型过程。
