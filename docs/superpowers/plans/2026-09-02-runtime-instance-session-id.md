# 运行实例身份与 SessionID 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 tx 内建立可复用的 L1 etcd 基建，让所有 Discovery 节点自动取得运行实例身份，并只向显式选择 `CModSessionID` 的 Login 提供 53 位 SessionID 分配能力。

**Architecture:** `github.com/ngnl47/tx/etcd` 只包装 etcd 客户端、键值、Watch 和租约原语；`modules/discovery` 负责 workSlot、InstanceID、实例目录与 LogicalSlot 领域语义；`sessionid` 只实现 29/12/12 位段算法。`CModDiscovery` 取得并冻结运行身份，`CModSessionID` 依赖 Discovery 并通过 Bee/Ant 窄接口暴露分配器。

**Tech Stack:** Go 1.26、`go.etcd.io/etcd/client/v3` 3.7.1、Bee、Ant、标准库 `context`、`sync`、`sync/atomic`、`time`。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-09-02-runtime-instance-session-id-design.md`

## Global Constraints

- 不执行 `git commit`；每个任务只保留可审核差异，最终由维护者提交。
- 新增或修改的注释、GoDoc 和维护说明使用中文。
- 不保留人工 `InstanceID` 兼容入口。
- 不扫描 Redis，也不通过无边界 etcd 键扫描分配 workSlot。
- `tx/etcd` 不认识 InstanceID、workSlot、SessionID、Discovery JSON 或 LogicalSlot。
- ProtoActor 官方 etcd Cluster Provider 保持独立，不改接 `tx/etcd`。
- workSlot 固定为 12 位 `[0, 4095]`；同进程多服务共享一个 workSlot 与 InstanceID。
- SessionID 固定为 29 位秒时间戳、12 位 workSlot、12 位秒内序列，纪元为 `2026-01-01T00:00:00Z`。
- 时间戳低 29 位自然回绕；不实现纪元耗尽，不把 SessionID 当作永久主键或时间排序值。
- 所有新增行为先写失败测试，再写最小实现；每个任务完成后运行聚焦测试与 `git diff --check`。

---

### Task 1: L1 etcd 客户端与键值原语

**Files:**
- Create: `/Users/a47/workspace/tx/etcd/config.go`
- Create: `/Users/a47/workspace/tx/etcd/client.go`
- Create: `/Users/a47/workspace/tx/etcd/client_test.go`
- Create: `/Users/a47/workspace/tx/etcd/client_integration_test.go`

**Interfaces:**
- Consumes: `clientv3.Client`、`clientv3.KV`、`clientv3.Watcher`。
- Produces:

```go
package etcd

type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
}

type KeyValue struct {
	Key            string
	Value          []byte
	CreateRevision int64
	ModRevision    int64
	LeaseID        int64
}

type Snapshot struct {
	Revision int64
	Values   []KeyValue
}

type EventType uint8

const (
	EventPut EventType = iota + 1
	EventDelete
)

type Event struct {
	Type  EventType
	Value KeyValue
}

type WatchResponse struct {
	Events []Event
	Err    error
}

func New(context.Context, Config) (*Client, error)
func NewWithClient(*clientv3.Client, bool) (*Client, error)
func (*Client) Close() error
func (*Client) Probe(context.Context) error
func (*Client) Get(context.Context, string) (KeyValue, bool, error)
func (*Client) Snapshot(context.Context, string) (Snapshot, error)
func (*Client) WatchPrefix(context.Context, string, int64) <-chan WatchResponse
func (*Client) Put(context.Context, string, []byte, int64) error
func (*Client) Delete(context.Context, string) error
func (*Client) Create(context.Context, string, []byte, int64) (KeyValue, bool, error)
func (*Client) CompareAndSwap(context.Context, string, int64, []byte) (bool, error)
```

- `NewWithClient(client, closeClient)` 只用于高级组合与测试；`closeClient=false` 时 `Close` 不关闭外部 client。
- `WatchPrefix` 负责把底层关闭、压缩和服务端错误统一写入最后一个 `WatchResponse.Err`，随后关闭结果通道。

- [x] **Step 1: Write the failing configuration and ownership tests**

```go
func TestConfigNormalizeClonesEndpoints(t *testing.T) {
	input := []string{"127.0.0.1:2379"}
	config, err := (Config{Endpoints: input}).Normalize()
	if err != nil { t.Fatal(err) }
	input[0] = "changed"
	if config.Endpoints[0] != "127.0.0.1:2379" { t.Fatal("endpoints were not cloned") }
}

func TestNewWithClientRejectsNilClient(t *testing.T) {
	if _, err := NewWithClient(nil, false); !errors.Is(err, ErrClientNil) {
		t.Fatalf("error = %v, want ErrClientNil", err)
	}
}
```

- [x] **Step 2: Run the package tests and verify the new package is missing**

Run: `go test ./etcd`

Expected: FAIL because package `github.com/ngnl47/tx/etcd` does not exist.

- [x] **Step 3: Implement Config, Client ownership and immutable value conversion**

```go
var (
	ErrInvalidConfig = errors.New("etcd: invalid config")
	ErrClientNil     = errors.New("etcd: client is nil")
	ErrWatchClosed   = errors.New("etcd: watch closed")
)

type Client struct {
	raw       *clientv3.Client
	closeRaw  bool
	closeOnce sync.Once
	closeErr  error
}
```

`KeyValue.Value`、`Snapshot.Values` 和 Watch 事件必须复制底层字节，不能把 etcd 响应缓冲区暴露给上层。

`Probe` 使用带调用方截止时间的只读限量查询验证当前客户端能访问 etcd 多数派，不创建探测键。

- [x] **Step 4: Add real etcd key, CAS and Watch tests**

```go
func TestClientCreateCompareAndSwapAndWatch(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" { t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test") }
	client := newIntegrationClient(t, endpoint)
	key := fmt.Sprintf("/tx/tests/etcd/client/%d", time.Now().UnixNano())
	created, ok, err := client.Create(context.Background(), key, []byte("v1"), 0)
	if err != nil || !ok || created.ModRevision == 0 { t.Fatalf("create = %#v, %t, %v", created, ok, err) }
	if ok, err := client.CompareAndSwap(context.Background(), key, created.ModRevision, []byte("v2")); err != nil || !ok {
		t.Fatalf("compare and swap = %t, %v", ok, err)
	}
}
```

同一集成测试还要从 `created.ModRevision+1` 启动 Watch，验证 Put、Delete 事件顺序以及取消后通道关闭。

同文件定义以下测试辅助函数，所有资源都通过 `t.Cleanup` 释放：

```go
func newIntegrationClient(t *testing.T, endpoint string) *Client
func uniqueIntegrationKey(t *testing.T, suffix string) string
func waitWatchResponse(t *testing.T, responses <-chan WatchResponse) WatchResponse
```

另加 `TestClientProbe`，验证真实 etcd 可达时成功、已取消上下文返回 `context.Canceled`。

- [x] **Step 5: Run focused tests**

Run: `go test ./etcd`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./etcd -run 'TestClient' -count=1`

Expected: PASS；未提供环境变量时只跳过真实 etcd 用例。

- [x] **Step 6: Check the task diff without committing**

Run: `gofmt -w etcd/*.go`

Run: `git diff --check`

Expected: no output.

---

### Task 2: L1 etcd 租约生命周期

**Files:**
- Create: `/Users/a47/workspace/tx/etcd/lease.go`
- Create: `/Users/a47/workspace/tx/etcd/lease_test.go`
- Modify: `/Users/a47/workspace/tx/etcd/client_integration_test.go`

**Interfaces:**
- Consumes: Task 1 `Client.Put`、`Client.Create`。
- Produces:

```go
var (
	ErrLeaseLost    = errors.New("etcd: lease lost")
	ErrLeaseStopped = errors.New("etcd: lease stopped")
)

func (*Client) Grant(context.Context, time.Duration) (*Lease, error)
func (*Lease) ID() int64
func (*Lease) Done() <-chan struct{}
func (*Lease) Err() error
func (*Lease) Put(context.Context, string, []byte) error
func (*Lease) Create(context.Context, string, []byte) (KeyValue, bool, error)
func (*Lease) Stop() error
func (*Lease) Revoke(context.Context) error
```

- `Stop` 只停止续租，让键在 TTL 后自然过期。
- `Revoke` 主动撤销租约，用于尚未进入 running 的失败启动。
- 两者幂等；`Done` 只关闭一次，`Err` 返回最终原因。

- [x] **Step 1: Write failing lease state tests**

```go
func TestLeaseStopIsIdempotent(t *testing.T) {
	lease := newLeaseStateForTest()
	if err := lease.Stop(); err != nil { t.Fatal(err) }
	if err := lease.Stop(); err != nil { t.Fatal(err) }
	<-lease.Done()
	if !errors.Is(lease.Err(), ErrLeaseStopped) { t.Fatalf("error = %v", lease.Err()) }
}
```

测试替身只验证状态机；不得为生产 API 暴露任意租约状态修改入口。同一个 `_test.go` 文件定义：

```go
func newLeaseStateForTest() *Lease
```

该函数只初始化 cancel、done 和同步字段，不连接 etcd。

- [x] **Step 2: Run tests to verify failure**

Run: `go test ./etcd -run TestLease`

Expected: FAIL because `Lease` and its state machine do not exist.

- [x] **Step 3: Implement Grant, KeepAlive, Stop and Revoke**

```go
type Lease struct {
	client   *Client
	id       clientv3.LeaseID
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	doneOnce sync.Once
	doneErr  error
}
```

`Grant` 必须在返回前启动 `KeepAlive`；续租响应通道意外关闭时记录 `ErrLeaseLost`。调用 `Stop` 或 `Revoke` 时先写入明确的正常结束原因，不能被后台续租协程覆盖。

- [x] **Step 4: Add real etcd natural-expiry and revoke tests**

```go
func TestLeaseStopExpiresKeyWithoutImmediateRevoke(t *testing.T) {
	client := integrationClientFromEnv(t)
	lease, err := client.Grant(context.Background(), 2*time.Second)
	if err != nil { t.Fatal(err) }
	key := uniqueIntegrationKey(t, "lease-stop")
	if err := lease.Put(context.Background(), key, []byte("value")); err != nil { t.Fatal(err) }
	if err := lease.Stop(); err != nil { t.Fatal(err) }
	assertKeyExists(t, client, key)
	assertKeyEventuallyMissing(t, client, key, 4*time.Second)
}
```

并列用例验证 `Revoke` 后键立即消失，以及客户端连接中断导致 `Done` 关闭。

复用 Task 1 的 `newIntegrationClient` 与 `uniqueIntegrationKey`，并在同文件定义：

```go
func integrationClientFromEnv(t *testing.T) *Client
func assertKeyExists(t *testing.T, client *Client, key string)
func assertKeyEventuallyMissing(t *testing.T, client *Client, key string, timeout time.Duration)
```

- [x] **Step 5: Run focused tests and format**

Run: `go test ./etcd`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./etcd -run 'TestLease' -count=1`

Run: `gofmt -w etcd/*.go && git diff --check`

Expected: PASS and no diff errors.

---

### Task 3: Discovery 迁移到 L1 etcd

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/discovery/etcd_config.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/etcd.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/slot_etcd.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/endpoints_test.go`
- Modify: `/Users/a47/workspace/tx/tests/discovery_etcd_config_test.go`
- Modify: `/Users/a47/workspace/tx/tests/discovery_etcd_integration_test.go`

**Interfaces:**
- Consumes: Task 1 `etcd.Client` and Task 2 `etcd.Lease`。
- Produces: behavior-compatible `EtcdRegistry`、实例目录 Watch 与 `EtcdSlotStore`，但 `modules/discovery` 不再直接 import `clientv3` 或 `mvccpb`。

- [ ] **Step 1: Add an architecture boundary test**

```go
func TestDiscoveryDoesNotImportEtcdDriver(t *testing.T) {
	for _, path := range []string{"../modules/discovery/etcd.go", "../modules/discovery/slot_etcd.go"} {
		content, err := os.ReadFile(path)
		if err != nil { t.Fatal(err) }
		if bytes.Contains(content, []byte("go.etcd.io/etcd/client/v3")) {
			t.Fatalf("%s imports the etcd driver directly", path)
		}
	}
}
```

- [ ] **Step 2: Run the boundary and existing discovery tests to verify failure baseline**

Run: `go test ./tests -run 'TestDiscoveryDoesNotImportEtcdDriver|TestEtcdConfig|TestEtcdSlot'`

Expected: new boundary test FAIL; existing behavior tests PASS.

- [ ] **Step 3: Replace raw driver calls with L1 methods**

```go
type EtcdRegistry struct {
	client *txetcd.Client
	config EtcdConfig
}

func (c EtcdConfig) clientConfig() txetcd.Config {
	return txetcd.Config{Endpoints: append([]string(nil), c.Endpoints...), DialTimeout: c.DialTimeout}
}
```

`EtcdRegistry.Watch` 和 `EtcdSlotStore.Watch` 消费 `WatchResponse`；`CreateSlot` 使用 `Client.Create(..., leaseID=0)`；`PromoteSlot` 使用 `Get + CompareAndSwap`。Discovery 继续负责 JSON 校验和领域错误映射。

- [ ] **Step 4: Preserve prefix and logical-slot behavior**

实例前缀继续归一化为 `<root>/instances`，LogicalSlot 继续使用 `<root>/slots`。本任务不增加 workSlot 键，也不改变注册接口。

- [ ] **Step 5: Run focused and real integration tests**

Run: `go test ./modules/discovery ./tests -run 'TestEtcd|TestDiscovery'`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./tests -run 'TestEtcd' -count=1`

Expected: PASS；架构测试确认 Discovery 不再直接依赖 etcd driver。

- [ ] **Step 6: Check the task diff without committing**

Run: `gofmt -w modules/discovery/*.go tests/discovery_etcd_config_test.go tests/discovery_etcd_integration_test.go`

Run: `git diff --check`

Expected: no output.

---

### Task 4: workSlot 与自动 InstanceID

**Files:**
- Create: `/Users/a47/workspace/tx/modules/discovery/identity.go`
- Create: `/Users/a47/workspace/tx/modules/discovery/identity_test.go`
- Create: `/Users/a47/workspace/tx/modules/discovery/identity_integration_test.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/etcd.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/runtime.go`
- Modify: `/Users/a47/workspace/tx/modules/discovery/runtime_test.go`
- Modify: `/Users/a47/workspace/tx/tests/discovery_etcd_integration_test.go`

**Interfaces:**
- Consumes: Task 2 `Lease.Create`、`Lease.Put`、`Lease.Stop`、`Lease.Revoke`。
- Produces:

```go
const WorkSlotCount = 1 << 12

var (
	ErrWorkSlotExhausted   = errors.New("discovery: workSlot exhausted")
	ErrRuntimeClosed       = errors.New("discovery: runtime closed")
	ErrRuntimeIdentityLost = errors.New("discovery: runtime identity lost")
)

type RuntimeIdentity struct {
	InstanceID string
	WorkSlot   uint16
	ModRevision int64
}

func NewRuntimeWithIdentity(context.Context, EtcdConfig, string, Instance) (*Runtime, error)
func (*Runtime) Identity() RuntimeIdentity
func (*Runtime) Instance() Instance
func (*Runtime) Done() <-chan struct{}
func (*Runtime) Err() error
```

`NewRuntimeWithIdentity` 第三个参数是 `AppID`，第四个参数是 ID 为空的实例描述；函数内部填充 ID 后再执行既有 `Instance.Validate`。现有 `NewRuntime(ctx, config, instance)` 只作为 Task 4 到 Task 6 之间的短暂编译迁移入口，Task 6 完成 framework 调用方迁移后必须删除，最终 API 使用 `NewRuntime(ctx, config, appID, descriptor)`。

- [ ] **Step 1: Write failing pure identity tests**

```go
func TestRuntimeIdentityFormatsInstanceID(t *testing.T) {
	identity, err := newRuntimeIdentity("txgame", 17, 291)
	if err != nil { t.Fatal(err) }
	if identity.InstanceID != "txgame-17-291" { t.Fatalf("identity = %#v", identity) }
}

func TestWorkSlotAttemptsWrapWithinBoundedRange(t *testing.T) {
	got := workSlotAttempts(4094)
	if len(got) != WorkSlotCount || got[0] != 4094 || got[1] != 4095 || got[2] != 0 {
		t.Fatalf("attempts = %v", got[:3])
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./modules/discovery -run 'TestRuntimeIdentity|TestWorkSlot'`

Expected: FAIL because identity helpers do not exist.

- [ ] **Step 3: Implement bounded workSlot claim**

```go
func (r *EtcdRegistry) claimWorkSlot(ctx context.Context, lease *txetcd.Lease, appID string, start uint16) (RuntimeIdentity, error) {
	for offset := 0; offset < WorkSlotCount; offset++ {
		slot := uint16((int(start) + offset) & (WorkSlotCount - 1))
		created, ok, err := lease.Create(ctx, r.workSlotKey(slot), []byte(appID))
		if err != nil { return RuntimeIdentity{}, err }
		if ok { return newRuntimeIdentity(appID, slot, created.ModRevision) }
	}
	return RuntimeIdentity{}, ErrWorkSlotExhausted
}
```

生产入口从随机起点调用该函数；单元测试直接传入固定起点。不得列出 workSlot 前缀后在内存挑选空位。

- [ ] **Step 4: Make registration own one shared process lease**

注册顺序固定为：Grant lease → Claim workSlot → build InstanceID → Put instance with same lease → KeepAlive already active。实例键为 `<root>/instances/<instance-id>`，workSlot 键为 `<root>/work-slots/<slot>`。

```go
type EtcdRegistration struct {
	registry *EtcdRegistry
	lease    *txetcd.Lease
	identity RuntimeIdentity
	instance Instance
	running  bool
	// 既有并发关闭状态保留。
}
```

`SetStatus(StatusRunning)` 后 `running=true`。`Close` 删除实例键；未曾 running 的失败启动调用 `Revoke`，已经 running 的实例调用 `Stop` 让 workSlot 自然过期。

- [ ] **Step 5: Expose immutable runtime identity and loss signal**

`Runtime.Done`、`Runtime.Err` 把注册租约状态映射为 Discovery 领域错误：正常关闭返回 `ErrRuntimeClosed`，意外续租终止返回 `ErrRuntimeIdentityLost` 并保留底层原因。`Identity`、`Instance` 返回值副本。Watch 中断仍走原有目录失效和重建，不关闭 `Done`。

新路径使用 `EtcdRegistry.RegisterRuntime(ctx, appID, descriptor)`；旧 `Register(ctx, instance)` 在本任务内保持原行为以保证 framework 尚未迁移时仍可编译。Task 6 删除旧方法并把 `RegisterRuntime` 收敛为最终 `Register`。

- [ ] **Step 6: Add real concurrent claim, reuse and close tests**

```go
func TestConcurrentRuntimeClaimsUseDifferentWorkSlots(t *testing.T) {
	prefix := uniqueDiscoveryPrefix(t, "concurrent-runtime")
	first := newIntegrationRuntime(t, "txgame", prefix)
	second := newIntegrationRuntime(t, "txgame", prefix)
	if first.Identity().WorkSlot == second.Identity().WorkSlot { t.Fatal("workSlot duplicated") }
	if first.Instance().ID == second.Instance().ID { t.Fatal("InstanceID duplicated") }
}
```

`tests/discovery_etcd_integration_test.go` 定义：

```go
func uniqueDiscoveryPrefix(t *testing.T, suffix string) string
func newIntegrationRuntime(t *testing.T, appID, prefix string) *discovery.Runtime
func waitWorkSlotReleased(t *testing.T, client *clientv3.Client, key string, timeout time.Duration)
```

辅助函数使用当前测试独立 prefix，并通过 `t.Cleanup` 关闭 Runtime；它不向生产包增加测试入口。

补充以下真实 etcd 断言：

- running 实例关闭后实例键立即删除，workSlot 键在 TTL 内仍存在，TTL 后消失。
- 新进程复用同一 workSlot 时 `ModRevision` 和 InstanceID 均变化。
- 4096 个明确键预占后返回 `ErrWorkSlotExhausted`，不得扫描其他前缀。
- 两个不同 Discovery prefix 可以取得相同 workSlot，明确其属于两个独立唯一性域。

- [ ] **Step 7: Run focused tests**

Run: `go test ./modules/discovery`

Run: `go test ./...`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./tests -run 'TestEtcd.*Runtime|TestConcurrentRuntime' -count=1`

Run: `gofmt -w modules/discovery/*.go tests/discovery_etcd_integration_test.go && git diff --check`

Expected: PASS and no diff errors.

---

### Task 5: 53 位 SessionID 生成器

**Files:**
- Create: `/Users/a47/workspace/tx/sessionid/generator.go`
- Create: `/Users/a47/workspace/tx/sessionid/generator_test.go`

**Interfaces:**
- Consumes: `context.Context` and a validated 12-bit workSlot。
- Produces:

```go
package sessionid

const (
	EpochUnix    int64 = 1767225600
	MaxSessionID int64 = 1<<53 - 1
)

var (
	ErrInvalidWorkSlot = errors.New("sessionid: invalid workSlot")
	ErrClockBeforeEpoch = errors.New("sessionid: clock is before epoch")
)

type Allocator interface {
	AllocateSessionID(context.Context) (int64, error)
}

func New(workSlot uint16) (*Generator, error)
func (*Generator) AllocateSessionID(context.Context) (int64, error)
```

- [ ] **Step 1: Write failing layout and boundary tests**

```go
func TestGeneratorBuildsJavaScriptSafeID(t *testing.T) {
	generator := newGeneratorForTest(4095, time.Unix(EpochUnix+(1<<29)-1, 0))
	id, err := generator.AllocateSessionID(context.Background())
	if err != nil { t.Fatal(err) }
	if id <= 0 || id > MaxSessionID { t.Fatalf("id = %d", id) }
}

func TestGeneratorWrapsTimestampBits(t *testing.T) {
	clock := newFakeClock(time.Unix(EpochUnix+(1<<29)-1, 0))
	generator := newGeneratorWithClock(17, clock.Now, clock.Wait)
	last, _ := generator.AllocateSessionID(context.Background())
	clock.Set(time.Unix(EpochUnix+(1<<29), 0))
	first, _ := generator.AllocateSessionID(context.Background())
	if timestampBits(last) != (1<<29)-1 || timestampBits(first) != 0 { t.Fatalf("ids = %d, %d", last, first) }
}
```

同时添加：全零组合返回 1、不同 workSlot 不重复、同秒 4096 个序列唯一、序列耗尽等待下一秒、时钟回退等待追平、纪元前拒绝、并发分配无重复。

测试文件使用 package `sessionid`，定义以下非生产辅助函数：

```go
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(time.Time) *fakeClock
func (*fakeClock) Now() time.Time
func (*fakeClock) Set(time.Time)
func (*fakeClock) Wait(context.Context, time.Duration) error
func newGeneratorForTest(uint16, time.Time) *Generator
func newGeneratorWithClock(uint16, func() time.Time, func(context.Context, time.Duration) error) *Generator
func timestampBits(int64) int64
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./sessionid`

Expected: FAIL because package does not exist.

- [ ] **Step 3: Implement the minimal synchronized generator**

```go
type Generator struct {
	mu           sync.Mutex
	workSlot     uint16
	lastSecond   int64
	nextSequence uint16
	now          func() time.Time
	wait         func(context.Context, time.Duration) error
}
```

计算必须使用：

```go
elapsed := rawSecond - EpochUnix
timestamp := elapsed & ((1 << 29) - 1)
id := (timestamp << 24) | (int64(g.workSlot) << 12) | int64(sequence)
```

序列按原始 Unix 秒重置。`rawSecond < lastSecond` 时等待时钟追平；同秒序列耗尽时等待下一秒；等待必须响应调用上下文。ID 为零时消耗 sequence 0 并继续返回 sequence 1。

- [ ] **Step 4: Run race and focused tests**

Run: `go test -race ./sessionid`

Expected: PASS with no duplicate ID and no race report.

- [ ] **Step 5: Check the task diff without committing**

Run: `gofmt -w sessionid/*.go && git diff --check`

Expected: no output.

---

### Task 6: Framework 自动运行身份与失效关闭

**Files:**
- Modify: `/Users/a47/workspace/tx/internal/framework/assembly.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/cmod_discovery.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/discovery.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/runtime.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/server.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/shutdown.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/discovery_test.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/server_lifecycle_test.go`
- Modify: `/Users/a47/workspace/tx/tests/discovery_etcd_integration_test.go`

**Interfaces:**
- Consumes: Task 4 `discovery.NewRuntimeWithIdentity(ctx, config, appID, descriptor)`、`Runtime.Instance`、`Runtime.Done`、`Runtime.Err`。
- Produces: Discovery CMod 启动后冻结的自动 InstanceID，以及 workSlot 所有权失效触发的整进程排水关闭。

- [ ] **Step 1: Write failing automatic identity tests**

```go
func TestDiscoveryCModDoesNotRequireRunOptionInstanceID(t *testing.T) {
	server := &Server{config: Config{AppID: "txgame", Discovery: validDiscoveryConfig()}}
	mod := server.discoveryCMod(RunOptions{})
	if validator, ok := mod.(CModValidator); ok {
		if err := validator.Validate(newCModContext(server)); err != nil { t.Fatalf("validate = %v", err) }
	}
}
```

测试文件定义 `validDiscoveryConfig() DiscoveryConfig`；CMod Validate 使用既有 `newCModContext` 构造真实只读上下文，不新增宽松的生产替身接口。

真实 etcd 框架测试以空 `RunOptions.InstanceID` 启动两个 Server，断言 `Context().Instance().ID` 非空且不同；另以一个 Server 同时选择两个服务，断言两个 Service Factory 观察到完全相同的 InstanceID。

- [ ] **Step 2: Run tests to verify the old validation fails**

Run: `go test ./internal/framework -run 'TestDiscoveryCModDoesNotRequireRunOptionInstanceID'`

Expected: FAIL with `instance ID is required`.

- [ ] **Step 3: Let Discovery assign and freeze runtime.instance**

`Server.Start` 初始只构造 ID 为空的实例描述。`startDiscovery` 调用新签名，把返回的 `Runtime.Instance()` 写入 `runtime`：

```go
func (runtime *runtime) setInstance(instance discovery.Instance) {
	runtime.instanceMu.Lock()
	runtime.instance = cloneInstance(instance)
	runtime.instanceMu.Unlock()
}
```

LogicalSlot 必须在最终 InstanceID 可见后创建。RPC 与 Actor 仍可先启动并把 endpoint 写入实例描述；Discovery 注册随后取得完整快照。

全部 framework 调用方迁移完成后，在本任务内删除旧 `NewRuntime(ctx, config, instance)` 与旧 `EtcdRegistry.Register(ctx, instance)`，将新入口分别命名为最终 `NewRuntime(ctx, config, appID, descriptor)` 和 `Register(ctx, appID, descriptor)`。不得把迁移入口留到任务结束。

- [ ] **Step 4: Add a single fatal-error path for identity loss**

```go
func (server *Server) reportFatal(err error) {
	if err == nil { return }
	server.fatalOnce.Do(func() {
		server.mu.Lock()
		server.fatalErr = err
		server.mu.Unlock()
		go func() { _ = server.Stop(context.Background()) }()
	})
}
```

`Server` 增加 `fatalOnce sync.Once` 和 `fatalErr error`。`startDiscovery` 监听 `Runtime.Done()`：`ErrRuntimeClosed` 不报告致命错误；`ErrRuntimeIdentityLost` 记录日志并调用 `reportFatal`。`shutdown` 返回值包含 `fatalErr`，使 `Run` 能把身份失效返回给进程入口。

- [ ] **Step 5: Write and pass the fatal-drain lifecycle test**

```go
func TestFatalDiscoveryLossDrainsAndStopsServer(t *testing.T) {
	service := newBlockingServiceRecorder()
	server := newStartedServerForFatalTest(t, service)
	server.reportFatal(errors.New("workSlot ownership lost"))
	waitClosed(t, server.stopDone)
	if !service.drained || !service.stopped { t.Fatalf("lifecycle = %#v", service) }
	if !strings.Contains(server.stopErr.Error(), "workSlot ownership lost") { t.Fatalf("stop error = %v", server.stopErr) }
}
```

同文件定义一个实现 `Service`、`Drainer`、`Stopper` 的 `blockingServiceRecorder`，以及：

```go
func newBlockingServiceRecorder() *blockingServiceRecorder
func newStartedServerForFatalTest(t *testing.T, service *blockingServiceRecorder) *Server
func waitClosed(t *testing.T, done <-chan struct{})
```

这些辅助函数只装配内存生命周期，不连接外部依赖。

- [ ] **Step 6: Run focused and real etcd framework tests**

Run: `go test ./internal/framework`

Run: `go test ./...`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./internal/framework -run 'Test.*Discovery.*Identity' -count=1`

Run: `gofmt -w internal/framework/*.go && git diff --check`

Expected: PASS；普通 Watch 重建测试不触发进程关闭。

---

### Task 7: CModSessionID 与 Bee/Ant 窄接口

**Files:**
- Create: `/Users/a47/workspace/tx/internal/framework/cmod_session_id.go`
- Create: `/Users/a47/workspace/tx/internal/framework/session_id.go`
- Create: `/Users/a47/workspace/tx/internal/framework/cmod_session_id_test.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/cmod_builtin.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/runtime.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/context.go`
- Modify: `/Users/a47/workspace/tx/bee/cmod.go`
- Modify: `/Users/a47/workspace/tx/bee/context.go`
- Modify: `/Users/a47/workspace/tx/ant/cmod.go`
- Modify: `/Users/a47/workspace/tx/ant/context.go`
- Modify: `/Users/a47/workspace/tx/tests/facade_symmetry_test.go`

**Interfaces:**
- Consumes: Task 4 Discovery identity/loss signal and Task 5 `sessionid.Allocator`。
- Produces:

```go
const CModSessionID CModID = "session-id"

// Bee 与 Ant 使用相同接口形状。
type SessionIDAllocator interface {
	AllocateSessionID(context.Context) (int64, error)
}

type Context interface {
	CModSessionID() (SessionIDAllocator, error)
}
```

- [ ] **Step 1: Write failing CMod dependency and facade symmetry tests**

```go
func TestSessionIDCModRequiresDiscovery(t *testing.T) {
	mod := (&Server{}).sessionIDCMod()
	if got := mod.Spec().Requires; !slices.Equal(got, []CModID{CModDiscovery}) {
		t.Fatalf("requires = %v", got)
	}
}

func TestBeeAndAntExposeSessionIDAllocator(t *testing.T) {
	assertMethodSetSubset(t, reflect.TypeOf((*bee.Context)(nil)).Elem(), reflect.TypeOf((*ant.Context)(nil)).Elem())
	var _ interface{ AllocateSessionID(context.Context) (int64, error) } = (bee.SessionIDAllocator)(nil)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `go test ./internal/framework ./tests -run 'TestSessionID|TestBeeAndAntExposeSessionIDAllocator'`

Expected: FAIL because CMod and facade methods do not exist.

- [ ] **Step 3: Implement a lease-bound allocator runtime**

```go
var ErrSessionIDUnavailable = errors.New("framework: SessionID allocator is unavailable")

type sessionIDRuntime struct {
	next     sessionid.Allocator
	identity sessionIDIdentity
	closed   atomic.Bool
}

type sessionIDIdentity interface {
	Identity() discovery.RuntimeIdentity
	Done() <-chan struct{}
	Err() error
}

func (runtime *sessionIDRuntime) AllocateSessionID(ctx context.Context) (int64, error) {
	if runtime == nil || runtime.closed.Load() { return 0, ErrSessionIDUnavailable }
	select {
	case <-runtime.identity.Done():
		return 0, errors.Join(ErrSessionIDUnavailable, runtime.identity.Err())
	default:
		return runtime.next.AllocateSessionID(ctx)
	}
}
```

`CModSessionID.Start` 从 Discovery identity 读取 workSlot 并构造生成器；`Drain` 先设置 closed，`Stop` 幂等清理引用。它不创建第二份 etcd 租约。

测试使用实现 `sessionIDIdentity` 的内存替身关闭 Done 通道并返回 `ErrRuntimeIdentityLost`；生产装配只传入当前进程的 `*discovery.Runtime`。

- [ ] **Step 4: Add framework and facade accessors**

Framework `Context` 增加内部 `SessionIDs() sessionid.Allocator`。Bee/Ant `contextView.CModSessionID` 必须先检查 owner 是否声明 `CModSessionID`，未声明时返回与其他 CMod 一致的错误。

- [ ] **Step 5: Test allocation, undeclared access and lease loss**

覆盖：

- 已声明 `CModSessionID` 时分配正数且不超过 `sessionid.MaxSessionID`。
- 只声明 Discovery 时不能读取 SessionID 分配器。
- `CModSessionID` 自动把 Discovery 加入依赖闭包。
- Discovery identity Done 后分配返回 `ErrSessionIDUnavailable`。
- Drain 后不再发号。

- [ ] **Step 6: Run facade and race tests**

Run: `go test -race ./internal/framework ./bee ./ant ./tests -run 'Test.*SessionID|TestBeeCommonFacadeMethodsAreCompatibleWithAnt|TestBuiltInCModsDeclareStableDependencies'`

Run: `gofmt -w internal/framework/*.go bee/*.go ant/*.go tests/facade_symmetry_test.go && git diff --check`

Expected: PASS and no race report.

---

### Task 8: 删除人工 InstanceID 并完成 tx 回归

**Files:**
- Modify: `/Users/a47/workspace/tx/internal/framework/config.go`
- Modify: `/Users/a47/workspace/tx/bee/config.go`
- Modify: `/Users/a47/workspace/tx/ant/config.go`
- Modify: `/Users/a47/workspace/tx/bee/server_test.go`
- Modify: `/Users/a47/workspace/tx/ant/server_test.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/cmod_builtin_test.go`
- Modify: `/Users/a47/workspace/tx/tests/facade_symmetry_test.go`
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/implementation_plan.md`

**Interfaces:**
- Consumes: Tasks 1–7 complete behavior。
- Produces: 无人工 InstanceID 的 Bee、Ant 和 framework `RunOptions`，以及与代码一致的稳定文档。

- [ ] **Step 1: Add a compile-time public-boundary test**

```go
func TestRunOptionsDoNotExposeManualInstanceID(t *testing.T) {
	for _, value := range []reflect.Type{reflect.TypeOf(bee.RunOptions{}), reflect.TypeOf(ant.RunOptions{})} {
		if _, ok := value.FieldByName("InstanceID"); ok { t.Fatalf("%s still exposes InstanceID", value) }
	}
}
```

- [ ] **Step 2: Run the boundary test to verify failure**

Run: `go test ./tests -run TestRunOptionsDoNotExposeManualInstanceID`

Expected: FAIL because both facades still contain `InstanceID`.

- [ ] **Step 3: Remove InstanceID from all three RunOptions types**

删除字段、注释和 `toFramework` 映射。无 Discovery 的纯单机进程允许 `Context.Instance().ID` 为空；声明 Discovery 的节点必须在 Service Factory 运行前取得自动 ID。

- [ ] **Step 4: Update tests to assert generated identity only where Discovery is selected**

RPC-only、Redis-only 和纯生命周期测试不再填充无意义 InstanceID。Discovery 测试改为真实 etcd 或可控 Discovery 替身，并断言 ID 形如 `<AppID>-<workSlot>-<ModRevision>`，不写死具体槽位和修订版本。

- [ ] **Step 5: Update stable documentation**

README 与 `docs/design.md` 必须说明：

- `tx/etcd` 是 L1 外部后端适配层。
- Discovery 是 L2 运行时，内部拥有 workSlot 与自动 InstanceID。
- `CModSessionID` 只向需要发号的 Login 暴露。
- LogicalSlot 不使用 workSlot 替代。
- 正常关闭删除实例键但让 workSlot 自然过期。
- ProtoActor Provider 不通过 `tx/etcd`。

`docs/implementation_plan.md` 更新 P0 状态，但不得提前标记 txgame、txgameslg 的项目接入已经完成。

- [ ] **Step 6: Run full tx verification**

Run: `go test ./...`

Run: `go test -race ./etcd ./sessionid ./modules/discovery ./internal/framework ./bee ./ant`

Run with real dependency: `TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./etcd ./modules/discovery ./tests -run 'TestEtcd|TestConcurrentRuntime' -count=1`

Run: `git diff --check`

Expected: all PASS and no diff errors.

- [ ] **Step 7: Produce the maintainer commit message without committing**

```text
feat(runtime): 自动分配运行实例身份与 SessionID

新增 L1 etcd 租约与原子键能力，由 Discovery 统一申请 workSlot 并生成 InstanceID。
增加 CModSessionID 和 53 位 SessionID 生成器，移除人工 InstanceID 启动参数。
```

---

## Follow-up Plans

本计划完成后再分别编写并执行：

1. txgame：AccountID/RoleID 分段 SPI、LoginAccessGuard、`CModSessionID` 接入和旧 MongoDB SessionID 流程删除。
2. txgameslg：独立 Login SPI 范例与 `CModSessionID` 接入。
3. txgame/txgameslg：多 Login、双 Agent、真实 MongoDB/etcd/Redis 的登录与消息链路验收。

这些项目计划不能反向修改本计划已经冻结的 tx 公共契约。
