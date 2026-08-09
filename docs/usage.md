# tx 使用手册

本文记录 `github.com/ngnl47/tx` 当前已实现并验证的运行时能力。手册会随实现持续维护；仍未决策的行为会明确标注。

## 核心决策快照

本节用于后续模型或新同事快速对齐当前方案，具体 API 示例以后文为准。

- `tx` 是 runtime 基础库，只提供元数据、变更追踪、存储抽象、Redis/Mongo/Fusion adapter、延迟任务、生命周期和日志能力。
- 生成代码由 `txgen` 负责；`tx` 不反向依赖生成器，也不读取项目工程的 yml。
- 项目工程负责读取 yml、创建 Redis/Mongo client、组装 route table 和 runtime，再把 reader/writer 注册给生成包。
- `GD*` 是业务数据对象；`GD*Store` 是独立、可组合的变更追踪结构，绑定同一份 `GD*` 数据后才具备读值、修改和投影意义，不保存第二份完整业务数据。
- `GD*` 可懒持有默认主 Store，方便使用 `role.SetName(...)` 等代理方法；Store 本身可独立创建、绑定、组合和复制，不以某个 `GD*` 的身份为唯一归属。
- Store 不是协议字段类型，输入模型和 cmd 请求/响应都不应声明 `*GD*Store` 字段。需要投影下行时，通过 Provider 的 `Set*Store` / `Set*Stores` 传入 viewStore。
- 业务 handler 推荐使用项目 facade，例如 `db.Save(role)`；显式 `storage.Save(ctx, writer, role.Store())` 属于框架层、测试层或高频对象循环控制入口。
- 创建对象的业务流程不由通用 `db.Create` facade 代理。业务负责初始化 `GD*` 字段；框架层 `storage.Create` 只表达 upsert/首次缓存写入语义。
- 当前标脏主线是 `change.DeltaSet`。内部保存字段编号和强类型 key/index；`"Items.1001.Quantity"` 这类字符串只在输出边界临时生成。
- Store 拦截模式和 snapshot diff 模式都保留，且同一变更周期互斥。高频大集合优先 Store wrapper；低频请求可使用 `BeginChange/SaveChange` 允许直接改 public field。
- `DeltaSet` 不保存字段值；Redis、Mongo、Fusion 和 Provider 在边界通过生成 Store 读取当前 `GD*` 最终值。
- 存储边界必须解析到当前字段值：`DeltaValue(path)` 返回 `(nil, true)` 表示业务要写入空值；返回 `(_, false)` 则保存失败并保留 delta，绝不把解析失败静默写成 `null`。Fusion 延迟回写会原样保留这类业务空值；字段删除必须使用 `DeltaDelete` / `Document.Clear`，不能把 map 中的 `nil` 当删除标记。
- 外部协议的返回值必须有明确语义：只有已定义的成功、冲突或未命中值可以继续处理；未知 Lua 返回值、损坏 Redis JSON、空锁或其他不完整响应都会返回错误，绝不默认成功或静默丢弃。
- `DeltaSet` 的 `Kind` 也属于严格协议：仅 `DeltaKindSet`、`DeltaKindReplace`、`DeltaDelete`、`DeltaClear` 可进入通用文档、Redis 或 Mongo 边界；边界会先调用 `ValidateKinds()`，再压缩或归一化，因此未知值即使随后被同路径更新覆盖也会返回错误，不能被当作普通写入。
- `DeltaPath` 的 key 类型同样严格：只允许无 key、带符号整数、无符号整数或字符串 key；非法 key 类型会使路径解析失败，不能退化成父字段路径。
- `DocumentLoader` 的单条结果必须成对一致：`exists == true` 时文档非 `nil`，`exists == false` 时文档必须为 `nil`；批量读取逐项遵守同一规则。自定义 adapter 返回矛盾组合会被 runtime 拒绝，不会被当成缓存未命中继续回源。
- `DocumentReplacer` 的文档也必须非 `nil`：空 map 是合法的空 JSON 对象，`nil` 没有明确写入语义并返回 `storage.ErrNilDocument`；不能把它当作成功的空操作。
- 非空 `DeltaSet` 在 Redis patch 或 Mongo update 边界若无法解析 Go/JSON/BSON 字段路径，会直接返回错误；若路径可解析但最终不能生成任何有效写入，则返回 `storage.ErrNoEffectiveDeltas`。两种失败都会保留 Store delta 以便定位输入或重试，不能把无效路径当成已保存。
- `map[string]` 的危险 key 在 Mongo update 边界降级整字段写，不做私有转义，保证数据库中仍能直观看到业务 key。
- Redis 当前使用 string JSON main/subKey 文档。带 `redis:"subKey:..."` 的字段拆独立 key；main 文档仍是根记录热数据的一部分。
- 整文档替换或 Mongo 回填即使只包含 subKey 字段，也会在 Redis 中保留 main 文档 `{}`；main key 始终作为 Redis 热记录存在标记，不能因字段拆分而缺失。完整替换时，新的文档未包含的已知 subKey 会一并删除，不能让旧子文档回流。
- Fusion 延迟保存队列只保存记录定位，到期后读取 Redis 当前热数据并整文档写回 Mongo；它不是保存每一轮字段 diff 的队列。
- `delaySaveTime` 是 Redis 热数据回写 Mongo 的延迟；`delayDelTime` 是 Redis 热数据 TTL，不表示 Mongo 删除。
- Redis 批量读取生产默认依赖云代理或单节点的原生 `MGET`；本地原生 Redis Cluster debug 才显式开启 `LoadOptions.ClusterSafe` 按 hash tag 分组。

## 定位

`tx` 是运行时基础库，负责：

- 解析和保存类型元数据。
- 追踪字段级变更。
- 将生成 Store 的 `DeltaSet` 落地到 Redis、Mongo 或 Fusion 策略。
- 提供 Redis 延迟任务 backend 和 Fusion 后台回写所需组件。

生成代码依赖 `tx`，`tx` 不依赖生成器工程。

## 快速接入流程

业务工程通常按下面顺序接入运行时：

1. 由生成工程输出 `server/gen/data` 和 `server/gen/cmd`。
2. 业务服务读取本地 yml，解析 Redis/Mongo routeType 路由表。
3. 业务服务创建 go-redis、MongoDB driver client，并用 tx 的薄适配层包装。
4. 用 Redis/Mongo cluster adapter 组装 Fusion strategy。
5. 将 Fusion strategy 注册给生成包的默认 reader。
6. handler 选择一条持久化主线：`BeginChange/SaveChange` 用于直接字段修改，`Store + storage.Save` 用于 setter/Store 拦截式标脏。
7. 后台 Scheduler 消费 Redis 延迟任务，把 Redis 热数据回写 Mongo。

一个简化的启动伪代码如下：

```go
func BootstrapStorage(ctx context.Context, cfg AppConfig) (*fusion.Strategy, error) {
	redisCluster, err := buildRedisCluster(ctx, cfg.Storage.Redis)
	if err != nil {
		return nil, err
	}
	mongoCluster, err := buildMongoCluster(ctx, cfg.Storage.MongoDB)
	if err != nil {
		return nil, err
	}
	delayQueue, err := buildRedisDelayQueue(ctx, cfg.Storage.Redis)
	if err != nil {
		return nil, err
	}

	store := &fusion.Strategy{
		Redis:         redisCluster,
		RedisLoader:   redisCluster,
		RedisDeleter:  redisCluster,
		RedisReplacer: redisCluster,
		MongoLoader:   mongoCluster,
		MongoDeleter:  mongoCluster,
		MongoQueue:    delayQueue,
	}

	data.RegisterStorageRuntime(runtimeReaders{
		redis:  redisCluster,
		mongo:  mongoCluster,
		fusion: store,
	})
	return store, nil
}
```

`runtimeReaders` 是业务工程自己的小适配结构，只需要满足生成包里的 `StorageRuntime` 接口：

```go
type runtimeReaders struct {
	redis  storage.DocumentLoader
	mongo  storage.DocumentLoader
	fusion storage.DocumentLoader
}

func (r runtimeReaders) RedisReader() data.DefaultReader  { return r.redis }
func (r runtimeReaders) MongoReader() data.DefaultReader  { return r.mongo }
func (r runtimeReaders) FusionReader() data.DefaultReader { return r.fusion }
```

如果 reader 同时实现 `storage.BatchDocumentLoader`，生成包会把它注册成默认批量 reader。Redis/Mongo/Fusion 的 cluster adapter 均实现批量读取接口。

handler 中常见写法：

```go
func RenameRole(ctx context.Context, roleID int64, name string) error {
	role, exists, err := data.FindGDRoleByID(ctx, roleID)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrNotExists
	}

	role.SetName(name)
	if err := db.Save(role); err != nil {
		return err
	}
	return nil
}
```

读取走默认 reader，写入走项目侧 facade。这样业务代码能清楚看到哪些操作会落地，避免“修改内存对象后自动保存”的隐式副作用。
上例使用 `db.Save(role)`，表示保存数据对象当前主 Store 中记录的变更，并在保存成功后清空本轮脏标记。业务 facade 接收数据对象，内部再取 `role.Store()`，避免 handler 直接接触框架 Store。
这类 facade 可以使用 `storage.StoreProvider[S]` 作为泛型约束，不需要反射或 `any`：

```go
func Save[S storage.DeltaStore](value storage.StoreProvider[S]) error {
	return storage.Save(ctx, writer, value.Store())
}
```

`storage.Save` 只有在整个写入策略成功后才会清理 Store delta。以 Fusion 为例，Redis 热写和延迟任务入队是两步操作；如果 Redis 已写但入队失败，`Save` 会返回错误，原 delta 仍留在 Store 中，下一次调用可以安全重试并再次登记任务：

```go
if err := storage.Save(ctx, fusionStore, role.Store()); err != nil {
	// role.Store() 仍保留本轮 delta；不能把这次请求当成已完成持久化。
	return err
}
```

有状态服务由 `txn.Runtime.Flush` 保留失败目标并在下一轮重试；无状态服务的 facade 应把错误交给请求重试或业务失败处理。该路径不承诺 Redis 写入与跨组件任务入队的分布式原子性。

如果业务需要显式持有 Store，例如高频对象在内存中维护变更队列、定时合并后再落地，也可以直接使用
`storage.Save(ctx, writer, role.Store())` 这类框架层写入入口，并由调用方决定何时保存或重新累计下一轮变更。

## 服务发现

`modules/discovery` 是可选的 etcd 服务发现基础模块。tx 只接收已经解析完成的
`EtcdConfig` 和启动期构造的 `Instance`，不读取项目 yml、环境变量，也不启动 RPC、Actor 或消息队列。

项目启动时先用 lease 注册本进程。实例应先以 `starting` 注册，等全部服务模块可用后再
切换 `running`；关闭时先切 `stopping`，让精确会话路由继续完成排空，再撤销租约：

```go
registry, err := discovery.NewEtcdRegistry(appCtx, discovery.EtcdConfig{
    Endpoints: []string{"127.0.0.1:2379"},
    Prefix:    "/txgame/local",
    LeaseTTL:  10 * time.Second,
})
if err != nil {
    return err
}
defer registry.Close()

registration, err := registry.Register(appCtx, discovery.Instance{
    ID:       "game-local-1",
    Services: []string{"game"},
    Endpoint: "127.0.0.1:9000",
    Version:  "dev",
    Status:   discovery.StatusStarting,
})
if err != nil {
    return err
}
defer registration.Close(context.Background())

if err := startModules(); err != nil {
    return err
}
if err := registration.SetStatus(context.Background(), discovery.StatusRunning); err != nil {
    return err
}

// 关闭顺序：先停止新服务路由，再排空本模块，最后删除实例记录。
_ = registration.SetStatus(context.Background(), discovery.StatusStopping)
_ = stopModules()
return registration.Close(context.Background())
```

`Prefix` 会规范化为以 `/instances` 结尾的目录；例如 `/txgame/local` 最终写入
`/txgame/local/instances/<instance-id>`。`LeaseTTL` 必须是至少一秒的整数秒，实例 ID
不能包含 `/`。租约 keepalive 中断后 `registration.Done()` 会关闭，`registration.Err()`
返回 `ErrEtcdLeaseLost`；宿主应把该状态视为当前实例已被摘除并停止接受新请求。

需要消费全局实例目录时，使用 `Watch` 更新本地 `ServiceDirectory`。初始快照、任意 watch
事件或事件内容不合法都会直接返回错误；调用方必须重启 watch 或把依赖它的服务标记为不可用，
不能静默继续使用旧目录。

```go
directory := discovery.NewServiceDirectory()
go func() {
    if err := registry.Watch(appCtx, directory); err != nil && !errors.Is(err, context.Canceled) {
        reportDiscoveryFailure(err)
    }
}()

// 普通服务请求只选 running 实例，按稳定顺序轮转。
battle, ok := directory.ResolveService("battle")

// 会话等精确路由可继续解析 stopping 实例，用于连接排空。
msgq, ok := directory.ResolveID("msgq-2")
```

当前目录只实现两类解析：`ResolveService` 是健康服务副本选择；`ResolveID` 是已知实例的精确
路由。`roleId -> 实例` 绑定、Actor `(kind, identity)` owner、消息 Seq/Ack 和有界 mailbox
属于后续 RPC/通信模块，不在服务发现包内。

本机或 CI 已启动 etcd 时，可显式执行真实 lease/watch 回归；未设置环境变量时该集成用例会跳过，
不会要求所有开发机安装 etcd：

```bash
cd /Users/a47/workspace/tx
TX_ETCD_ENDPOINT=127.0.0.1:2379 \
  /Users/a47/sdk/go1.26.5/bin/go test ./tests \
  -run '^TestEtcdRegistryLifecycleIntegration$' -count=1
```

## 类型注解

type 级注解支持 marker 和带参数两种形态。无参数时，下面两种写法等价：

```go
// @genData
// @genData:``
```

带参数时，参数统一写在反引号内。单值写法会默认映射到 `value` 参数：

```go
// @comment:`"角色"`
// @comment:`""`
```

上面两行会分别解析为 `Params["value"] = "角色"` 和 `Params["value"] = ""`。多参数写法使用 Go struct tag 语法：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @genEnum:`gcgc:"true" gcvo:"true"`
```

解析后会统一得到 `anno.Directive`。marker 注解的 `Params` 和 `Flags` 为空；带参数注解的键值会进入 `Params`。

### field tag 解析规则

field 级注解来自 Go struct tag，外层遵循 Go 原生 struct tag 规则：

```go
Items map[int32]*GDItem `comment:"拥有物品" json:"items,omitempty" redis:"subKey:items"`
```

- 外层 tag 项之间用空格分隔。
- 每项是 `key:"value"` 形式。
- value 必须是双引号包起来的字符串。
- `anno.ParseStructTag` 会先通过 Go 标准库读取已注册注解名对应的 value，再解释该 value。

内置 storage 相关 field tag 的 value 使用紧凑语法：

```go
redis:"subKey:items,fullUpdate"
mongodb:"index,unique,expireAfter:0"
```

- 逗号 `,` 分隔多个片段。
- `key:value` 表示参数，进入 `Params[key] = value`。
- 没有冒号的片段表示 flag，进入 `Flags[flag] = true`。
- 片段两边空格会被裁剪，空片段会跳过。
- field tag 的单值不会自动映射到 `Params["value"]`；type 注解的单值规则只作用于 type 级注解。

字段名类 tag 不属于内置注解解析结果，由 AST 扫描直接读取。Go 字段名是框架内部字段身份；
`json`、`bson`、`msgpack` 分别决定对应后端的物理字段名。`alias` 只是输入侧语法糖，如果写了
`alias:"rid"`，会同时作为 JSON、BSON、Msgpack、Redis 的默认字段名；写了 `alias` 的字段不能再写
`json`、`bson` 或 `msgpack`。不写 `alias` 时，各后端 tag 可以各自命名，例如
`json:"roleId,omitempty" bson:"_id,omitempty" msgpack:"roleId,omitempty"`。

### type 注解字段表

| 注解 | 字段 | 必填 | 取值 | 说明 |
| --- | --- | --- | --- | --- |
| `@storage` | `mode` | 是 | `redis`、`mongo`、`fusion` | 选择存储模式。`redis` 只写 Redis；`mongo` 只写 Mongo；`fusion` 使用 Redis 热数据加 Mongo 延迟回写。 |
| `@storage` | `delaySaveTime` | fusion 建议必填 | 秒数字符串或 Go duration | Redis 热数据变更后，经过多久进入 Mongo 回写。别名：`saveTime`、`delaySave`。 |
| `@storage` | `delayDelTime` | Redis/Fusion 热数据建议必填 | 秒数字符串、Go duration 或 `-1` | Redis 热数据 TTL。写入后会续期；不表示 Mongo 删除延迟。别名：`delTime`、`delayDelete`。 |
| `@storage` | `memoryCache` | 否 | 秒数字符串或 Go duration | 预留本地内存缓存时间配置，当前 runtime 不把它作为核心路径。 |
| `@redis` | `routeType` | 是 | 字符串 | 路由配置名，用来在 yml 路由表中选择 Redis endpoint。别名：`shardType`、`config`。 |
| `@redis` | `routeBy` | 是 | Go 字段名 | 路由字段，决定 Redis endpoint 与 Redis Cluster hash tag。别名：`shardBy`。 |
| `@redis` | `idField` | 是 | Go 字段名 | 业务记录主键字段，用于生成记录 key。别名：`id`。 |
| `@redis` | `key` | 是 | 字符串 | Redis key 前缀，例如 `role`，最终 key 类似 `role:{1001}`。 |
| `@redis` | `storeType` | 否 | 字符串 | 预留存储类型标记。 |
| `@redis` | `omitempty` | 否 | flag 或布尔字符串 | 空文档写入策略标记。 |
| `@mongodb` | `routeType` | 是 | 字符串 | 路由配置名，用来在 yml 路由表中选择 Mongo endpoint。别名：`shardType`、`config`。 |
| `@mongodb` | `routeBy` | 是 | Go 字段名 | 路由字段，决定 Mongo endpoint。别名：`shardBy`。 |
| `@mongodb` | `idField` | 是 | Go 字段名 | 业务记录主键字段；runtime 会通过该字段的 `bson` tag 推导 Mongo filter 字段。别名：`id`。 |
| `@mongodb` | `collection` | 是 | 字符串 | Mongo collection 名称。别名：`c`、`col`。 |
| `@mongodb` | `enableSharding` | 否 | flag 或布尔字符串 | 标记该 collection 开启分片语义；当前主要进入元数据。 |

常见 type 注解组合：

```go
// 只存 Redis，适合纯热数据或可丢弃缓存。
// @storage:`mode:"redis" delayDelTime:"2592000"`
// @redis:`routeType:"war" routeBy:"RoleId" idField:"ID" key:"legion_war_record"`
type GDLegionWarRecord struct {
	ID     int64 `json:"id" bson:"id" msgpack:"id"`
	RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
}
```

```go
// 只存 Mongo，适合不需要 Redis 热数据的记录。
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"friend" routeBy:"RoleId" idField:"RoleId" collection:"friend_records"`
type GDFriendRecord struct {
	RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

```go
// Fusion：Redis 保存热数据，Mongo 延迟回写。
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
// @mongodb:`routeType:"role" routeBy:"RoleId" idField:"RoleId" collection:"roles"`
type GDRole struct {
	RoleId int64  `json:"roleId" bson:"_id" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

### field tag 字段表

| tag | 字段或 flag | 取值 | 说明 |
| --- | --- | --- | --- |
| `comment` | 单值 | 任意字符串 | 字段说明，只作为元数据和生成代码注释使用。 |
| `json` | 字段名 | 字符串或 `-` | JSON 字段名。`-` 表示 JSON 忽略该字段。 |
| `json` | 选项 | `omitempty`、`string` 等 Go JSON 选项 | 必须写在字段名之后，例如 `json:"name,omitempty"`。`json:",omitempty"` 属于歧义写法，生成器会拒绝。 |
| `bson` | 字段名 | 字符串、`_id` 或 `-` | BSON 字段名。`_id` 表示绑定 Mongo 文档主键；`-` 表示 BSON 忽略该字段。 |
| `bson` | 选项 | Mongo Go Driver 支持的选项 | 例如 `omitempty`、`inline`。匿名嵌入结构在 JSON 下默认内联，BSON/Msgpack 如果需要内联需显式配置。 |
| `msgpack` | 字段名 | 字符串或 `-` | Msgpack 字段名，cmd Provider 下行变更文档使用该名称。 |
| `msgpack` | 选项 | Msgpack 编码器支持的选项 | 例如 `omitempty`、`inline`。 |
| `alias` | 单值 | 字符串 | 输入侧语法糖：同时作为 JSON、BSON、Msgpack、Redis 的默认字段名。不能和 `json`、`bson`、`msgpack` 混用。 |
| `redis` | `subKey` | 字符串 | 将该字段从 Redis 主文档拆到独立 key，例如 `role:{1001}:items`。 |
| `redis` | `ignore` | flag 或布尔字符串 | Redis 文档忽略该字段。 |
| `redis` | `fullUpdate` | flag 或布尔字符串 | Redis 写入策略：该字段位于 `subKey` 时可直接替换独立 Redis 文档，避免 Lua 读取并合并整个文档。它不改变字段 `Set` 或集合 wrapper `Replace` 的业务覆盖语义。 |
| `mongodb` | `index` | flag 或布尔字符串 | 为该字段生成 Mongo 索引元数据。 |
| `mongodb` | `unique` | flag 或布尔字符串 | 索引唯一约束，通常和 `index` 一起使用。 |
| `mongodb` | `ignore` | flag 或布尔字符串 | Mongo 文档忽略该字段。 |
| `mongodb` | `order` | `asc`、`desc` 或数字字符串 | 索引方向。 |
| `mongodb` | `indexType` | 字符串 | 索引类型，例如文本索引等。 |
| `mongodb` | `weight` | 数字字符串 | 文本索引权重。 |
| `mongodb` | `expireAfter` | 秒数字符串 | Mongo TTL 索引过期时间；命中后删除整条文档，不是删除单个字段。别名：`expire`。 |

常见 field tag 组合：

```go
// 普通字段：三个后端字段名一致。
Name string `comment:"名字" json:"name,omitempty" bson:"name,omitempty" msgpack:"name,omitempty"`
```

```go
// 业务主键直接绑定 Mongo _id。
RoleId int64 `comment:"角色ID" json:"roleId" bson:"_id" msgpack:"roleId"`
```

```go
// Redis 大字段拆分 subKey，避免普通字段变更时重写大集合。
Items map[int32]*GDItem `comment:"物品" json:"items,omitempty" bson:"items,omitempty" msgpack:"items,omitempty" redis:"subKey:items,fullUpdate"`
```

```go
// Mongo 索引字段。
Name string `comment:"名字" json:"name" bson:"name" msgpack:"name" mongodb:"index"`
```

```go
// Mongo TTL 字段。注意：TTL 到期删除整条 Mongo 文档。
ExpireAt int64 `comment:"过期时间" json:"expireAt" bson:"expireAt" msgpack:"expireAt" mongodb:"index,expireAfter:0"`
```

```go
// 输入侧 alias 语法糖，等价于同时使用 roleId 作为 json/bson/msgpack 字段名。
RoleId int64 `comment:"角色ID" alias:"roleId"`
```

非法或不推荐写法：

```go
// 非法：alias 不能和 json/bson/msgpack 混用。
RoleId int64 `alias:"rid" json:"roleId"`

// 非法：json 选项前必须显式写字段名。
Name string `json:",omitempty"`

// 非法：json 选项列表不能有空片段。
Name string `json:"name,omitempty,"`

// 非法：Store 类型是框架运行时类型，不能作为输入结构字段。
Role *GDRoleStore `json:"role"`
```

`@mongodb.idField` 使用 Go 字段名，tx runtime 会通过该字段的 `bson` tag 得到 Mongo 过滤字段。
业务 `idField` 可以直接绑定到 Mongo `_id`，也可以作为普通 BSON 字段保存。

以 `RoleId = 1001`、`Name = "alice"` 为例，常见组合如下：

| 配置组合 | Mongo 文档结构 | runtime 过滤条件 | 说明 |
| --- | --- | --- | --- |
| `idField:"RoleId"`，字段写 `bson:"_id"` | `{"_id": 1001, "name": "alice"}` | `{_id: 1001}` | Mongo 物理主键直接使用业务 ID。 |
| `idField:"RoleId"`，字段写 `bson:"roleId"` 或缺省生成 `bson:"roleId"` | `{"_id": ObjectID, "roleId": 1001, "name": "alice"}` | `{roleId: 1001}` | Mongo `_id` 和业务 ID 分离，runtime 按业务字段查。 |

因此，`bson:"_id"` 是 Mongo 序列化层的字段绑定；`mongodb.idField` 是 tx 存储框架层的业务记录身份。
两者配合后由 runtime 统一推导过滤字段，不再需要额外开关。

txgen 会在生成阶段校验 `bson:"_id"` 的归属：同一 data 类型最多只能有一个字段声明
`bson:"_id"`；如果存在，该字段必须是 `mongodb.idField` 指向的 Go 字段。`routeBy` 只负责
路由取值，不决定 Mongo 物理主键字段。

## 日志

`tx/log` 基于标准库 `log/slog` 提供最小 logger 注入入口：

```go
func SetDefault(logger *slog.Logger)
func Default() *slog.Logger

func Debug(msg string, args ...any)
func Info(msg string, args ...any)
func Warn(msg string, args ...any)
func Error(msg string, args ...any)

func DebugContext(ctx context.Context, msg string, args ...any)
func InfoContext(ctx context.Context, msg string, args ...any)
func WarnContext(ctx context.Context, msg string, args ...any)
func ErrorContext(ctx context.Context, msg string, args ...any)

func NewLogger(name string) Logger

func (l Logger) Debug(msg string, args ...any)
func (l Logger) Info(msg string, args ...any)
func (l Logger) Warn(msg string, args ...any)
func (l Logger) Error(msg string, args ...any)

func (l Logger) DebugContext(ctx context.Context, msg string, args ...any)
func (l Logger) InfoContext(ctx context.Context, msg string, args ...any)
func (l Logger) WarnContext(ctx context.Context, msg string, args ...any)
func (l Logger) ErrorContext(ctx context.Context, msg string, args ...any)
```

默认返回 `slog.Default()`；传入 `nil` 会恢复默认 logger。实际项目可在启动期调用 `log.SetDefault(logger)` 注入自己的 `slog.Logger`，并通过 `slog.Handler` 对接文件日志、云日志或第三方日志实现。

默认调用方式：

```go
log.Error(
    "delay task failed, retry scheduled",
    "taskType", task.Type,
    "taskID", task.ID,
    "action", task.Action,
    "attempts", attempts,
    "err", err,
)
```

需要按功能派生日志时，可以在项目中显式定义包级变量：

```go
var RoleLog = log.NewLogger("role")

RoleLog.Error("role save failed", "roleId", roleID, "err", err)
```

`NewLogger` 返回轻量代理，不是固定绑定的 `*slog.Logger`。代理值本身可以作为包级变量复用；
每次输出时都会读取当前 `Default()`，再附加 `logger=<name>` 属性派生一次日志调用，避免包级变量早于项目日志初始化时绑定到初始化前的 logger。
因此，项目启动后调用 `log.SetDefault(logger)`，此前已经声明的 `RoleLog` 也会使用新的 default logger。

## 元数据与字段名

`meta.TypeSpec` 使用 Go 字段名作为内部字段身份；`TypeSpec.Fields` 的 key、`idField`、`routeBy`、
`LoadFields()` 入参和 `ChangeDocument()` 默认输出都使用 Go 字段名。`meta.Field` 同时保存
`JSONName`、`BSONName`、`MsgpackName`、`RedisName`，运行时适配器在靠近具体后端时再转换字段名。
生成代码会额外提供两类名字入口：`GDRoleGoFieldName` 这类常量表示 Go 字段名，供部分读取和
业务字段选择使用；`GDRoleFieldName(field, backend)` 这类函数按 `go/json/bson/msgpack` 返回
字段名本体。Redis、Mongo、Provider 是使用方策略：Redis 当前选择 JSON 名，Mongo 当前选择
BSON 名，Provider 下行当前选择 Msgpack 名。

例如：

```go
RoleId int64 `json:"roleId,omitempty" bson:"_id,omitempty" msgpack:"roleId,omitempty"`
```

Store 内部变更文档使用 `RoleId`；完整 JSON 序列化使用 `roleId`；Redis 写入当前也选择 JSON
名 `roleId`；Mongo filter/update 选择 BSON 名 `_id`；cmd Provider 下行 body 选择 Msgpack 名
`roleId`。

示例：

```go
field := data.NewGDRoleStore(nil).FieldIndex(data.GDRoleGoFieldRoleId)

data.GDRoleFieldName(field, change.NameBackendGo)      // "RoleId"
data.GDRoleFieldName(field, change.NameBackendJSON)    // "roleId"
data.GDRoleFieldName(field, change.NameBackendBSON)    // "_id"
data.GDRoleFieldName(field, change.NameBackendMsgpack) // "roleId"
```

## DeltaSet

生成 Store 的保存主路径使用 `DeltaSet`。`DeltaSet` 只表示一次保存周期内“哪些字段需要被落地或下发”，
不保存字段值；Redis、Mongo、Fusion 或下行 Provider 在输出边界通过生成 Store 读取当前 `GD*` 对象里的最终值。
运行时不保留字符串路径变更集合；业务代码也不直接维护脏字段容器。
例如输出文档里的 `"Items.1001.Quantity"` 或 `"Tags.0"` 只是边界层为了 Redis/Mongo/Provider 生成的可读路径；
`DeltaSet` 内部保存的是字段编号、map key 或 slice index 组成的结构化路径，不会在热路径上常驻这些拼接字符串。

### 性能基线

涉及 `DeltaSet`、集合递归、批量读取、Redis/Mongo 写入或状态刷盘的热点路径改动前后，都应在同一台机器、同一 Go 版本下运行 benchmark，并同时查看耗时和分配次数。不要用不同机器的绝对 ns/op 数值作结论。

`tx` 提供 `Compact()` 的隔离基线，分别覆盖重复普通字段、256 个 map 子字段和 256 个子路径后整字段覆盖：

```sh
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp \
  /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^$' \
  -bench '^BenchmarkDeltaSetCompact$' -benchmem -count=5
```

`txgen` 还提供生成 Store 的端到端基线，用来比较 snapshot diff 与 Store 路径，以及字段名投影：

```sh
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp \
  /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^$' \
  -bench '^(BenchmarkGeneratedPersistenceModes|BenchmarkFieldNameDocument)$' \
  -benchmem -count=5
```

Fusion 的同步热写基线覆盖 Redis JSON patch 构建，以及 `delaySaveTime` / `delayDelTime` 对应的延迟任务入队；它不连接 Redis/Mongo，也不包含到期任务消费或显式删除的记录锁，因此可单独观察新增、更新的运行时 CPU 与分配开销：

```sh
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp \
  /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^$' \
  -bench '^BenchmarkFusionStrategyApplyDeltas$' -benchmem -count=5
```

例如将 `Compact()` 的实现从双层路径比较改成索引结构前，先记录 `MapItemPaths256` 与 `AncestorReplace256` 的基线；改动后只有在这两类负载的耗时或分配确有收益，并且没有牺牲 `RepeatedField64` 的常见路径时，才引入更复杂的结构。

### 常用业务写法

最常见的写法是直接操作数据对象。`GD*` 会懒加载自己的 Store，并代理到 Store setter：

```go
role := data.NewGDRole()
role.SetRoleId(1001)
role.SetName("alice")
role.SetPower(470001)

doc, err := role.ChangeDocument()
if err != nil {
	return err
}
```

`ChangeDocument()` 与 `MsgpackChangeDocument()` 都返回 `(change.Document, error)`。它们会在边界读取当前
`GD*` 值并验证每条 delta 路径；解析失败时必须停止发送或保存，不能把字段伪造成 `null`。

这时 `ChangeDocument()` 使用 Go 字段名输出后端无关文档：

```go
change.Document{
	Set: map[string]any{
		"RoleId": int64(1001),
		"Name":   "alice",
		"Power":  int64(470001),
	},
	Clear: map[string]struct{}{},
}
```

如果用于 cmd 下行，生成代码会把 Store 里的 `DeltaSet` 转成 Msgpack 字段名文档：

```go
body, err := role.MsgpackChangeDocument()
if err != nil {
	return err
}
```

例如 `HeadImg` 的 tag 是 `msgpack:"hi,omitempty"`，那么下行变更文档里使用 `hi`，不是 Go 字段名
`HeadImg`。

`Deltas()` 返回压缩后的 `change.DeltaSet`。生成 Store 只负责提供 `TypeSpec`、`RecordKey`、
`DeltaSet`、字段名解析器和值解析器；真正落库由 `tx/storage` 的通用 `Create` / `Save` 执行。

### Store 树、共享引用与 viewStore

`GD*Store` 不复制业务数据，而是绑定一个 `GD*` 引用并保存该路径上的字段选择和变更状态。父 Store
按需持有直接指针、map key、slice index 的子 Store，最终在 `Deltas()` 或 `ChangeDocument()` 边界递归投影：

```text
GDRoleStore
└── ItemsStore
    └── key 1001 -> GDItemStore{Value: role.Items[1001]}
```

同一份业务数据可以出现在多条路径，不需要复制对象：

```go
gift := &data.GDItem{ItemId: 1001, Quantity: 1}

roleA.ItemsStore().Set(1001, gift)
roleB.ItemsStore().Set(1001, gift)
```

两个角色各自拥有 `Items[1001]` 的 Store 节点，保存时各自产生持久化变更；但它们共享同一个 `gift` 数据指针。
框架不会维护共享对象的全局反向引用，所以后续更新必须说明具体持久化路径：

```go
for _, role := range guildRoles {
	item, ok := role.ItemsStore().Get(1001)
	if ok {
		item.SetQuantity(2)
	}
}
```

不要期待 `gift.Store().SetQuantity(2)` 自动把变更扩散到所有引用它的角色；独立 `gift.Store()` 只描述
`gift` 自己作为根对象时的变更，不具备“我属于哪些角色”的业务上下文。

`viewStore` 与主 Store 使用同一种状态树和 Delta 投影逻辑，但用途是只读下行投影：主 Store 可以修改数据并保存；
viewStore 只选择需要下行的字段，不参与持久化。生成代码会保护 viewStore 的修改 API，错误调用 `Set`、`Update`、
`Delete` 等方法会立即报错，避免把视图误当成业务写入器。

```go
// 主 Store：修改数据。
item, _ := role.ItemsStore().Get(1001)
item.SetQuantity(2)

// viewStore：只选择下行内容。
view := role.Store().View()
viewItem, _ := view.ItemsStore().Get(1001)
viewItem.TouchItemId()
viewItem.TouchQuantity()

body, err := view.MsgpackChangeDocument()
if err != nil {
	return err
}
```

`NewViewStore()` 从空选择状态开始；`store.View()` 深复制当前 Store 的字段、集合和子 Store 状态，继续引用同一份
`GD*` 数据。它不会复制业务数据，也不能用 Go 赋值直接复制 Store，因为内部 map 和子 Store 必须递归复制。

viewStore 是“选择哪些字段”的副本，不是“创建瞬间的数据快照”。生成文档时会读取当前 `GD*` 值并重新校验每条路径：

```go
view := role.Store().View()
viewDress, _ := view.DressStore().GetAt(0)
viewDress.TouchExpire()

role.DressStore().RemoveAt(0) // slice 结构已变化

doc, err := view.ChangeDocument()
if err != nil {
	return err
}
// 不输出不可靠的 Dress.0.Expire。
// 降级输出 Dress 的当前完整 slice。
```

直接指针替换或清空、map key 删除或指向新对象、slice 长度变化、插入、删除、换位或 index 指向新对象，都会触发
最近安全祖先的整字段输出。这样不会产生失效的 map key 或 slice index 路径。

如果 map 的 key 类型不能表达为 `change.PathKey`（例如业务 enum 或结构体 key），生成器仍会提供该 key 的子 Store，
但导出时保守替换整个 map 字段。这样不会生成无法被 Redis/Mongo 正确定位的伪路径。

如果下行内容必须严格固定为某个时刻的值，应在完成字段选择后立即物化文档或协议 body，而不是把 viewStore 留到后续继续使用：

```go
role.SetPower(100)

view := role.Store().View()
view.TouchName()
view.TouchPower()
body, err := view.MsgpackChangeDocument() // 此刻物化，后续 role 修改不会改变 body。
if err != nil {
	return err
}

role.SetPower(200)
send(body)
```

### 集合递归深度预算

`change.MaxDeltaDepth` 当前为 `4`，它是集合子对象路径的真实递归预算，而不是仅用于存储的数组长度。
map key 或 slice index 与所属字段合并为一个路径节点，因此下面这条路径正好占用四层：

```text
Role.Items[1001].Attrs["atk"].Buffs[2].Modifiers[7]
```

Store 树投影和 `BeginChange/SaveChange` 快照 diff 都使用同一预算。若业务继续修改第五层，框架会立即停止
向下追踪，并把最近可解析祖先改为整体覆盖，保证最终数据正确且避免无意义的深层遍历。例如：

```go
// 第五层的 Value 不再生成 Items.1001.Attrs.atk.Buffs.2.Modifiers.7.Value。
// 改为整体覆盖 Items.1001.Attrs.atk.Buffs.2.Modifiers.7 当前对象。
buff.SetValue(9)
```

该数据类型首次发生这种降级时会输出一次结构化警告。它不是业务错误，也不会 panic；警告用于提示这条路径的
持久化写放大已经超过当前的精细追踪预算。

```go
role := data.NewGDRole()
role.SetName("alice")
role.SetName("bob")

deltas := role.Store().Deltas()
// 只保留 Name 字段被 Set 的 Delta 记录；值不保存在 DeltaEntry 中。
```

`DeltaSet` 也可以在边界转成 `change.Document`。转换会先复制再压缩，不会污染 Store 里原始的
变更队列：

```go
doc := role.Store().Deltas().DocumentWithResolver(
	change.FieldNameResolverFunc(data.GDRoleFieldName),
	change.NameBackendMsgpack,
	role.Store(),
)
// 如果 HeadImg 的 msgpack 字段名是 hi，则 doc.Set["hi"] 是当前头像值。
```

### Set、Touch、Replace、Clear

生成 Store 每个字段通常会有四类方法：

| 方法 | 是否修改内存字段 | Delta 语义 | 常见用途 |
| --- | --- | --- | --- |
| `SetName(value)` | 是 | set | 日常修改标量字段。 |
| `SetItems(value)` | 是 | replace | 覆盖 map/slice/struct 指针字段，不保留旧值中未出现的字段。 |
| `ItemsStore().Set(key, value)` / `DressStore().SetAt(index, value)` | 是 | replace | 覆盖一个 map/slice 元素，不保留该旧元素中未出现的字段。 |
| `TouchName()` | 否 | set 当前内存值 | 字段值已经被业务直接改过，需要补标脏；或 viewStore 只想把当前字段加入下行。 |
| `ClearName()` | 是，设置为 Go 零值 | set 零值 | 保留字段并写入 Go 零值，例如字符串写入 `""`。 |

在 Delta 路径里，普通字段清零和集合清空会被区分：

```go
role.ClearHeadImg()
// 普通字段：DeltaSet 只记录 HeadImg 需要写入；边界从 Store 当前值读取到 Go 零值。
// Redis/Mongo 中会保留 headImg 字段，值为 ""。

role.TasksStore().Clear()
// map/slice：DeltaClear Tasks，表示容器仍存在，但内容为空。
```

如果业务确实需要删除 map key 或 Mongo 字段，集合 Store 会使用 `DeltaDelete` 表达，例如
`Items[1001]` 被删除时，对 Mongo 边界可转换为 `$unset: {"items.1001": ""}`。

日常业务优先使用 `Set*`：

```go
role.SetName("bob")
role.SetPower(role.GetPower() + 100)
```

生成代码同时支持两条持久化主线。休闲游戏的无状态请求可以使用快照 diff：

```go
if err := role.BeginChange(); err != nil {
	return err
}

role.Name = "bob"
role.Items[1001].Quantity = 7

if err := db.SaveChange(role); err != nil {
	return err
}
```

`[]*GD...` 中已有元素也会递归比较生成数据对象的子字段，不会因为使用了直接字段赋值就退化为整元素写：

```go
role.Dress = []*data.GDDressItem{{Id: 7, Expire: 100}}

if err := role.BeginChange(); err != nil {
	return err
}
role.Dress[0].Expire = 200

if err := db.SaveChange(role); err != nil {
	return err
}
// 输出边界：Set["Dress.0.Expire"] = int64(200)
```

若元素本身被替换为另一个对象、设为 `nil`，或者 slice 长度和顺序变化，框架会保守地写对应整元素或整个 slice，避免 index 语义漂移。

高频有状态对象优先使用 Store 拦截式标脏：

```go
role.SetName("bob")
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 7})

view := role.Store().View() // 保存前复制一份下行视图

if err := storage.Save(ctx, fusionDB, role.Store()); err != nil {
	return err
}
```

`BeginChange/SaveChange` 和 Store 写入模式在同一个变更周期内互斥；`NewViewStore()` 与 `store.View()` 只用于下行/广播视图，不参与持久化模式。业务项目通常会再包装一层 facade：Store 拦截路径使用 `db.Save(role)`，snapshot diff 路径使用 `db.SaveChange(role)`，避免 handler 直接传入 `ctx`、writer 或 Store，并保证两条路径都经过项目的存储健康闸门。

如果业务直接修改了字段，需要手动 `Touch*` 补标脏：

```go
role.Name = "bob"
role.TouchName()
```

如果只想把字段加入下行变更，但不想污染主 Store，可以创建 viewStore：

```go
view := role.NewViewStore()
view.TouchName()
view.TouchPower()

body, err := view.MsgpackChangeDocument()
if err != nil {
	return err
}
```

这适合广播或响应中“额外携带一些未修改字段”的场景。主对象的落库 Store 不会因此多出脏字段。

普通字段为生成数据对象指针时，应通过父 Store 取得路径上下文中的子 Store；子 Store 的字段修改会在边界投影为父对象的嵌套 delta：

```go
role.SetDailyTask(&data.GDDailyTask{Active: 1})
role.ClearChanges()

task := role.DailyTaskStore()
task.SetActive(2)
// ChangeDocument(): Set["DailyTask.Active"] = int64(2)
```

`SetDailyTask` 覆盖或 `ClearDailyTask` 清空字段时，父 Store 会替换或移除自己的子 Store 节点。旧对象没有父回调，因此旧对象后续的独立 Store 修改不会污染角色 Store。直接写 `role.DailyTask` 属于快照 diff 或手动 `TouchDailyTask()` 路径，不会自动产生嵌套 Store 变更。

### 单机事务

为类型声明 `@transaction` 后，生成代码提供稳定 `LockKey()` 与 `LockTarget()`。业务传入 GD 数据对象即可，事务只回滚 Store 受控 API 的修改，不处理跨进程锁、Redis/Mongo 分布式事务或裸字段赋值：

```go
	err := txn.Run(ctx, txn.Locks(role), func(tx *txn.Tx) error {
	store := role.Store()
	store.SetName("alice")
	item, ok := store.ItemsStore().Get(1001)
	if ok {
		item.SetQuantity(item.GetQuantity() + 1)
	}
	store.DressStore().Append(&data.GDDressItem{Id: 7, Expire: 100})
	return nil
})
```

回调返回错误或 panic 时，字段值、map/slice 结构、子 Store delta 都会恢复到进入事务前的状态。未进入 `txn.Run` 的普通 Store 修改不会建立 undo log，也不会复制旧集合。

直接使用 `txn.Run` 或 `txn.Call` 的临时事务要求进入前的目标 Store 没有遗留 delta。开发、测试或灰度环境可通过项目配置打开运行时检查；检查在完整锁集获取后、业务回调开始前执行，发现事务外修改时会释放锁并返回错误：

```go
txn.Configure(txn.Config{
	Debug: txn.DebugConfig{
		CheckCleanLockTargets: true,
	},
})

role.SetName("outside-transaction")
err := txn.Run(ctx, txn.Locks(role), process)
// err: transaction: lock target role:1001 has pending deltas before transaction
```

项目应在启动时把自己的 yml 配置转换为这项配置；`tx` 不读取项目文件。生产环境通常保持默认 `false`，避免每个事务入口递归投影整棵 Store 树。开启后应先保存或 `ClearDeltas()` 处理事务外已有的 Store 变更，再进入事务；不要把事务外修改混入本次提交边界。

常驻内存实体由 `txn.Runtime` 管理时，前一轮事务的 delta 会刻意保留到下一次 Flush，因此必须使用 `runtime.Call`。它会在内部允许已有 delta，普通业务代码不要直接调用 `txn.CallWithOptions`。

锁等待通过调用方的 `context.Context` 统一控制。获取完整锁集之前发生超时或取消时，框架会释放本次已获取的锁，且不会执行回调：

```go
ctx, cancel := context.WithTimeout(rootCtx, 200*time.Millisecond)
defer cancel()

err := txn.Run(ctx, locks, func(tx *txn.Tx) error {
    // 只有所有 LockTarget 都已锁定后才会进入这里。
    return update(tx)
})
if errors.Is(err, context.DeadlineExceeded) {
    // 返回给调用方重试、排队或降级。
}
```

多个目标按 `LockTarget.Order`、`LockKey.Scope`、`LockKey.Value` 的稳定顺序获取锁。生成的 `LockTarget()` 默认 order 为 `0`；需要区分锁层级时，可由 `@transaction` 生成不同的 order，例如军团为 `10`、角色为 `20`。同一逻辑 Key 不得以不同 order 重复声明。

前置查询后需要动态收集锁时使用 `LockSet`。Builder 不在收集阶段自行去重或加锁，最终仍交给 `Run` 统一校验：

```go
locks := txn.NewLockSet().Add(guild)
for _, member := range members {
    locks.Add(member)
}

err := txn.Run(ctx, locks.Build(), func(tx *txn.Tx) error {
	return updateGuildMembers(tx)
})
```

需要复用一段带锁业务逻辑时，定义 `Procedure` 并按项目约定使用 `P` 前缀。根过程通过 `txn.Call` 获取完整锁集；子过程通过当前 `tx.Call` 同步执行，只能声明根过程已经持有的锁：

```go
type PGuildContribution struct {
	Guild *data.GDLegion
	Role  *data.GDRole
}

func (p *PGuildContribution) Locks(context.Context) ([]txn.LockTarget, error) {
	return txn.Locks(p.Guild, p.Role), nil
}

func (p *PGuildContribution) Process(tx *txn.Tx) error {
	p.Guild.SetExp(p.Guild.Exp + 10)
	if err := tx.Call(&PGrantReward{Role: p.Role}); err != nil {
		return err
	}
	return nil
}

type PGrantReward struct {
	Role *data.GDRole
}

func (p *PGrantReward) Locks(context.Context) ([]txn.LockTarget, error) {
	return txn.Locks(p.Role), nil
}

func (p *PGrantReward) Process(*txn.Tx) error {
	p.Role.SetPower(p.Role.Power + 10)
	return nil
}

err := runtime.Call(ctx, &PGuildContribution{Guild: guild, Role: role})
```

`txn.Tx` 只表示本次已持锁的运行环境，负责 undo、savepoint、子过程调用和提交/回滚回调；它不是业务数据，也不是 `context.Context`。事务内通过生成的 `GD*` 代理方法或显式 Store 修改数据，两者都会走同一受控 Store 路径并登记 undo。过程成功只表示内存修改已提交，不等价于 Redis 或 Mongo 已写入；`AfterCommit` 在锁释放、undo 解绑后按注册顺序执行，任一回调失败会停止后续回调，但不会回滚内存修改。过程返回错误时，`AfterCommit` 不会执行；可用 `AfterRollback` 做指标记录、临时资源释放等不应持久化的数据清理。

事务中可通过 `tx.DirtyTargets()` 取得本次经受控 Store API 修改、且当前仍有 delta 的根目标。结果按锁顺序返回；被 savepoint 回滚或随后 `UnTouch` 清空的 Store 不会出现。它供 `txn.Runtime` 登记落盘目标，普通业务循环不需要调用：

```go
func (p *PUpdateRole) Process(tx *txn.Tx) error {
	p.Role.SetPower(p.Role.Power + 10)
	return nil
}
```

### 有状态落盘调度

`txn.Runtime` 是实体长期驻留内存服务中 Procedure 的标准搭档。事务成功后它只按锁键登记对应 Store；多个事务持续修改同一对象时，Store 保留路径 delta，下一次 Flush 才把当前值物化为一次 Fusion 写入。

这里的 `pending` 指“已发生受控修改、尚未成功 Flush 的持久化根对象”。`Runtime.Pending()` 返回按 `LockKey` 去重后的这个数量，而不是内存中全部 `GD*` 的数量：同一个 `GDRole` 连续修改多次、或仅修改 `GDRole.Items[1001]` 等子字段，都只算一个 pending root；同一事务修改一个 `GDLegion` 和两个不同的 `GDRole`，则算三个。Flush 成功、Store delta 清理后，对应 root 才会移出 pending；未产生 delta 或纯内存对象不计入其中。

```go
runtime := txn.NewRuntime(fusion, txn.RuntimeConfig{
	FlushInterval:          10 * time.Second,
	ShutdownFlushTimeout:   30 * time.Second,
	MaxRootDeltaEntries:    1024,
	PendingMemorySoftLimit: 512 << 20, // 512 MiB
	MemorySampleInterval:   time.Second,
	MaxPendingRoots:        0, // 可选的数量兜底
})

if err := runtime.Call(ctx, &PGuildContribution{Guild: guild, Role: role}); err != nil {
	return err
}

// 尚未发生 Redis/Mongo I/O；role、guild 的 Store 仍保留本轮及此前未 Flush 的 delta。
if err := runtime.Flush(ctx); err != nil {
	return err // delta 保留，下一次 Flush 可重试
}
```

`Flush` 会为每个实体取得与 `Procedure` 相同的本机锁，在锁内执行一次 `Deltas -> Compact`，并将这份投影交给框架写入入口，成功后才清理 delta。这样并发 Procedure 不会在 Flush 读取 `GD*` 当前值时修改同一个对象：

```text
事务 1：role.SetGold(100)
事务 2：role.SetGold(120)
Flush：  只写 role.Gold = 120
```

服务启动后应将定时循环纳入自己的优雅关停 context。标准项目启动调用 `Runtime.Start(waitGroupContext)`；关闭阶段调用 `Runtime.Shutdown(context.WithoutCancel(serviceCtx))`，由 Runtime 使用配置的超时完成最后刷盘：

```go
if err := runtime.Start(serviceCtx); err != nil {
	return err
}

// 先由服务全局 WaitGroupContext 等待已经进入的业务任务结束，再执行。
if err := runtime.Shutdown(context.WithoutCancel(serviceCtx)); err != nil {
	return err
}
```

`Run` 遇到单次写入错误会记录错误并保留目标，下一轮继续重试。`Shutdown` 在 `ShutdownFlushTimeout` 有效期内立即 Flush 并按 `FlushInterval` 继续尝试；截止时间到达时会返回错误和剩余待刷数量。该超时只约束内存 Store 到 Redis 的最终刷盘，不等待 Fusion 的 Redis 延迟队列回写 Mongo。第一版不把 `DeltaSet`、Store 或 GD 指针交给 Actor 外的异步写队列；如未来需要解除 Flush 的 I/O 持锁时间，应在锁内物化不可变写入批次，并补齐顺序确认与失败重试语义。

`MaxRootDeltaEntries` 限制单个 pending root 的细粒度 Delta 数量。Runtime 只在 `Flush` 取得该对象锁、准备投影时检查，避免每次 `Procedure` 提交递归遍历整棵 Store 树；若数量超过预算，就清理树内的细粒度 dirty 状态，并按当前 `GD*` 值记录全部可更新顶层字段的 `Replace`，自动跳过 BSON `_id`。例如一轮持续修改 `role.Items[1..2000]` 时，预算为 `1024` 不会拒绝这次业务，而会把本次保存收敛为 `Name`、`Items` 等顶层字段的覆盖写，避免不同 key 长期累积。该收敛会记录一条 `txn runtime root deltas collapsed` 告警；它限制 dirty 路径，不限制 `GD*` 业务对象本身或干净子 Store 缓存的内存。

`PendingMemorySoftLimit` 是进程 Heap 告警阈值（字节）。Runtime 按 `MemorySampleInterval` 读取 `HeapAlloc`；超过阈值只记录 `txn runtime pending memory threshold exceeded` 告警，不拒绝新 root，也不改变已提交事务。采样间隔为 `0` 时每次新增 root 预检都采样；内存阈值为 `0` 时禁用该观测。部署应通过 `GOMEMLIMIT` 约束整个 Go 进程；不能用进程级 Heap 采样作为拒绝业务请求的根因。

`MaxPendingRoots` 是可选的数量硬保护。容量满时同样仅拒绝新 root，并返回 `txn.ErrPendingRootCapacity`；设置为 `0` 表示不限制。当前同一路径的重复写入不会无限增加 dirty 条目，例如连续修改 `role.Items[1001].Quantity`，最终只保留 `Items.1001.Quantity` 的最新状态；但持续新增不同 key 仍会增加路径数量。生成的 map/slice Store 会缓存读取过的子 Store 以保持路径身份，不过投影和清理只遍历实际变脏的子路径，不会扫描全部缓存元素。不要在 setter 内加入隐式 Flush 或 I/O；由 Runtime 的调度周期和业务侧反压策略决定何时落盘。

### 零值和删除

`Set*` 设置零值表示“把字段写成零值”，不是删除字段：

```go
role.SetName("")
role.SetPower(0)
```

对应变更文档：

```go
change.Document{
	Set: map[string]any{
		"Name":  "",
		"Power": int64(0),
	},
}
```

落到 Redis/Mongo 后，字段仍然存在，只是值为 `""` 或 `0`。这和 `omitempty` 不冲突：`omitempty`
主要影响完整对象序列化时是否省略零值；生成 Store 的 Delta set 语义会明确保留这次设置的零值。

`Clear*` 不删除普通字段，而是写入其 Go 零值：

```go
role.ClearName()
```

对应变更文档：

```go
change.Document{Set: map[string]any{"Name": ""}}
```

要删除 map 中某个元素，使用集合 wrapper 的 `Delete(key)`；它会生成 `DeltaDelete`，Redis 移除 JSON 路径，Mongo 转成 `$unset`。

### 集合字段和路径标脏

集合字段可以整字段标脏，也可以通过生成 Store wrapper 标记内部路径。低频业务可以整字段标脏，降低编码心智负担：

```go
role.SetItems(map[int32]*data.GDItem{
	1001: {ItemId: 1001, Quantity: 3},
})
```

`SetItems(value)` 用 Go 赋值语义整体覆盖 map，并记录整字段 `Items`。后续 Redis 中如果
`Items` 配置了 `redis:"subKey:items"`，只会更新
`role:{1001}:items` 这个 subKey 文档，不会重写主文档。

也可以手动修改后补 `TouchItems()`，同样是整字段语义：

```go
role.Items[1001].Quantity += 3
role.TouchItems()
```

热点集合路径建议使用生成的集合 Store wrapper。map 的 `Set`、`Delete`、`Touch(key)` 会记录 key 级状态；如果 map value 是生成数据对象指针，`Get(key)` 返回该 key 路径专属的子 Store：

```go
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 3})
// 变更文档：Set["Items.1001"] = 当前 item

item, _ := role.ItemsStore().Get(1001)
item.SetQuantity(7)
// 变更文档：Set["Items.1001.Quantity"] = 7
```

slice 的 `SetAt`、`TouchAt` 会在长度和顺序不变时记录 index 级 dirty：

```go
role.TagsStore().SetAt(0, "old_new")
// 变更文档：Set["Tags.0"] = "old_new"
```

如果 slice 元素是生成数据对象指针，`GetAt` 返回父 slice 的 index 路径专属子 Store：

```go
role.Dress = []*data.GDDressItem{{Id: 7, Expire: 100}}

item, ok := role.DressStore().GetAt(0)
if ok {
	item.SetExpire(200)
}
// 变更文档：Set["Dress.0.Expire"] = 200
```

`Append`、`RemoveAt`、`Replace`、`Clear` 这类改变长度、顺序或整体语义的操作仍会记录整字段 dirty；如果同一轮里已经存在整字段 dirty，后续子字段 dirty 会被吸收。

这里的 `"Tags.0"` 和 `"Dress.0.Expire"` 都是输出边界文档 key，不是 Store 内部 dirty 结构。Store 内部仍是 `DeltaEntry{Path: Tags + index 0}` 或 `DeltaEntry{Path: Dress + index 0 + Expire}`。

`Append`、`RemoveAt`、`Clear`、`Replace` 会改变长度、顺序或整体语义，因此仍然记录整字段 dirty。
生成代码不会暴露字符串 path 级标脏 API，业务侧应使用强类型方法。

### 覆盖规则

`DeltaSet` 同一目标多次写入时，只保留最后一次：

```go
role.SetName("alice")
role.SetName("bob")
```

最终只记录：

```go
Set["Name"] = "bob"
```

父路径和子路径的覆盖规则由 Store 和 `DeltaSet.Compact()` 共同负责。例如：

```go
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 3})
role.TouchItems()
```

最终只需要保留 `Items` 这个根字段；整字段写会覆盖并清理更细的 key/index 子路径。

### AllTouch 和 ClearChanges

`AllTouch()` 表示整对象需要全量落地。它会清空已有字段级 `DeltaSet`：

```go
role.SetName("bob")
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 3})
role.Store().AllTouch()
```

执行后不再保留 `Name` 或集合字段的局部记录。后续保存路径应根据 `AllDirty()` 走整文档替换。

`ClearChanges()` 或 Store 的 `AllUnTouch()` 用于加载完成、主动放弃本轮修改，或显式 Store 写入成功后手动清空脏标记。它会递归清理整棵 Store 树的字段、集合和子节点状态。
普通请求链路建议使用项目 facade 自动清理：

```go
if err := db.Save(role); err != nil {
	return err
}
```

高频对象也可以显式持有 Store，把多次修改合并到同一个 tracker，定时或达到阈值后写入：

```go
store := role.Store()
store.IncPower(10)
store.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 7})

if err := storage.Save(ctx, fusionStore, store); err != nil {
	return err
}
```

这种写法适合 SLG/MMO 战斗、沙盘状态等有状态高频对象；关键是由业务循环明确控制“累计多久、何时保存”。保存成功后 Store 会自动清理 dirty；如果需要先广播或排队合并，可以在保存前复制 `store.View()` 或 `store.Deltas()`。

生成的加载方法在成功读取后会自动清空脏标记，避免“刚加载出来的数据”被误认为需要保存。

### DeltaSet 到后端文档

生成 Store 的主路径记录的是 `change.DeltaSet`：

- `DeltaKindSet`：设置标量字段值，或修改已存在集合元素的子字段。
- `DeltaKindReplace`：覆盖集合、结构体指针或集合元素的当前值。它保留到 Redis 边界，Lua 按路径直接赋值，不走递归 merge，因此旧对象的遗漏字段不会残留。
- `DeltaDelete`：删除字段或集合元素，Mongo 边界会转成 `$unset`，Redis 边界会从 JSON 文档移除该路径。
- `DeltaClear`：清空集合容器，保留字段但写为空 map/slice。
- `FieldID`：生成代码提供的稳定字段编号，避免热路径反复保存字段名字符串。
- `DeltaPath`：字段路径。生成集合 Store 会在这里表达 map key、slice index 和生成对象子字段路径。

生成代码会传入字段名 resolver，把字段编号解析为字段名：

```text
DeltaPath{FieldID(fieldGDRoleName)} -> Name
DeltaPath{FieldID(fieldGDRoleItems)} -> Items
```

变更落地和下行投影不会对整个数据对象做全量 `json.Marshal`、`bson.Marshal` 或
`msgpack.Marshal`。`DeltaSet` 只遍历本保存周期内的脏字段，通过生成的 resolver 或
`TypeSpec` 转成目标后端字段名，再生成局部 patch/update document。若某个脏字段本身是
map、slice 或 struct 指针，具体字段值在最终编码时仍会交给对应 JSON/BSON/Msgpack 编码器展开。
完整对象读取、回源或整文档替换路径才会依赖序列化库按 tag 映射字段。

## Redis 存储模型

Redis 使用 string JSON 文档模型。

当前主线选择普通 Redis string 存储 JSON 文档，并通过 Lua 在写入时合并或替换文档。RedisJSON/ReJSON
可以作为后续可选 backend 单独评估，但不是当前默认路径；引入前需要确认目标云 Redis、测试环境和
Redis Cluster 行为都支持对应 JSON 命令。

### key 结构

主 key 保存没有 `subKey` 的字段：

```text
role:{470001} -> {"roleId":470001,"name":"alice","power":47}
```

带 `redis:"subKey:items"` 的字段保存到独立 key：

```text
role:{470001}:items -> {"items":{"1001":{"quantity":3}}}
```

Redis Cluster 下使用 hash tag 保证同一条记录的主文档和 subKey 位于同一 slot：

```text
role:{id}
role:{id}:items
```

当 `routeBy != idField` 且 `RecordKey.ShardKey` 存在时，hash tag 使用路由字段：

```text
arena:{roleId}:id
```

### 写入流程

生成 Store 会把业务变更转成 `DeltaSet`，并优先交给实现了 `storage.DeltaApplier` 的 Redis adapter：

```go
role := data.NewGDRole()
role.SetRoleId(470001)
role.SetName("alice")
role.SetPower(47)
```

如果是新建或首次缓存，框架层使用 `storage.Create`：

```go
err := storage.Create(ctx, redisCluster, role.Store())
```

Redis adapter 收到 Delta 变更后会：

1. 根据 `TypeSpec` 和 `RecordKey` 生成 Redis key 前缀，例如 `role:{470001}`。
2. 根据字段的 Go 字段名定位 `redis:"subKey:..."`，再用 JSON 字段名写入 main 文档或 subKey 文档。
3. 把每个文档的 patch 编码成 JSON 字符串。
4. 调用 Lua 脚本：顶层简单 `DeltaKindSet` 使用 merge；嵌套路径和 `DeltaKindReplace` 使用有序路径操作直接覆盖目标路径；两者都会在 Redis 内部 `cjson.decode` 后再编码写回。
5. 按 `ApplyOptions.TTL` 设置或续期 Redis 热数据。
6. 递增 `<mainKey>@v` 版本键，并让它与 main/subKey 使用同一有效 TTL；持久化热数据时版本键也会持久化。

更新已有记录使用 `storage.Save`：

```go
role.SetName("bob")

err := storage.Save(ctx, redisCluster, role.Store())
```

`storage.Save` 强制 `Upsert=false`。如果 Redis 中不存在该记录，Redis adapter 会返回
`storage.ErrNotExists`，避免普通更新意外创建新热数据。

### 局部路径的类型约束

Lua 只会为**缺失**的对象父节点创建 `{}`。已存在的 `null`、数字、字符串等非容器父节点会使 adapter 返回
可用 `errors.Is` 判断的 `storage.ErrPathTypeMismatch`，Redis 原文档保持不变。运行时不会把错误数据悄悄改写成
对象，因为这会掩盖上游模型或数据迁移问题。

已有数组可以使用生成 Store 的稳定下标更新既有元素的子字段。例如 `queue` 当前为
`[{"code":2001,"count":5}]` 时：

```go
item, _ := probe.QueueStore().GetAt(0)
item.SetCount(8)

err := storage.Save(ctx, redisCluster, probe.Store())
// Redis 的 queue 子文档更新为：{"queue":[{"code":2001,"count":8}]}
```

Lua 不会为数组补空位、插入或删除元素。`Append`、`InsertAt`、`RemoveAt`、`Replace` 与 `Clear` 这类会改变
长度或整体结构的操作由生成 Store 标记为整字段覆盖；数组下标不存在或路径段不是整数时返回
`storage.ErrPathTypeMismatch`。其中 `Clear()` 会明确持久化为空 JSON 数组 `[]`，不会因 Lua 重编码退化为
空对象 `{}`。

```go
// Redis 初始值：{"roleId":1001}
role.SetRankInfo(&data.GDRankInfo{})
role.ClearChanges()
role.RankInfoStore().SetLevel(7)
err := storage.Save(ctx, redisCluster, role.Store())
// 成功，结果包含："rankInfo":{"level":7}

// Redis 初始值：{"roleId":1001,"rankInfo":7}
role.ClearChanges()
role.RankInfoStore().SetLevel(8)
err = storage.Save(ctx, redisCluster, role.Store())
// errors.Is(err, storage.ErrPathTypeMismatch) == true
// Redis 仍保持 "rankInfo":7。
```

需要有意重建整个复杂字段时，使用字段级 `Set*`，让它生成 `DeltaKindReplace` 覆盖目标字段，而不是尝试
从错误类型的父节点继续局部写入：

```go
role.SetRankInfo(&data.GDRankInfo{Level: 8})
err := storage.Save(ctx, redisCluster, role.Store())
```

整字段替换使用字段 `Set*`，保存入口仍是 `storage.Save`：

```go
role.SetItems(map[int32]*data.GDItem{
	1001: &data.GDItem{ItemId: 1001, Quantity: 3},
})

err := storage.Save(ctx, redisCluster, role.Store())
```

同样地，`ItemsStore().Set` 覆盖单个元素，而不是合并旧元素：

```go
// Redis 原值：{"items":{"1001":{"quantity":1,"quality":9}}}
role.ItemsStore().Set(1001, &data.GDItem{ItemId: 1001, Quantity: 3})
err := storage.Save(ctx, redisCluster, role.Store())
// Redis 结果：{"items":{"1001":{"itemId":1001,"quantity":3}}}
// quality 不会残留。
```

如果这次整字段替换同时允许创建记录，使用 `storage.Create` 表达。Fusion 回源 Mongo 后回填 Redis 也会走整文档写入。

### TTL

生成 Store 会把 `@storage.delayDelTime` 作为 Redis 默认 TTL：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
```

上面配置表示 Redis 热数据默认 86400 秒过期。写入时如果调用方没有显式传 `ApplyOptions.TTL`，生成代码会自动填入该值。
显式 TTL 会覆盖默认值：

```go
err := storage.SaveWithOptions(ctx, redisCluster, role.Store(), storage.ApplyOptions{
	TTL: 3600,
})
```

如果 `TTL > 0`，Redis 使用该 TTL 写入或续期；如果 `TTL == 0`，生成 Store 会补默认 TTL；如果底层直接调用 runtime 且没有生成 Store 补默认值，
则需要调用方自己明确 TTL 策略。Fusion 从 Mongo 回填 Redis 时要求 `delayDelTime > 0`，避免生成没有过期时间的热数据。

### 读取流程

单条读取：

```go
role, exists, err := data.LoadGDRole(ctx, redisCluster, data.GDRoleRecordKey(470001))
```

只读取部分字段：

```go
role, exists, err := data.LoadGDRoleFields(ctx, redisCluster, data.GDRoleRecordKey(470001), "Name", "Power")
```

如果只请求 subKey 字段，例如 `Items`，Redis 可以只读取对应 subKey 文档：

```go
role, exists, err := data.LoadGDRoleFields(ctx, redisCluster, data.GDRoleRecordKey(470001), "Items")
```

批量读取：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, redisCluster, []int64{1001, 1002, 1003})
```

如果类型的 `routeBy != idField`，生成器不会生成简单的 `Load*ByID(ids)`，因为仅有 id 无法构造 Redis endpoint 和 hash tag。
这类类型需要业务传入包含 routeBy 的 `RecordKey` 列表，或使用生成器后续提供的 route-aware helper。

## RecordKey

`storage.RecordKey` 描述一条记录的存储定位：

```go
type RecordKey struct {
    TypeName string
    ID       any
    ShardKey any
}
```

存储定位只作用于当前被读写的根对象。一个带存储元数据的数据类型如果被嵌套在其他对象字段中，运行时会把它当作普通内联文档处理，不会因为子类型也可独立存储而自动生成或访问子记录。

- `ID` 来自 `idField`。
- `ShardKey` 来自 `routeBy`，仅在 `routeBy != idField` 时需要。
- 批量读取必须保证每条记录携带正确路由字段，才能构造正确 `RecordKey`、Redis endpoint 和 hash tag。
- 存储入口会校验 `RecordKey`：`ID` 不能是 nil、typed-nil 或空白字符串；当 `routeBy != idField` 时，缺少 `ShardKey` 返回 `storage.ErrInvalidRecordKey`，不会回退使用 `ID` 路由或生成错误 Redis hash tag。

常见情况一：`routeBy == idField`。

```go
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
key := data.GDRoleRecordKey(470001)
```

对应：

```go
storage.RecordKey{
	TypeName: "github.com/example/game/gen/data.GDRole",
	ID:       int64(470001),
}
```

常见情况二：`routeBy != idField`。

```go
// @redis:`routeType:"arena" routeBy:"RoleId" idField:"ID" key:"arena"`
key := storage.RecordKey{
	TypeName: data.GDArenaRecordTypeSpec().Type.Ref.FullName,
	ID:       int64(90001),
	ShardKey: int64(470001),
}
```

这里 `ID=90001` 是记录主键，`ShardKey=470001` 用于选择 Redis/Mongo endpoint 和生成 Redis hash tag。

读取部分字段时，生成代码会自动补充 `idField`；当 `routeBy != idField` 时也会补充 `routeBy`。这样即使业务只请求
`Name`，加载回来的对象仍然保留后续保存所需的记录定位信息。

## RouteTable

`storage.RouteTable` 是 Redis 和 Mongo 共用的运行时路由表，可同时作为 `redis.Cluster` 和 `mongo.Cluster` 的 router 使用。

### 直接用代码构造

小型测试或没有配置文件时，可以直接写 Go 代码：

```go
router := storage.NewRouteTable(map[string][]storage.RouteRule{
    "role": {
        {Min: 1, Max: 999999, Shards: []string{"role-a", "role-b"}},
        {Min: 1000000, Shards: []string{"role-c"}},
    },
    "global": {
        {Shards: []string{"main"}},
    },
})
```

路由规则：

- 先按 `routeType` 找到规则列表。
- 再按 `shardKey` 命中 `min/max` 区间；`min <= 0` 视为 `1`，`max <= 0` 视为无限大。
- 多 shard 规则用 `routeBy % len(shards)` 选择 endpoint。
- 单 shard 规则允许字符串主键，用于全局表或单分片表。

以上配置表示：

| routeType | shardKey | endpoint |
| --- | --- | --- |
| `role` | `1001` | `role-b`，因为 `1001 % 2 = 1` |
| `role` | `1000000` | `role-c` |
| `global` | `"season"` | `main` |

### 从 yml 配置构造

`storage/config.RouteTableConfig` 用于承接项目 yml 解析后的 Redis/Mongo 路由配置。项目负责读取文件和处理 include，随后把 yml 反序列化到该类型：

```yaml
main: "redis://main"
role:
  - min: 1
    shards:
      - "redis://role-a"
      - "redis://role-b"
rank:
  - "redis://rank-a"
  - "redis://rank-b"
arena:
  - min: 1
    max: 999999
    shards:
      - "redis://arena-a"
      - "redis://arena-b"
  - min: 1000000
    shards:
      - "redis://arena-c"
```

支持的 routeType 值形态：

- 标量 endpoint：归一化为单 shard 规则，例如 `main: "redis://main"`。
- endpoint 数组：归一化为一个多 shard 规则，例如 `rank: ["redis://rank-a", "redis://rank-b"]`。
- 规则数组：保留 `min/max/shards` 区间规则，例如上面的 `arena`。

规则对象只允许 `min`、`max`、`shards` 三个属性。未知属性（例如把 `shards` 写成 `shardz`）会在 yml
解析时返回错误，不能带着拼写错误继续启动。routeType 名必须是非空、无首尾空白的标量，且同一张表中不能
重复。每个 endpoint 也必须非空且不含首尾空白；URI 的协议、地址和鉴权参数仍由项目适配层在创建 Redis/Mongo
client 时解析。因此下面这些配置都会在解析期失败，而不会以后面的 `role` 覆盖前面的规则或把错误留到 client 装配：

```yaml
role: redis://first
role: redis://second

" role ": redis://main

rank: " redis://rank.example.com:6379 "
```

配置对象可直接构造运行时路由表：

```go
var redisRoutes config.RouteTableConfig
if err := yaml.Unmarshal(body, &redisRoutes); err != nil {
	return err
}
table, err := redisRoutes.RouteTable()
if err != nil {
	return err
}
endpoints := redisRoutes.Endpoints()
```

`Endpoints()` 返回配置中出现过的 endpoint URI 集合；当前 key 和 value 都是 URI，运行时装配可按 URI 创建并复用客户端。

tx 提供官方 driver 的薄适配层，业务工程仍负责读取本地配置、创建具体 client，再传给 tx runtime：

```go
redisClient := txgoredis.NewClient(goRedisClient)
mongoDB := txmongodriver.NewDatabase(mongoDriverDatabase)
```

对应 import 路径：

- `github.com/ngnl47/tx/storage/redis/goredis`
- `github.com/ngnl47/tx/storage/mongo/mongodriver`

## Redis Route/Cluster Adapter

`redis.Cluster` 按 `routeType` 和 `storage.RecordKey.ShardKey` 路由到具体 Redis adapter。

- 单条写入、替换、删除和读取会先选择 endpoint，再调用对应 adapter。
- 批量读取会先按 endpoint 分组，每个 endpoint 内仍使用客户端层原生 `MGET` 读取 main/subKey keys，然后按输入顺序还原结果。
- 默认策略贴合云 Redis 代理能力，不在 endpoint 内继续按 slot 拆分；本地原生 Redis Cluster 可显式打开 `LoadOptions.ClusterSafe` 调试模式。
- 写入和整文档替换直接接收 `storage.ApplyOptions`，不再暴露 Redis 专用 `ApplyOptions` 别名；`redis.LoadOptions` 仍是 Redis 批量读取的专用选项。

不同 Redis 环境下的批量读取策略：

- Redis 单节点：直接使用客户端层原生 `MGET`。
- 云 Redis 代理或云集群 endpoint：默认直接使用客户端层原生 `MGET`，由云代理负责跨 slot 分发和结果聚合。
- 本地原生 Redis Cluster 或直连 cluster 节点：显式设置 `LoadOptions.ClusterSafe: true` 后，运行时会按 hash tag 分组后多次 `MGET`；当前默认路径不做该拆分。

### 场景一：本地单节点 Redis

yml 可以只写一个 endpoint：

```yaml
role: "redis://127.0.0.1:6379/15"
global: "redis://127.0.0.1:6379/15"
```

初始化时把所有 endpoint 映射到同一个 go-redis client 即可：

```go
routeTable, err := redisRoutes.RouteTable()
if err != nil {
	return err
}

client := redis.NewClient(&redis.Options{
	Addr: "127.0.0.1:6379",
	DB:   15,
})

redisCluster := txredis.NewCluster(routeTable, map[string]txredis.ScriptClient{
	"redis://127.0.0.1:6379/15": txgoredis.NewClient(client),
})
```

批量读取时不需要特殊选项：

```go
docs, exists, err := redisCluster.LoadDocuments(ctx, GDRoleTypeSpec(), keys)
```

### 场景二：云 Redis 代理或云集群 endpoint

生产环境通常会把一个 endpoint 暴露为云代理地址。yml 可以按业务 routeType 配置多个云 endpoint：

```yaml
role:
  - "redis://role-cache-a.example.com:6379"
  - "redis://role-cache-b.example.com:6379"
rank: "redis://rank-cache.example.com:6379"
```

初始化时每个 endpoint 创建一个 go-redis client，并交给 `redis.Cluster`：

```go
routeTable, err := redisRoutes.RouteTable()
if err != nil {
	return err
}

shards := map[string]txredis.ScriptClient{}
for endpoint := range redisRoutes.Endpoints() {
	opt, err := redis.ParseURL(endpoint)
	if err != nil {
		return err
	}
	shards[endpoint] = txgoredis.NewClient(redis.NewClient(opt))
}

redisCluster := txredis.NewCluster(routeTable, shards)
```

`NewCluster` 的 shard 映射使用 `txredis.ScriptClient` 强类型约束，不能把任意对象塞入 Redis endpoint。若实际读写时
adapter 没有可用 client（包括接口中包装了 nil 指针），会返回 `txredis.ErrClientNotConfigured`；空 `DeltaSet` 或空批量读取
仍是正常 no-op，不会因为未装配 client 失败。

```go
adapter := txredis.NewAdapter(nil)
err := adapter.ApplyDelete(ctx, GDRoleTypeSpec(), data.GDRoleRecordKey(1001))
// errors.Is(err, txredis.ErrClientNotConfigured) == true
```

默认批量读取会先按 endpoint 分组，再在每个 endpoint 内使用原生 `MGET`。云代理能识别 `MGET` 的参数都是 key，
可以在代理层完成跨 slot 分发和聚合。不要把跨记录批量读取包进自定义 Lua：云代理不能可靠判断 Lua 的
`ARGV` 中哪些值是 key，也不能安全拆分一个要求原子执行的脚本。

### 场景三：本地原生 Redis Cluster 或直连节点

本地 debug 如果直连原生 Redis Cluster 节点，普通跨 slot `MGET` 可能报错。此时仍使用同一份 yml，但批量读取时显式打开
`ClusterSafe`：

```go
docs, exists, err := redisCluster.LoadDocumentsWithOptions(
	ctx,
	GDRoleTypeSpec(),
	keys,
	txredis.LoadOptions{ClusterSafe: true},
)
```

`ClusterSafe` 会在 endpoint 内继续按 hash tag 分组，多次执行 `MGET` 后按输入顺序还原结果。它适合本地原生 Redis Cluster
和问题排查；生产云 Redis 默认不需要打开，避免不必要的客户端侧拆分。

### routeBy、idField 和 hash tag

`idField` 表示业务主键，`routeBy` 表示路由字段。两者相同时，`RecordKey.ID` 就足够生成 Redis key：

```text
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
// roleId = 1001
role:{1001}
role:{1001}:items
```

两者不同时，生成代码需要把 `routeBy` 字段也放进 `RecordKey.ShardKey`，让主文档和 subKey 使用同一个 hash tag：

```text
// @redis:`routeType:"arena" routeBy:"RoleId" idField:"ID" key:"arena"`
// id = 90001, roleId = 1001
arena:{1001}:90001
arena:{1001}:90001:members
```

这样同一条记录的 main key、subKey key 和版本 key 能落到同一个 Redis Cluster slot；批量读取多条记录时，不同记录仍可能落到不同 slot。

## Mongo 存储模型

Mongo adapter 当前支持：

- `ApplyDeltasWithOptions`：将生成 Store 的 `DeltaSet` 转成 Mongo update 文档。
- `LoadDocument`：按记录键读取单条文档，可带 projection 字段。
- `LoadDocuments`：按记录键批量读取并保持返回顺序。
- `ReplaceDocument`：整文档替换，供 Fusion 延迟保存回写使用。
- `ApplyDelete`：显式物理删除接口，由业务代码决定是否调用。
- `mongo.Cluster`：按 `routeType` 和 `storage.RecordKey.ShardKey` 路由到具体 Mongo adapter；批量读取会按 shard 分组并按输入顺序还原结果。

项目装配时应传入同时实现所需读写能力的官方 driver 薄适配，而不是直接传任意对象：

```go
database := txmongodriver.NewDatabase(mongoDB)
adapter := txmongo.NewAdapter(database)
```

adapter 在真正执行读、写、替换或删除时会检查对应能力。装配对象缺少 `Updater`、`Finder`、`Replacer` 或
`Deleter` 时返回 `mongo.ErrUnsupportedClient`，不会伪装成空写入或“记录不存在”。

Mongo adapter 通过统一 filter 抽象调用底层 client：默认 filter 是 `{_id: id}`；配置
`idField` 时 filter 字段由 `spec.BSONName(idField)` 推导。Mongo update 会遵守 `storage.ApplyOptions.Upsert`：
生成 Store 的 Insert/Replace 路径会启用 upsert，Update 路径会关闭 upsert，避免普通更新意外创建缺失文档。

Mongo TTL index 的字段级 `expireAfter` 会删除整条文档，不是删除单个字段。生成阶段应限制同一类型最多一个 TTL 字段。

### Mongo 配置与装配示例

Mongo 和 Redis 共用 `storage/config.RouteTableConfig` 这套 routeType 配置结构。最简单的本地配置可以把多个 routeType 指向同一个 Mongo endpoint：

```yaml
main: "mongodb://127.0.0.1:27017/txgame"
role: "mongodb://127.0.0.1:27017/txgame"
rank: "mongodb://127.0.0.1:27017/txgame"
```

如果生产环境按业务或 routeBy 区间拆分多个 Mongo endpoint，可以写成规则数组：

```yaml
role:
  - min: 1
    max: 999999
    shards:
      - "mongodb://role-a.example.com:27017/game"
      - "mongodb://role-b.example.com:27017/game"
  - min: 1000000
    shards:
      - "mongodb://role-c.example.com:27017/game"
arena:
  - "mongodb://arena-a.example.com:27017/game"
  - "mongodb://arena-b.example.com:27017/game"
```

启动时由业务工程读取 yml，创建官方 MongoDB driver client，再交给 tx 的薄适配层：

```go
var mongoRoutes storageconfig.RouteTableConfig
if err := yaml.Unmarshal(body, &mongoRoutes); err != nil {
	return err
}

routeTable, err := mongoRoutes.RouteTable()
if err != nil {
	return err
}

shards := map[string]any{}
for endpoint := range mongoRoutes.Endpoints() {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(endpoint))
	if err != nil {
		return err
	}
	dbName := databaseNameFromURI(endpoint)
	shards[endpoint] = txmongodriver.NewDatabase(client.Database(dbName))
}

mongoCluster := txmongo.NewCluster(routeTable, shards)
```

对应 import 路径：

```go
import (
	"github.com/ngnl47/tx/storage"
	storageconfig "github.com/ngnl47/tx/storage/config"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	txmongodriver "github.com/ngnl47/tx/storage/mongo/mongodriver"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)
```

上面示例中的 `databaseNameFromURI` 由业务工程实现，用来从项目配置或 URI path 中决定 database 名称。tx runtime 不读取工程 yml，也不持有具体 Mongo client 生命周期；业务工程应在服务关闭时断开官方 driver client。

批量读取时，`mongo.Cluster` 会先按 `routeType` 和 `RecordKey.ShardKey` 找到 endpoint，再对每个 endpoint 发起一批 Mongo 查询，最后按输入顺序还原：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, mongoCluster, []int64{1001, 1002, 1003})
```

如果某个类型 `routeBy != idField`，生成的 `ByID` 批量 helper 会被跳过。业务需要显式构造带 route 字段的 `RecordKey`，保证每条记录能路由到正确 Mongo endpoint：

```go
keys := []storage.RecordKey{
	data.GDArenaRecordRecordKey(90001, 1001),
	data.GDArenaRecordRecordKey(90002, 1002),
}
stores, exists, err := data.LoadGDArenaRecordStores(ctx, mongoCluster, keys)
```

### Mongo filter 和字段名

`@mongodb.idField` 使用 Go 字段名，runtime 会通过该字段的 `bson` tag 推导 Mongo filter 字段：

```go
// @mongodb:`routeType:"role" routeBy:"RoleId" idField:"RoleId" collection:"roles"`
type GDRole struct {
	RoleId int64  `json:"roleId" bson:"_id" msgpack:"roleId"`
	Name   string `json:"name" bson:"name" msgpack:"name"`
}
```

读取、更新或删除 `RoleId=470001` 时，Mongo filter 是：

```go
bson.M{"_id": int64(470001)}
```

如果字段写的是 `bson:"roleId"`：

```go
RoleId int64 `json:"roleId" bson:"roleId" msgpack:"roleId"`
```

Mongo filter 会变成：

```go
bson.M{"roleId": int64(470001)}
```

因此，`bson:"_id"` 表示“这个 Go 字段绑定 Mongo 物理主键”；`idField:"RoleId"` 表示“tx 用这个 Go 字段作为业务记录身份”。
两者可以配合，也可以分离。

### 写入示例

新建或首次缓存时，调用方显式启用 upsert：

```go
role := data.NewGDRole()
role.SetRoleId(470001)
role.SetName("alice")

err := storage.Create(ctx, mongoCluster, role.Store())
```

普通保存默认关闭 upsert：

```go
role.SetName("bob")

err := storage.Save(ctx, mongoCluster, role.Store())
```

如果 Mongo 中没有对应记录，普通保存不会新建文档。需要明确创建时使用 `storage.Create`。
Mongo adapter 会校验单文档写入结果：普通 `Save` 未匹配记录时返回 `storage.ErrNotExists`，并保留
Store 当前 Delta 供业务定位或重试；`Create` 只接受“匹配已有记录”或“新建一条记录”两种结果。
把字段写为当前已有值时，Mongo 的 `ModifiedCount` 可以为 `0`，这仍是合法成功。

集合元素删除会转成 Mongo `$unset`：

```go
role.ItemsStore().Delete(1001)

err := storage.Save(ctx, mongoCluster, role.Store())
```

对应 Mongo update 语义接近：

```go
bson.M{
	"$unset": bson.M{"items.1001": ""},
}
```

map key 级 Delta 会转换为 Mongo dot path。例如 `Items[1001]` 会写成类似 `items.1001`。
如果 `map[string]` 的 key 为空字符串，或包含 `.` / `$`，runtime 不会转义或改写业务 key，而是在 Mongo 边界降级为整 map 字段写回：

```go
role.LabelsStore().Set("foo.bar", 7)

err := storage.Save(ctx, mongoCluster, role.Store())
```

对应 Mongo update 会写 `labels` 的完整当前 map，而不是写 `labels.foo.bar`。这样 Mongo 中保存的 key 仍然是业务可读的 `foo.bar`。
如果此时生成 Store 无法读取 `Labels` 的完整当前值，保存会直接返回错误；不会以 `null` 替代整张 map。
同一轮内即使还修改了 `Labels["safe"]`，Mongo 也只写一次完整 `labels`，避免 `$set` 同时出现 `labels` 与 `labels.safe` 的父子路径冲突。

同样地，若先整体覆盖集合、结构体指针或集合元素，随后又修改其子字段，Mongo 边界会以父路径的**最终当前值**做一次整路径替换，并移除后续子路径。例如：

```go
role.SetItems(map[int32]*data.GDItem{1002: {ItemId: 1002, Quantity: 9}})
role.ItemsStore().Get(1002).SetQuantity(10)

err := storage.Save(ctx, mongoCluster, role.Store())
```

最终只生成类似 `$set: {"items": <当前完整 Items>}` 的 update，不会在同一个 Mongo update 中同时写入 `items` 与 `items.1002.quantity`。

### 读取示例

完整读取：

```go
role, exists, err := data.LoadGDRole(ctx, mongoCluster, data.GDRoleRecordKey(470001))
```

部分字段读取：

```go
role, exists, err := data.LoadGDRoleFields(ctx, mongoCluster, data.GDRoleRecordKey(470001), "Name", "Power")
```

传入字段使用 Go 字段名，runtime 会转换为 Mongo projection 字段名。上例中 Mongo projection 使用 `name` 和 `power`。
生成代码会自动补充 `idField`，避免部分读取后的对象缺少记录键。

批量读取：

```go
stores, exists, err := data.LoadGDRoleStoresByID(ctx, mongoCluster, []int64{1001, 1002, 1003})
```

`mongo.Cluster` 会先按 routeType 和 shardKey 分组，再在每个 Mongo endpoint 内使用 `$in` 查询，最后按输入顺序还原结果。

### Mongo TTL 索引

字段级 `mongodb:"index,expireAfter:0"` 表示创建 Mongo TTL 索引。TTL 到期后 Mongo 删除的是整条文档：

```go
ExpireAt int64 `json:"expireAt" bson:"expireAt" msgpack:"expireAt" mongodb:"index,expireAfter:0"`
```

这通常用于临时房间、临时任务等可物理过期的数据。不要把它理解成“删除 ExpireAt 字段”。同一类型多个 TTL 字段会产生歧义，
生成阶段应直接拒绝。

## 通用文档接口

`tx/storage` 提供 Redis、Mongo、Fusion 和生成代码共用的文档接口：

- `DocumentLoader`：按 `TypeSpec + RecordKey` 读取单条文档。
- `BatchDocumentLoader`：按 `TypeSpec + []RecordKey` 批量读取文档。
- `DocumentReplacer`：按 `TypeSpec + RecordKey` 整文档替换。
- `DocumentReplacerWithOptions`：整文档替换并接收 `storage.ApplyOptions`，用于需要 TTL/upsert 等写入语义的路径。

延迟删除发现 Redis 生命周期版本已变化时，必须重新入队；若此时延迟队列未装配，handler 返回依赖错误而不是确认旧任务，避免后续自动清理任务丢失。

这些接口传递的 `map[string]any` 统一使用 **JSON 字段名**，不是某个具体数据库的原始文档。生成的
`TypeSpec` 会提供 `DocumentFactory`；Mongo adapter 用它把公共文档物化为 GD 对象，再由 `bson` tag
写入或读回 Mongo。因此 Fusion 可以直接把 Mongo 回源结果回填 Redis，不会把 Mongo 的 `_id` 或缩写字段名
写进 Redis。

```go
// 输入定义：JSON、BSON 字段名不同。
type GDRole struct {
	RoleId int64 `json:"roleId,omitempty" bson:"_id,omitempty"`
	Power  int64 `json:"power,omitempty" bson:"p,omitempty"`
}

// 任何 DocumentLoader 的返回值都使用 JSON 名。
doc, exists, err := mongoAdapter.LoadDocument(ctx, data.GDRoleTypeSpec(), key)
// doc == map[string]any{"roleId": json.Number("1001"), "power": json.Number("47")}

// 传给 Mongo replacer 仍写为 BSON 名：{_id: 1001, p: 47}。
err = mongoAdapter.ReplaceDocument(ctx, data.GDRoleTypeSpec(), key, doc)
```

`LoadFields` 的入参始终是 Go 字段名，例如 `"Name"`；公共文档返回键仍是 JSON 名 `"name"`。
Redis 的部分读取按 main/subKey 的 key 粒度优化：当请求字段位于 main 文档中时，读取结果可以包含同一 main
文档内的其他字段。Mongo 通过 projection 通常只返回请求字段和记录键。

完整文档经过 `omitempty` 编码时，零值字段可能在 Mongo 或 Fusion 回填后的 Redis 文档中不存在；生成的 GD
对象解码后仍得到对应 Go 零值。不要把“物理文档必须出现所有零值字段”当作存储契约。

### GD 字段演进约定

持久化 GD 的字段只新增，不删除、不改名、不改变 Go 类型，也不改变 `json`、`bson`、`msgpack` 字段名。需要
迁移时，输入定义保留旧字段并用注释标记废弃，由业务在加载或写入流程中完成迁移；框架不自动迁移，也不校验
历史文档是否缺少新增字段。旧文档缺少新增字段时，Go 解码结果自然为该字段的零值。

这些接口是 Store 读取、Fusion 回源、Redis 热数据回填和延迟保存回写的共同边界。具体后端仍负责把
`RecordKey` 转成自己的底层定位方式，例如 Redis key/hash tag 或 Mongo filter。

框架层可以把 Redis/Mongo/Fusion adapter 传给 `storage.Create/Save`；业务侧更推荐使用项目 facade：

```go
role, exists, err := data.LoadGDRole(ctx, fusionStrategy, data.GDRoleRecordKey(470001))
err = storage.Save(ctx, fusionStrategy, role.Store())
```

需要写测试时，可以用内存假实现，只实现最小接口：

```go
type fakeLoader struct {
	doc map[string]any
}

func (f fakeLoader) LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error) {
	return f.doc, f.doc != nil, nil
}
```

这样生成 Store 的加载逻辑可以在不启动 Redis/Mongo 的情况下单测。

## Fusion 流程

Fusion 策略的当前保存路径：

1. 业务变更生成 `DeltaSet`。
2. Redis 同步写入热数据。
3. Redis delayed task backend 写入延迟保存任务。
4. Scheduler 到期 claim 任务。
5. StorageTaskHandler 根据任务定位重新读取 Redis 热文档。
6. Handler 将 Redis 当前完整文档 replace 到 Mongo。
7. 任务成功后通过 Lua complete 删除该任务在 Redis 队列中的状态。

延迟任务队列使用 bucket hash tag，支持多个服务实例并发消费。语义是 at-least-once，Mongo 回写必须保持存储幂等。
任务失败重试时，Scheduler 会输出结构化错误日志；失败原因不写入 Redis 任务状态，避免错误字符串占用队列内存。重试间隔由 lease 的持久化 `Attempts` 计算：`retryInitialDelay` 按 `retryMultiplier` 递增至 `retryMaxDelay`，并可施加不超过最大值的 `retryJitter`。达到上限后任务持续重试，不会因次数耗尽被丢弃。
Scheduler 循环中的 claim、complete、retry 等 backend 错误也会输出结构化错误日志；服务关闭导致的 context 结束不会额外记录错误。
Fusion delayed task queue 接受保存 action 和 Redis 热数据延迟删除 action；其他未知 action 会返回错误。入队任务与 `StorageTaskHandler` 消费入口都会统一校验 `Type`、`ID`、`Action`：空白类型/动作、nil ID 与空字符串 ID 都返回 `task.ErrInvalidDelayTask`，防止手工写入或遗留异常任务落到存储层。`Complete`、`Retry` 额外要求有效的 lease token 和正数尝试次数；格式完整但已过期的 lease 仍按幂等 no-op 处理。任务类型优先来自 `DelayedOperation.Type`，为空时使用 `DelayedOperation.Key.TypeName`。Fusion 延迟保存按类型、记录 ID、route/shard key 和 action 生成一条合并任务，不暴露额外 suffix，避免同一记录产生多条重复保存任务。延迟保存任务不携带业务对象或字段变更日志，回写时重新读取 Redis 当前热数据。显式删除仍走显式删除接口，不进入延迟队列。Redis claim 发现损坏或不完整的 payload 同样返回错误，不会把它当成可执行任务。
Fusion 保存路径要求 Redis delta applier、Mongo document loader 与延迟队列都已装配；缺少其中任一项时，非空 Delta 会在 Redis I/O 前返回错误。延迟任务执行还要求 handler、Redis 文档读取、Mongo replace 和删除任务所需的 revision/lock 组件完整可用。`Strategy.Validate()` 会汇总检查 Redis delta/document/load-lock/delete、Mongo load/delete 和延迟队列；项目启动阶段必须在 Redis/Mongo 探活后调用它。`txgame/server` 的 `NewStorage` 已执行这两步，任一依赖或连接不可达即启动失败。`Scheduler.Start` 也返回 `error`，项目启动代码必须处理，不能在 worker 未运行时继续对外服务：

```go
if err := scheduler.Start(appContext); err != nil {
	return fmt.Errorf("start delayed task worker: %w", err)
}
```

运行期不为每次 Fusion 调用额外 Ping Redis 与 Mongo，实际读写命令就是对应后端的可用性探针。Redis 热命中只访问 Redis，即使 Mongo 暂时不可达也可以返回热数据；Redis miss、`DontLoadToRedis` 直读或延迟回写实际访问 Mongo 时，Mongo 通信错误会原样返回或由 Scheduler 重试，绝不伪装成记录不存在。Redis/Mongo 的短时不可用只会让当前读写返回 `error`，不会使 `txgame` 因通信错误 panic；业务 handler 应捕获该错误并结束本次上行请求。Fusion 热写只在 Redis 同步写入与延迟任务入队成功后返回，不等待 Mongo；因此 Mongo 暂时不可达不会丢弃已经写入 Redis 的热数据或已存在的延迟任务，恢复后 worker 会继续把 Redis 当前文档回写 Mongo。

```go
role.SetName("new-name")
if err := db.Save(role); err != nil {
	// Redis 或 Mongo 暂时不可用：结束当前请求，由业务重试或返回错误码。
	return err
}
```

这条规则只针对外部通信失败。修改 viewStore、混用 `BeginChange` 与 Store 模式等本地 API 违约仍属于编程错误，应在开发期修正，不能依赖运行时吞掉 panic。

例如 Mongo 临时不可达时：

```go
role.SetName("new-name")
err := storage.Save(ctx, fusionStore, role.Store()) // Redis 写入和任务入队成功。

role, exists, err := data.LoadGDRole(ctx, fusionStore, data.GDRoleRecordKey(1001))
// Redis 命中时仍可读取；只有 Redis miss 需要回源 Mongo 时才返回 Mongo 错误。
```

生产环境的延迟回写 handler 只负责存储同步。由于任务可能因为网络抖动、进程重启或租约恢复被再次执行，Mongo 回写路径应按“同一记录的当前 Redis 热文档 replace 到 Mongo”设计为存储幂等操作。比如同一条 `GDRole` save 任务执行两次，结果应只是 Mongo 中 `roleId=1001` 的文档被相同或更新后的热数据覆盖两次；队列中不保存 `GDRole` 的中间态快照，也不按每次字段变更生成事件日志。

Redis 热数据延迟删除任务也是存储同步的一部分：handler 执行时读取当前 `@v`，再读取 Redis 热文档并回写 Mongo，最后带着读取到的 revision 删除 Redis。如果回写 Mongo 期间这条记录又发生新写入，`@v` 会变化，Lua 删除会返回当前数字版本，handler 会重新排一条延迟删除任务。`@v` 由 Redis `INCR` 生成，读取、条件删除和 Lua 返回只接受正整数；缺失或非数字结果会返回错误并保留任务，绝不会退化为无条件删除。
handler 会在这个流程外层持有同一条记录的 Redis 锁，避免多个服务实例同时执行同一条记录的回写/删除窗口。

运维侧应关注 Scheduler 错误日志、重试次数和队列积压时间。如果某类任务持续失败，优先排查对应类型的 Redis 热文档是否能正常反序列化、Mongo 路由/过滤条件是否正确、以及目标集合是否可写。队列中的失败原因不会写入 Redis 状态；需要更完整的告警和指标时，应通过项目自己的日志、监控或后续可插拔 observer 接入。

Fusion 读取会先查 Redis，未命中时才回源 Mongo；Redis 命中不会额外探测 Mongo。单条回源会先获取记录级 Redis 锁，并在锁内再次读取 Redis：如果第二次读取命中，直接返回 Redis 热数据；只有第二次仍未命中时，才从 Mongo 全量读取文档，写回 Redis 热数据并使用 `delayDelTime` 续期，然后按请求字段返回结果。批量读取先合并第一次 Redis 查询；当 Mongo reader 支持批量读取时，所有 miss 会先通过一次 Mongo 全文批量读取取得候选文档，再逐条获取记录锁并二次读取 Redis。锁内已经被其他请求回填的记录直接返回最新 Redis 文档，仍未命中时才写入该条候选 Mongo 文档；因此批量回源不会用旧候选覆盖另一条回填完成后的热数据。该路径要求 `delayDelTime` 必须大于 0，避免生成没有 TTL 的 Redis 热数据；同时必须已装配带写入选项的 Redis 文档替换器和记录锁，缺少任一组件时会返回 `fusion.ErrStrategyDependenciesNotConfigured`，不会降级成只读 Mongo。显式使用 `LoadOptions.DontLoadToRedis` 时，才直接从 Mongo 按请求字段读取且不回填 Redis，适合低频只读场景。

部分字段读取的优化主要发生在 Redis 热数据层：运行时按 main/subKey key 粒度减少 Redis 读取，随后按字段过滤返回文档；Redis 和 Mongo 中保存的仍是全量数据。只请求 subKey 字段时，Redis 可以只读取对应 subKey 文档，不要求主文档同时命中。

Fusion 显式删除不会进入延迟队列。删除路径会先确认 Redis 与 Mongo 删除器均已装配；缺少任一组件时返回 `fusion.ErrStrategyDependenciesNotConfigured`，且不会删除任何一侧。检查通过后先删除 Redis 热数据，再立即删除 Mongo 文档；如果只绕过 Fusion 删除 Mongo，而 Redis 热数据和延迟保存任务仍存在，后续保存任务可能按 Redis 当前数据重新写回 Mongo。

### Fusion 初始化示例

Fusion 需要三类组件：

- Redis：同步热数据读写。
- Mongo：未命中回源、延迟回写和显式删除。
- DelayQueue：把“这条记录稍后保存到 Mongo”写入延迟任务队列。

示例：

```go
fusionStore := fusion.Strategy{
	Redis:         redisCluster,
	RedisLoader:   redisCluster,
	RedisDeleter:  redisCluster,
	RedisReplacer: redisCluster,
	RedisLocker:   redisCluster,
	MongoLoader:   mongoCluster,
	MongoDeleter:  mongoCluster,
	MongoQueue:    delayQueue,
}
```

业务写入：

```go
role.SetName("bob")

err := storage.Save(ctx, fusionStore, role.Store())
```

执行后：

1. Redis 立即写入热数据。
2. DelayQueue 记录一条保存任务，任务到期时间来自 `delaySaveTime`。
3. 如果配置了 `delayDelTime > 0`，DelayQueue 还会记录一条 Redis 热数据延迟删除任务，到期时间为 `delaySaveTime + delayDelTime`。
4. 本次调用不等待 Mongo 立即写入。

### 延迟回写任务

延迟任务不保存业务对象快照，也不保存字段变更日志。任务只保存记录定位信息：

```text
type = GDRole
id = 470001
shardKey = 470001
action = save
due = now + delaySaveTime
```

Scheduler 到期 claim 任务后，`StorageTaskHandler` 会重新读取 Redis 当前热数据，再整文档 replace 到 Mongo。
这意味着 10 分钟内同一条 `GDRole` 修改多次，Mongo 回写只需要使用 Redis 中的最新状态。

如果配置了 `delayDelTime`，写入时还会额外排一条 Redis 热数据延迟删除任务：

```text
type = GDRole
id = 470001
shardKey = 470001
action = delete
due = now + delaySaveTime + delayDelTime
```

删除任务到期后，handler 会先把 Redis 当前热数据回写 Mongo，再按 `<mainKey>@v` 比对删除 Redis。比如 `role:{470001}@v` 在任务开始时是 `5`，Mongo 回写期间业务又调用 `role.SetName("new")` 导致 `@v` 变成 `6`，删除 Lua 会返回当前版本 `6`，handler 不会删除 Redis，而是重新排一条 `delete` 任务。

### 多实例消费

Redis delayed task backend 使用 ZSet/hash 保存 due、payload、busy、token、attempts 等状态，并通过 Lua 保证
claim/complete/retry 的原子性。多个服务实例可以同时跑 Scheduler：

```text
server-a claim bucket 1 中的一批任务
server-b claim bucket 2 中的一批任务
server-c claim bucket 1 中租约过期后可恢复的任务
```

整体语义是 at-least-once：同一任务可能因为网络失败、进程重启或租约恢复被再次执行。因此 Mongo 回写必须是幂等的。
当前设计是“按记录键把 Redis 当前热文档 replace 到 Mongo”，重复执行只会覆盖同一条记录，不应产生额外业务副作用。

### 读取和回填

默认读取顺序：

1. 先读 Redis。
2. Redis 命中，直接返回。
3. Redis 未命中，获取记录级 Redis 锁。
4. 锁内再次读 Redis。
5. 第二次 Redis 命中，直接返回。
6. 第二次 Redis 仍未命中，读 Mongo 完整文档。
7. 将 Mongo 完整文档写回 Redis，并使用 `delayDelTime` 作为 Redis TTL。
8. 按请求字段返回给调用方。

代码示例：

```go
role, exists, err := data.LoadGDRole(ctx, fusionStore, data.GDRoleRecordKey(470001))
```

部分字段读取：

```go
role, exists, err := data.LoadGDRoleFields(ctx, fusionStore, data.GDRoleRecordKey(470001), "Name", "Power")
```

即使只请求 `Name` 和 `Power`，Redis miss 后默认也会从 Mongo 读取完整文档并回填 Redis。这样 Redis 继续承担热数据缓存职责。

这个锁用于保护并发回填。例如 `game-a` 和 `game-b` 同时读取 `role:{1001}` 未命中；`game-a` 先拿到锁、从 Mongo 读取并回填 Redis；`game-b` 随后拿到锁时会先二次读取 Redis，发现已经有热数据，于是不会再用自己稍后读到的 Mongo 快照覆盖 Redis。

如果明确是低频只读，不希望回填 Redis，可以使用 runtime option：

```go
doc, exists, err := fusionStore.LoadDocumentWithOptions(
	ctx,
	data.GDRoleTypeSpec(),
	data.GDRoleRecordKey(470001),
	fusion.LoadOptions{DontLoadToRedis: true},
	"Name",
)
```

这个路径只从 Mongo 读取请求字段，不写 Redis。

## TTL 与删除

`delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写。Fusion 延迟保存队列使用这个时间。

`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据；写入后会续期。显式传入 `ApplyOptions.TTL` 时，以显式值为准。Fusion 默认 Mongo 回源并回填 Redis 时必须配置该值。

`delayDelTime` 不代表 Mongo 删除延迟，也不会触发 Mongo 删除。业务是否执行 Mongo 物理删除，仍由显式 delete 接口和业务代码决定。

常见配置：

```go
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
```

含义：

| 字段 | 含义 |
| --- | --- |
| `delaySaveTime:"600"` | Redis 热数据变更后，约 10 分钟触发一次 Mongo 回写。 |
| `delayDelTime:"86400"` | Redis 热数据写入后 24 小时过期；再次写入会续期。 |

如果玩家持续在线且频繁修改，Redis 热数据会不断续期；Mongo 回写按延迟任务节奏合并。玩家长时间不活跃后，Redis key 到期释放内存；
再次加载时从 Mongo 读取完整数据并重新缓存到 Redis。

显式删除：

```go
err := role.Store().ApplyDelete(ctx, fusionStore)
```

Fusion 显式删除会：

1. 先取得该记录的 Redis 锁；延迟保存和延迟删除任务也使用同一把锁。
2. 删除 Redis 主文档、subKey 文档和版本键。
3. 立即删除 Mongo 文档。
4. 不写入延迟保存队列。

例如某个延迟保存任务已经到期时，显式删除会与它串行：保存先取得锁时，删除会在保存完成后删除 Mongo；删除先取得锁时，保存任务稍后读取 Redis 会发现记录不存在并跳过回写。因此不会出现“删除完成后再被旧热数据重新写回 Mongo”的交错。

Redis 热数据还维护一个生命周期版本键 `<mainKey>@v`，例如 `role:{1001}@v`。这个键不是业务字段，也不会写入 Mongo；它只用于 Redis 热数据过期/删除窗口的并发保护。每次热写都会递增它，并使用与 main/subKey 相同的有效 TTL；热数据持久化时它也会持久化，因此不会在热数据仍有效时提前失去删除窗口保护。运行时的带版本删除原语会在 Lua 中先比对 `@v`，只有版本一致才删除 main/subKey/version key；如果版本已变化，说明这条热数据发生过新写入，删除方应跳过本次删除并稍后重试。所有附属 key 都从 mainKey 派生：

| Redis key | 示例 | 用途 |
| --- | --- | --- |
| mainKey | `role:{1001}` | Redis 主文档。 |
| subKey | `role:{1001}:items` | 字段级 `redis:"subKey:items"` 拆出的子文档。 |
| version | `role:{1001}@v` | Redis 热数据生命周期版本。 |
| lock | `role:{1001}@lock` | Redis miss 回填时的记录级锁。 |

这些 key 共享 `{1001}` 这个 hash tag；当 `routeBy != idField` 时也会共享 routeBy 生成的 hash tag，例如 `arena_record:{470001}:90001`、`arena_record:{470001}:90001@v` 和 `arena_record:{470001}:90001@lock`。

如果业务希望“逻辑删除”，推荐在业务数据里定义字段：

```go
role.SetDeleted(true)
err := db.Save(role)
```

框架不会替业务决定逻辑删除字段，也不会把 `delayDelTime` 解释成 Mongo 删除时间。

## 测试

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

## 排查清单

### 读取不到数据

- 确认生成包已经调用 `RegisterStorageRuntime` 或对应的 `SetDefault*Reader`。
- 确认 `@storage.mode` 与实际注册的 reader 匹配；Fusion 类型默认读 Fusion reader，Redis 类型默认读 Redis reader，Mongo 类型默认读 Mongo reader。
- 确认 `idField` 和 `routeBy` 使用 Go 字段名，例如 `RoleId`，不是 `roleId`、`_id` 或 msgpack 名。
- 如果 `routeBy != idField`，确认调用的是带 route 字段参数的 helper，或手动构造了带 `ShardKey` 的 `RecordKey`。
- Fusion 从 Mongo 回填 Redis 时要求 `delayDelTime` 大于 0；如果缺失，默认回填路径会失败。
- 部分读取传入的是 Go 字段名或生成常量，例如 `data.GDRoleGoFieldName`，不是 JSON/BSON/Msgpack 字段名。

### Redis 中没有热数据

- Redis mode 或 Fusion mode 写入后才会产生 Redis 热数据；Mongo-only 类型不会写 Redis。
- Fusion 读取 Mongo 后默认会回填 Redis，但 `LoadOptions.DontLoadToRedis` 会关闭回填。
- 检查 Redis routeType 是否能在 yml 路由表中找到 endpoint。
- 检查 `RecordKey.ShardKey` 是否正确；`routeBy != idField` 时缺少 ShardKey 会导致路由或 hash tag 不符合预期。
- 检查 `delayDelTime`，Redis key 可能已经按 TTL 过期。

### Mongo 没有立即更新

- Fusion 写入路径先同步写 Redis，再通过延迟任务回写 Mongo；这不是立即写 Mongo。
- `delaySaveTime` 决定保存任务到期时间，同一条记录在延迟窗口内多次修改会合并为读取 Redis 当前热文档后 replace Mongo。
- 确认 Redis delayed task backend 和 Scheduler 已在服务中启动。
- 确认 Scheduler 日志没有持续 claim、retry、complete 错误。
- 如果业务绕过 Fusion 直接删 Mongo，而 Redis 热数据和延迟保存任务还在，后续回写可能重新写入 Mongo。

### 更新没有字段变化

- 直接字段赋值不会标脏，例如 `role.Name = "bob"` 只改内存，不会进入 Store dirty。
- 使用生成代理方法：`role.SetName("bob")`、`role.SetItems(...)`、`role.TouchItems()`。
- 如果已经通过集合 wrapper 生成了 key/index 级 Delta，再调用 `TouchItems()` 或 `AllTouch()` 时，顶层 dirty 会覆盖并清理更细的路径记录。
- 保存成功后如果继续复用对象，调用 `ClearChanges()` 或 Store 的 `AllUnTouch()` 清掉本轮 dirty。

### 批量读取报 Redis cross-slot

- 默认批量读取面向单节点或云 Redis 代理 endpoint，直接使用原生 `MGET`。
- 本地原生 Redis Cluster 或直连 cluster 节点调试时，使用 `redis.LoadOptions{ClusterSafe: true}`。
- 不要把跨记录批量读取包进 Lua；Lua 脚本的 `KEYS` 必须在同一个 slot，云代理也不能可靠拆分自定义脚本。
