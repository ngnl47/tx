# tx 使用手册

本文记录 `github.com/ngnl47/tx` 当前已实现并验证的运行时能力。手册会随实现持续维护；仍未决策的行为会明确标注。

## 定位

`tx` 是运行时基础库，负责：

- 解析和保存类型元数据。
- 追踪字段级变更。
- 将 `ChangeSet` 落地到 Redis、Mongo 或 Fusion 策略。
- 提供 Redis 延迟任务 backend 和 Fusion 后台回写所需组件。

生成代码依赖 `tx`，`tx` 不依赖生成器工程。

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
RoleId int64 `comment:"角色编号" json:"roleId,omitempty" redis:"subKey:items"`
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

字段别名类 tag 不属于内置注解解析结果，由 AST 扫描直接读取：

```text
alias tag；或 json/bson/msgpack tag；否则使用 lower-camel Go 字段名
```

`json:"roleId,omitempty"`、`bson:"roleId,omitempty"` 或 `msgpack:"roleId,omitempty"` 都只取逗号前的
`roleId` 作为统一 alias。脚手架输入中 `alias` 不能和 `json/bson/msgpack` 混用；如果
`json`、`bson`、`msgpack` 多个 tag 同时声明字段名，字段名必须一致。

### 内置注解字段

| 位置 | 注解 | 可用字段或值 | 说明 |
| --- | --- | --- | --- |
| type | `@storage` | `mode`、`delaySaveTime`、`saveTime`、`delaySave`、`delayDelTime`、`delTime`、`delayDelete`、`memoryCache` | 存储模式和时间策略。`mode` 支持 `redis`、`mongo`、`fusion`。纯数字时间按秒解析，也支持 Go duration，例如 `10m`。 |
| type | `@redis` | `routeType`、`shardType`、`config`、`routeBy`、`shardBy`、`idField`、`id`、`key`、`storeType`、`omitempty` | Redis 根对象配置。`routeType/shardType/config` 等价；`routeBy/shardBy` 等价；`idField/id` 等价。`omitempty` 可写为参数或 flag。 |
| type | `@mongodb` | `routeType`、`shardType`、`config`、`routeBy`、`shardBy`、`idField`、`id`、`collection`、`c`、`col`、`enableSharding`、`useMongoObjectID` | Mongo 根对象配置。`collection/c/col` 等价。`enableSharding`、`useMongoObjectID` 可写为参数或 flag。 |
| field | `redis` | `subKey`、`ignore`、`fullUpdate` | 字段级 Redis 配置。`ignore`、`fullUpdate` 可写为 flag 或布尔参数。 |
| field | `mongodb` | `index`、`unique`、`ignore`、`order`、`indexType`、`weight`、`expire`、`expireAfter` | 字段级 Mongo 配置。`index`、`unique`、`ignore` 可写为 flag 或布尔参数；`expire/expireAfter` 表示 Mongo TTL 索引，命中后删除整条文档。 |

`@mongodb` 默认按 Mongo `_id` 过滤文档，适合把业务 `idField` 写入 `_id` 的数据。开启
`useMongoObjectID` 后，Mongo 保留驱动或数据库自己的 `_id`，tx runtime 的查询、更新、替换、
删除会改为按 `idField` 指定的业务字段过滤，例如 `{roleId: 1001}`。这只影响 Mongo 过滤条件，
不改变 ChangeSet 字段 alias，也不要求业务结构把字段命名成 `_id`。

以 `RoleId = 1001`、`Name = "alice"` 为例，常见组合如下：

| 配置组合 | Mongo 文档结构 | runtime 过滤条件 | 说明 |
| --- | --- | --- | --- |
| 默认，业务字段正常保存为 `roleId` | `{"_id": 1001, "roleId": 1001, "name": "alice"}` | `{_id: 1001}` | 可用，但 `_id` 和业务字段会同时存在且值相同。 |
| 默认，业务字段绑定到 `bson:"_id"` | `{"_id": 1001, "name": "alice"}` | `{_id: 1001}` | Mongo 物理主键直接使用业务 ID。 |
| 开启 `useMongoObjectID`，业务字段正常保存为 `roleId` | `{"_id": ObjectID, "roleId": 1001, "name": "alice"}` | `{roleId: 1001}` | Mongo `_id` 和业务 ID 分离，这是保留 Mongo ObjectID 时的推荐形态。 |
| 开启 `useMongoObjectID`，但业务字段绑定到 `bson:"_id"` | `{"_id": 1001, "name": "alice"}` | `{roleId: 1001}` | 冲突形态：runtime 会按 `roleId` 查，但文档没有 `roleId`。 |

因此，`bson:"_id"` 是 Mongo 序列化层的字段绑定；`mongodb.idField` 是 tx 存储框架层的业务记录身份。
两者可以配合，但不能互相替代。开启 `useMongoObjectID` 时，业务 ID 必须作为普通字段保留在 Mongo
文档中，不能再把同一个字段绑定到 `_id`。

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
每次输出时都会读取当前 `Default()`，再附加 `logger=<name>` 属性派生一次日志调用，避免包级变量早于项目日志初始化时绑定到旧 logger。
因此，项目启动后调用 `log.SetDefault(logger)`，此前已经声明的 `RoleLog` 也会使用新的 default logger。

## 元数据与字段别名

字段别名用于生成 JSON/BSON/Msgpack tag，也用于 `ChangeSet` 路径、Redis 文档字段名和 Mongo 字段名。

当前 AST 扫描别名优先级：

```text
alias tag > json/bson/msgpack tag > lower-camel Go 字段名
```

例如：

```go
Key int64 `json:"k"`
```

会把字段 alias 解析为 `k`。序列化后使用 `{"k":...}`，反序列化时 Go 标准库会根据 `json` tag 回填到 `Key` 字段。
如果没有 `alias` 和 `json`，但写了 `bson:"k"` 或 `msgpack:"k"`，同样会把统一字段 alias 解析为 `k`。
当 `json`、`bson`、`msgpack` 同时声明字段名时，脚手架侧要求这些名称一致，避免同一字段在运行时路径和序列化路径上分裂。

## ChangeSet

`change.Tracker` 记录字段级变更，字段可以通过编号解析为 alias：

- `TouchField` 记录普通 set。
- `TouchFieldPath` 记录子路径 set。
- `ReplaceFieldPath` 记录整体或子路径 replace。
- `UnsetFieldPath` 记录 unset。
- `FieldChange.FullPath(resolver)` 将字段编号和子路径拼成完整 alias path。

同一保存周期内，父级变更会覆盖同目标下已有或后续追加的子路径变更。通过字段编号记录时，例如先记录
`TouchFieldPath(fieldItems, Path{"1001", "quantity"})`，再记录 `TouchField(fieldItems, items)`，最终
`ChangeSet` 只保留 `items` 整字段。通过完整路径记录时同理，例如先记录
`Touch(Path{"items", "1001", "quantity"}, 3)`，再记录 `Touch(Path{"items"}, items)`，最终也只保留
`items`。这用于避免大集合字段在整字段保存时继续保留大量 key 级 path 记录。

`AllTouch()` 表示整对象需要全量落地，会直接清空已有字段级 `ChangeSet`。例如已经记录了
`items.1001.quantity`、`name` 等部分标脏后再执行 `AllTouch()`，后续只需要根据 `AllDirty()` 走全量保存，
不再保留这些局部变更。

生成代码应传入生成的 `FieldResolver`，避免运行时反射字段名。

## Redis 存储模型

Redis 使用 string JSON 文档模型。

当前主线选择普通 Redis string 存储 JSON 文档，并通过 Lua 在写入时合并或替换文档。RedisJSON/ReJSON
可以作为后续可选 backend 单独评估，但不是当前默认路径；引入前需要确认目标云 Redis、测试环境和
Redis Cluster 行为都支持对应 JSON 命令。

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

## RouteTable

`storage.RouteTable` 是 Redis 和 Mongo 共用的运行时路由表，可同时作为 `redis.Cluster` 和 `mongo.Cluster` 的 router 使用。

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
```

支持的 routeType 值形态：

- 标量 endpoint：归一化为单 shard 规则。
- endpoint 数组：归一化为一个多 shard 规则。
- 规则数组：保留 `min/max/shards` 区间规则。

配置对象可直接构造运行时路由表：

```go
table, err := redisConfig.RouteTable()
endpoints := redisConfig.Endpoints()
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

## Mongo 存储模型

Mongo adapter 当前支持：

- `ApplyChangesWithOptions`：将 `ChangeSet` 转成 Mongo update 文档。
- `LoadDocument`：按记录键读取单条文档，可带 projection 字段。
- `LoadDocuments`：按记录键批量读取并保持返回顺序。
- `ReplaceDocument`：整文档替换，供 Fusion 延迟保存回写使用。
- `ApplyDelete`：显式物理删除接口，由业务代码决定是否调用。
- `mongo.Cluster`：按 `routeType` 和 `storage.RecordKey.ShardKey` 路由到具体 Mongo adapter；批量读取会按 shard 分组并按输入顺序还原结果。

Mongo adapter 通过统一 filter 抽象调用底层 client：默认 filter 是 `{_id: id}`，开启
`useMongoObjectID` 时 filter 是 `{idField: id}`。Mongo update 会遵守 `storage.ApplyOptions.Upsert`：
生成 Store 的 Insert/Replace 路径会启用 upsert，Update 路径会关闭 upsert，避免普通更新意外创建缺失文档。

Mongo TTL index 的字段级 `expireAfter` 会删除整条文档，不是删除单个字段。生成阶段应限制同一类型最多一个 TTL 字段。

## 通用文档接口

`tx/storage` 提供 Redis、Mongo、Fusion 和生成代码共用的文档接口：

- `DocumentLoader`：按 `TypeSpec + RecordKey` 读取单条文档。
- `BatchDocumentLoader`：按 `TypeSpec + []RecordKey` 批量读取文档。
- `DocumentReplacer`：按 `TypeSpec + RecordKey` 整文档替换。
- `DocumentReplacerWithOptions`：整文档替换并接收 `storage.ApplyOptions`，用于需要 TTL/upsert 等写入语义的路径。

这些接口是 Store 读取、Fusion 回源、Redis 热数据回填和延迟保存回写的共同边界。具体后端仍负责把
`RecordKey` 转成自己的底层定位方式，例如 Redis key/hash tag 或 Mongo filter。

## Fusion 流程

Fusion 策略的当前保存路径：

1. 业务变更生成 `ChangeSet`。
2. Redis 同步写入热数据。
3. Redis delayed task backend 写入延迟保存任务。
4. Scheduler 到期 claim 任务。
5. StorageTaskHandler 根据任务定位重新读取 Redis 热文档。
6. Handler 清理 nil 删除标记后 replace Mongo 文档。
7. 任务成功后通过 Lua complete 删除该任务在 Redis 队列中的状态。

延迟任务队列使用 bucket hash tag，支持多个服务实例并发消费。语义是 at-least-once，Mongo 回写必须保持存储幂等。
任务失败重试时，Scheduler 会输出结构化错误日志；失败原因不写入 Redis 任务状态，避免错误字符串占用队列内存。
Scheduler 循环中的 claim、complete、retry 等 backend 错误也会输出结构化错误日志；服务关闭导致的 context 结束不会额外记录错误。
Fusion delayed task queue 和 handler 都只接受保存 action；delete 或其他未知 action 会返回错误。入队时必须带有明确的类型和记录 ID，避免无效任务进入 Redis 队列；handler 消费端也会拒绝缺少记录 ID 的任务，防止手工写入或遗留异常任务落到存储层。任务类型优先来自 `DelayedOperation.Type`，为空时使用 `DelayedOperation.Key.TypeName`。Fusion 延迟保存按类型、记录 ID、route/shard key 和 action 生成一条合并任务，不暴露额外 suffix，避免同一记录产生多条重复保存任务。延迟保存任务不携带 ChangeSet，回写时重新读取 Redis 当前热数据。删除必须走显式删除接口，不进入延迟队列。
Scheduler 的 `Start`、`RunOnce` 和 `Schedule` 在自身或 backend 未配置时都是 no-op，便于本地测试或按环境关闭后台回写组件；生产环境仍应配置 Redis delayed task backend。

生产环境的延迟回写 handler 只负责存储同步。由于任务可能因为网络抖动、进程重启或租约恢复被再次执行，Mongo 回写路径应按“同一记录的当前 Redis 热文档 replace 到 Mongo”设计为存储幂等操作。比如同一条 `GDRole` save 任务执行两次，结果应只是 Mongo 中 `roleId=1001` 的文档被相同或更新后的热数据覆盖两次；队列中不保存 `GDRole` 的中间态快照，也不按每次字段变更生成事件日志。

运维侧应关注 Scheduler 错误日志、重试次数和队列积压时间。如果某类任务持续失败，优先排查对应类型的 Redis 热文档是否能正常反序列化、Mongo 路由/过滤条件是否正确、以及目标集合是否可写。队列中的失败原因不会写入 Redis 状态；需要更完整的告警和指标时，应通过项目自己的日志、监控或后续可插拔 observer 接入。

Fusion 读取会先查 Redis，未命中时回源 Mongo。默认回源会从 Mongo 全量读取文档，写回 Redis 热数据并使用 `delayDelTime` 续期，然后按请求字段返回结果；该路径要求 `delayDelTime` 必须大于 0，避免生成没有 TTL 的 Redis 热数据。显式使用 `LoadOptions.DontLoadToRedis` 时，才直接从 Mongo 按请求字段读取且不回填 Redis，适合低频只读场景。

部分字段读取的优化主要发生在 Redis 热数据层：运行时按 main/subKey key 粒度减少 Redis 读取，随后按字段过滤返回文档；Redis 和 Mongo 中保存的仍是全量数据。只请求 subKey 字段时，Redis 可以只读取对应 subKey 文档，不要求主文档同时命中。

Fusion 显式删除不会进入延迟队列。删除路径应先删除 Redis 热数据，再立即删除 Mongo 文档；如果只绕过 Fusion 删除 Mongo，而 Redis 热数据和延迟保存任务仍存在，后续保存任务可能按 Redis 当前数据重新写回 Mongo。

## TTL 与删除

`delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写。Fusion 延迟保存队列使用这个时间。

`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据；写入后会续期。显式传入 `ApplyOptions.TTL` 时，以显式值为准。Fusion 默认 Mongo 回源并回填 Redis 时必须配置该值。

`delayDelTime` 不代表 Mongo 删除延迟，也不会触发 Mongo 删除。业务是否执行 Mongo 物理删除，仍由显式 delete 接口和业务代码决定。

## 测试

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./... -count=1
```
