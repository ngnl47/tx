# Tx storage metadata implementation plan

本文是 `github.com/ngnl47/tx` 中重写 legacy framework tag storage chain 的实施计划。

目标不是兼容式修补 legacy framework，而是把旧链路抽象成一个新的 Go module 能力：

- 使用 Go 注释作为元数据主入口，保持类型定义和存储定义靠近。
- 使用 Go AST 读取 Go 类型结构和注释，建立注释元数据与 Go 类型之间的绑定关系。
- 语法形态沿用紧凑注解写法；当前不保留旧注解迁移器，输入侧统一使用新 Tx 语义。
- 不支持独立 schema 文件；存储元数据只来自 Go 注释、struct tag 和 AST 扫描结果。
- 运行时元数据只允许初始化期注册；不支持隐式运行时追加 tag。
- `fusion` 作为公开存储模式名；旧 `redisDelayMongoDB` 不再作为运行时或生成器输入。
- Redis key 默认使用 `shardBy` 作为 Redis Cluster hash tag。
- Fusion 延迟任务抽象成可替换任务后端。
- 测试用例只放在 `tests/` 目录下。

## Module Layout

建议新增包结构：

```text
tx/
  meta/             # 元数据模型、registry、annotation 输出模型
  anno/             # Go 注释中的 type-level tag 解析、规范化、校验
  astscan/          # Go AST 扫描器，提取 package/type/field/method/comment 信息
  storage/          # 存储抽象：Store mode、route、key、options、runtime metadata
  storage/redis/    # Redis key builder、route resolver、后续 Redis adapter
  storage/mongo/    # Mongo collection/index/sharding metadata、后续 Mongo adapter
  storage/fusion/   # Fusion 策略与延迟任务抽象
  storage/task/     # Delay task backend interface 和内存实现
  gen/              # 代码生成入口或生成辅助
  tests/            # 所有测试文件，仅此目录下生成 *_test.go
```

当前仓库已有 `gen/`、`tag/`、`util/`、`tests/`。新实现可逐步新增上面的包；如后续确认 `tag/` 可承载注释 tag 相关能力，也可以把 `anno/` 合并进去，但建议第一版先保持边界清晰。

## Annotation Direction

主入口放在 Go 注释中，并由 Go AST 绑定到准确的 type/field 节点。语法形态保留 legacy framework 风格的 `// @name:"k:v,flag"`，这样 struct 级注释 tag 与 field 级 Go struct tag 使用同一种参数格式。

第一版建议支持：

```go
// @storage:"mode:fusion,delaySaveTime:600s,delayDelTime:24h"
// @redis:"routeType:role,routeBy:roleId,idField:roleId,key:role,omitempty"
// @mongodb:"routeType:role,routeBy:roleId,idField:roleId,collection:roles,enableSharding"
type GDRole struct {
    RoleId int64 `json:"roleId" mongodb:"index,unique"`
}
```

字段级继续使用 Go 原生 struct tag：

```go
Items map[int32]*GDItem `redis:"subKey:items"`
RoleId int64 `mongodb:"index,unique"`
```

注释语法设计原则：

- 使用 `// @<tagName>:"<content>"` 前缀，保持与 Go `reflect.StructTag` 相近的写法。
- type-level tag 第一版支持：`storage`、`redis`、`mongodb`。
- field-level tag 继续读取原生 struct tag，第一版支持：`redis`、`mongodb`。
- 参数继续使用 `key:value` 和 flag 混合格式，延续 legacy framework 的紧凑写法。
- parser 只负责理解单条 tag 内容；注释和 struct/field 的绑定必须由 Go AST 完成。
- 新语义使用 `mode:fusion`；旧 `mode:redisDelayMongoDB` 不再支持。
- 时间字段使用 Go duration 风格字符串：`600s`、`10m`、`24h`。
- Redis hash tag 默认使用 `routeBy` 指向的字段，不再暴露旧库全局 `UseShardByInKey=false` 兼容行为。
- 旧字段名如 `routeBy`、`idField` 可以保留为 canonical 写法；也可以在后续版本引入更短别名，但第一版以降低迁移成本为优先。

## Schema Direction

独立 schema 文件不再作为支持范围。第一版只维护一条元数据链路：

```text
Go 注释 / struct tag -> anno -> astscan -> meta -> storage/gen
```

设计原则：

- 不提供 YAML/JSON schema loader。
- 不提供 schema 导入、导出或冲突合并。
- 外部工具若需要消费元数据，应基于后续 generator 输出，而不是手写 schema 文件。

## Phase 1: Metadata Core

目标：建立新库的核心元数据模型和只允许初始化期注册的 registry。

建议实现：

- `meta.TypeRef`
  - `ImportPath string`
  - `Package string`
  - `Name string`
  - `FullName string`
- `meta.Field`
  - `Name string`
  - `Alias string`
  - `Type string`
  - `Exported bool`
- `meta.Type`
  - `Ref TypeRef`
  - `Fields []Field`
  - `Methods []Method`
- `meta.Registry`
  - `RegisterType(meta.TypeSpec) error`
  - `Freeze()`
  - `Lookup(typeRef) (TypeSpec, bool)`
  - 注册后 freeze，freeze 后不允许隐式修改。
- `meta.Overlay`
  - 预留显式覆盖机制，但第一版可以只定义接口，不启用运行期修改。

验收：

- Registry 支持初始化期注册、冻结、查找。
- Freeze 后注册或覆盖返回明确错误。
- 错误包含 type 名和字段名，便于迁移诊断。

测试：

- `tests/meta_registry_test.go`
- 覆盖注册、重复注册、冻结后拒绝、lookup。

## Phase 2: Annotation Parser And Validator

目标：解析 Go 注释中的 type-level tag 和字段上的 struct tag，输出强类型配置。

建议实现：

- `anno.ParseTypeTagLine(text string) (*anno.Directive, error)`
- `anno.ParseTypeDoc(comments []string) ([]anno.Directive, error)`
- `anno.ParseStructTag(tag reflect.StructTag, scope anno.Scope) ([]anno.Directive, error)`
- `meta.Normalize(directives []anno.Directive) (meta.TypeSpec, error)`
- `meta.Validate(spec, astType) error`
- 强类型配置：
  - `StorageSpec`
  - `RedisSpec`
  - `MongoSpec`
  - `FusionSpec`
  - `FieldSpec`
  - `IndexSpec`

校验规则：

- type 必须能在 AST 扫描结果中找到。
- `redis.idField`、`redis.routeBy`、`mongodb.idField`、`mongodb.routeBy` 必须匹配字段 alias。
- `storage.mode` 第一版支持：`redis`、`mongo`、`fusion`。
- `fusion` 模式必须同时具备 redis 和 mongo 配置。
- `redis.key` 缺省可由类型名推导。
- `mongo.collection` 缺省可由类型名或命名策略推导，但建议第一版要求显式配置。
- 未识别参数必须报错，不静默忽略。

验收：

- 能解析 `// @storage:"..."`、`// @redis:"..."`、`// @mongodb:"..."`。
- 能解析字段级 `redis:"..."`、`mongodb:"..."` struct tag。
- 能产生稳定、可序列化的 `TypeSpec`。
- 校验错误可以定位到 type/field/property。

测试：

- `tests/anno_parse_test.go`
- `tests/anno_validate_test.go`
- 测试文件中的 fixture 使用内联字符串或写入 `t.TempDir()`，不要在包目录放测试 fixture。

## Phase 3: Go AST Scanner

目标：替换旧 `gentag` 的文本扫描，用 Go AST 提取 Go 类型结构并把注释 tag 绑定到准确节点。

建议实现：

- `astscan.ScanDir(root string, opts Options) (*astscan.PackageSet, error)`
- `astscan.ScanFiles(files []string, opts Options) (*astscan.PackageSet, error)`
- 提取：
  - package import path
  - struct type
  - exported/unexported field
  - embedded field
  - field tags: `json`、`bson`、`alias`
  - method receiver 和 method 名
  - type doc comment 中的 `// @storage` / `// @redis` / `// @mongodb`
  - field struct tag 中的 `redis` / `mongodb`
- alias 规则第一版建议：
  - 显式 tag field name 优先。
  - Go field alias 推导：`alias` tag > `json` tag > lower-camel Go field name。

验收：

- 能扫描 Go module 内指定目录。
- 能解析嵌入字段和常规字段。
- 能稳定输出 type/field alias map。
- 能将 type-level 注释 tag 绑定到准确的 AST type 节点。
- 能将 field-level struct tag 绑定到准确的 AST field 节点。

测试：

- `tests/astscan_test.go`
- 使用 `t.TempDir()` 创建临时 Go module 或临时 package 文件。

## Phase 4: Legacy Migrator Removed

结论：`tx` 和 `txgen` 是全新项目，不承担旧注解兼容职责。此前的 `migrate` 包属于项目初期过渡产物，已从 runtime 中移除。

当前约束：

- 输入侧统一使用新的 Tx 注解语义。
- 不支持 `mode:redisDelayMongoDB` 等旧模式别名。
- 不保留迁移报告、迁移 API 或旧注解测试。
- 如后续确实需要迁移工具，应作为独立工具工程实现，不放入 `tx` runtime。

## Phase 5: Runtime Storage Metadata

目标：把 type-level 注释 tag、field-level struct tag 和 AST type 信息组合成运行时可用的 storage metadata。

建议实现：

- `storage.TypeConfig`
  - `StorageMode`
  - `Redis *RedisConfig`
  - `Mongo *MongoConfig`
  - `Fusion *FusionConfig`
  - `Fields map[string]FieldConfig`
- `storage.Resolve(specs, packageSet) (*storage.Registry, error)`
- `storage.Registry` freeze 后用于 runtime。

关键行为：

- `mode=fusion` 要求 redis/mongo 均存在。
- Redis key builder 默认：`namespace:key:{shardBy}:id` 当 `id != shardBy`。
- Redis key builder 当 `id == shardBy`：`namespace:key:{id}`。
- Mongo filter 默认：`{idField: id}`；若 sharding enabled 且 `id != shardBy`，补 `shardBy`。
- 生成 route resolver，不依赖反射临时推导。

验收：

- 给定 type config，能生成 Redis key。
- 给定 type config，能生成 Mongo collection/filter/index metadata。
- 所有 runtime metadata 初始化后不可变。

测试：

- `tests/storage_key_test.go`
- `tests/storage_mongo_filter_test.go`
- `tests/storage_resolve_test.go`

## Phase 6: Delay Task Backend Abstraction

目标：Fusion 不绑定 Redis ZSet + Lua，抽象可替换任务后端。

建议接口：

```go
type DelayTask struct {
    Type      string
    ID        string
    ShardKey  string
    KeySuffix string
    Action    string
    DueAt     time.Time
}

type DelayTaskBackend interface {
    Enqueue(ctx context.Context, task DelayTask) error
    ClaimDue(ctx context.Context, now time.Time, limit int) ([]DelayTaskLease, error)
    Complete(ctx context.Context, lease DelayTaskLease) error
    Retry(ctx context.Context, lease DelayTaskLease, next time.Time, reason error) error
}
```

第一版实现：

- `storage/task/memory`: 内存 backend，只用于测试和本地开发。
- `lifecycle`: 提供可取消、可等待后台 worker 完成的运行时上下文，供 Fusion 延迟回写和其他后台组件在服务关停时等待活跃任务结束。
- `storage/task/redis`: Redis 持久化 delayed task backend，使用 sorted-set/hash 记录任务内容、到期时间、busy lease、完成与重试状态，并通过 Lua 保证 claim/complete/retry 的原子性。
- Redis delayed task keys 必须按任务 id 分桶，每个 bucket 使用独立 hash tag，例如 `tx:task:{0}:due`、`tx:task:{0}:payload`，保证单次 Lua 脚本访问的 due/payload/busy/token/attempts key 落在同一个 Redis Cluster slot，同时避免所有任务集中到一个 slot。
- Redis delayed task backend 面向多服务实例并发消费：claim/complete/retry 通过 lease token 隔离，但整体语义是 at-least-once，后续 Fusion Mongo 回写必须按存储幂等方式设计。
- `log`: 基于标准库 `log/slog` 提供最小 logger 注入入口，默认使用 `slog.Default()`；已提供 `Debug/Info/Warn/Error` 和 Context 版本的包级快捷方法，并提供 `NewLogger(name)` 轻量命名 logger 代理；Fusion Scheduler 失败重试时输出结构化错误日志，失败原因不进入 Redis 队列状态。
- `storage/fusion.TaskQueue` 负责把 Fusion 延迟保存 operation 转换为 `storage/task.DelayTask`，只保留 record id、route/shard key、action 和 due time；任务类型优先来自 `DelayedOperation.Type`，为空时使用 `RecordKey.TypeName`；Fusion 层不暴露额外 suffix，同一记录的延迟保存按 locator 合并为一条任务；入队时只接受保存 action，并要求类型和 record id 非空，避免无效任务进入 Redis 队列。延迟保存任务不携带 ChangeSet，回写时重新读取 Redis 当前热数据。
- `storage/fusion.StorageTaskHandler` 负责消费 delayed task：保存任务从 Redis 读取完整热数据并 replace 到 Mongo；Redis 热数据延迟删除任务会读取 `<mainKey>@v`、保存 Redis 当前热文档到 Mongo，然后仅在版本未变化时删除 Redis main/subKey/version key，版本变化时重新排队。
- Fusion 显式删除不进入延迟队列：运行时先删除 Redis 热数据，再通过显式 Mongo deleter 立即删除 Mongo 文档；如果业务绕过 Fusion 只删除 Mongo，未过期 Redis 热数据仍可能被延迟保存任务重新写回 Mongo。
- Fusion 延迟保存已通过真实 Redis/Mongo 集成路径验证：Redis 同步写入热数据，Redis delayed task backend 入队，Scheduler claim 到期任务，StorageTaskHandler 读取 Redis 热数据并整文档 replace Mongo。
- Fusion 读取默认在 Redis miss 后从 Mongo 全量加载并回填 Redis 热数据，回填路径运行时要求 `delayDelTime` 大于 0；回填前会获取记录级 Redis 锁并二次读取 Redis，防止并发 miss 时旧 Mongo 快照覆盖新的 Redis 热数据；显式 `DontLoadToRedis` 选项才允许直接读 Mongo 且不回填；部分字段读取主要按 Redis main/subKey key 粒度减少热数据读取，只请求 subKey 字段时不强依赖主文档命中。
- Redis 热数据生命周期版本键 `<mainKey>@v` 已作为运行时原语实现并接入 Fusion 延迟删除 handler：Redis delete 可带 expected revision 比对，只在版本未变化时删除 main/subKey/version key。该版本只用于 Redis 热数据过期/删除窗口的并发保护，不是业务字段，也不是 Mongo 文档字段。
- Mongo runtime 已提供最小 route/cluster adapter，按 routeType 和 RecordKey.ShardKey 路由单条读写，并在批量读取时按 Mongo shard 分组后还原输入顺序；`storage/mongo/mongodriver` 提供官方 MongoDB driver 的薄适配层。
- Redis runtime 已提供与 Mongo 对等的最小 route/cluster adapter：先按 routeType 和 RecordKey.ShardKey 路由到具体 Redis endpoint/adapter，再在每个 endpoint 内执行批量读取并还原输入顺序；`storage/redis/goredis` 提供 go-redis 的薄适配层。
- Redis 写入和整文档替换 API 直接使用 `storage.ApplyOptions`，不再保留 Redis 包内的 `ApplyOptions` 类型别名；`redis.LoadOptions` 仅表达 Redis 批量读取专用选项。
- `storage.RouteTable` 已提供 Redis/Mongo 共用的运行时路由表：支持 `min/max` 区间命中、多 shard 按 `routeBy % len(shards)` 选 endpoint、单 shard 字符串主键直连，并同时实现 Redis/Mongo cluster router 接口；后续 yml 配置解析应构造该路由表，而不是在 Redis/Mongo adapter 内重复实现路由规则。
- `storage/config.RouteTableConfig` 已提供项目 yml 路由配置的强类型承接模型：支持标量 endpoint、endpoint 数组和 `min/max/shards` 规则数组；项目负责读取本地 yml 和 include，tx 负责把解析后的配置对象归一化为 `storage.RouteTable`。
- Redis 批量读取默认贴合生产云存储代理能力：同一个 Redis endpoint 内允许直接批量读取多条不同 hash tag 的记录，由云代理负责跨 slot 分发和结果聚合，避免把生产热路径退化为多次 IO。
- Redis 批量读取应使用客户端层原生 `MGET` 展平 main/subKey key 后读取，再在 Go 侧按固定步长合并文档；不要把跨记录批量读取包进 Lua。云代理能识别 `MGET` 的所有参数都是 key，但不能可靠分析自定义 Lua 中哪些 `ARGV` 是 key，也不能安全拆分一个需要原子执行的脚本。
- 本地原生 Redis Cluster 或直连节点只作为研发 debug 兼容路径：已提供显式可选的 `LoadOptions.ClusterSafe` 模式，按 hash tag 分组后多次读取，保证本地集群可复现和排查跨槽问题，但不作为生产默认策略。
- StorageTaskHandler 在 Mongo replace 前会递归移除 Redis 文档中的 nil 删除标记，避免已删除字段以 Mongo `null` 形式重新落库。
- `delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写；`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据，不代表 Mongo 删除延迟。
- `change.Mode` 已作为生成代码的持久化模式边界：`ModeSnapshot` 用于原生字段修改后的快照 diff 持久化，`ModeStore` 用于生成 Store 的拦截式脏标记持久化；两者在同一变更周期内互斥。当前快照 diff 由 txgen 生成代码实现，tx 只提供共用模式枚举和错误。
- RedisJSON/ReJSON 可作为后续可选 Redis backend 单独评估：当前主线仍保持 Redis string JSON main/subKey 文档模型，后续只有在确认目标云 Redis、本地测试环境、Redis Cluster 行为和 JSON path 局部读写收益后再引入。
- 后续可增加 SQL backend、消息队列 backend；当前优先贴合生产参考流程，不引入 Kafka 等新的持久化队列依赖。
- 日志基建后续专题：在 txgen 脚手架主链路跑通后，提前讨论并实施项目级 logger 管理策略，包括业务项目统一定义 logger 枚举或包级变量、logger 名称规范、日志级别配置、handler 初始化、Provider/Factory 是否需要引入，以及是否需要自动推导包名或模块名以减少手写 logger 名称。

验收：

- Fusion 只依赖接口，不依赖具体 Redis task 实现。
- memory backend 支持按 due time claim。
- 同一任务可定义幂等 key。
- Redis backend 支持任务进程重启后的持久化保留与 leased 超时恢复。
- Redis backend 在 Redis Cluster 下不会因为 Lua 多 key 跨 slot 失败。
- Redis route/cluster adapter 能按 routeType 和 RecordKey.ShardKey 分组到 Redis endpoint；默认批量读取不在 endpoint 内继续拆 slot，显式 `LoadOptions.ClusterSafe` 才在 endpoint 内按 hash tag 拆分。
- 通用 RouteTable 能同时作为 Redis/Mongo router 使用，并贴合现有配置的 `min/max/shards` 规则。
- RouteTableConfig 能从 yml 混合形态归一化出 RouteTable，并暴露 endpoint 集合供业务工程创建和复用具体客户端。
- 有 subKey 的批量读取仍然使用客户端 `MGET`：每条记录展开为 main key 加需要的 subKey key，返回后按输入记录顺序和每条记录 key 数合并。
- Redis backend 按 bucket 分散队列 key，claim worker 会轮询 bucket 消费。
- 多个服务实例同时消费时，同一任务同一时刻只能被一个 lease 持有；lease 超时后允许重新领取。
- Fusion 后台 worker 使用 lifecycle 上下文，在优雅关停时等待已 claim 的 Mongo 回写任务完成。
- Fusion delayed save worker 可把 Redis 热数据回写到 Mongo，并且不会把删除标记落成 Mongo null 字段。
- Fusion delete 不产生 Mongo 延迟删除任务，显式删除调用会删除 Redis 热数据并立即删除 Mongo 文档。
- Fusion delayed task handler 只接受保存 action，delete 或其他未知 action 都不会被静默吞掉。

测试：

- `tests/task_backend_memory_test.go`
- `tests/task_backend_redis_test.go`
- `tests/lifecycle_context_test.go`
- `tests/fusion_scheduler_test.go`

## Phase 7: Code Generation Integration

目标：代码生成职责迁移到独立的 `txgen` 工程，`tx` 只保留运行时库、注解扫描、元数据归一化和存储基础能力。

不再在 `tx` 中保留 `gen.Model`、`gen.LoadTagsAndScan(...)`、`gen.RenderMetadata(...)` 这类中间快照导出 API。后续 Store 生成、Redis key helper、Mongo filter helper、字段编号 resolver 等生成逻辑都在 `txgen` 中实现和测试。

验收：

- `tx` 中没有 `gen` 包。
- 生成侧集成测试迁移到 `txgen/tests`。
- 生成产物仅依赖 `github.com/ngnl47/tx` 的运行时 API。

测试：

- `tests/gen_metadata_test.go`
- 使用 snapshot 字符串断言核心字段，不引入大体量 golden 文件。

## Phase 8: Storage Adapter Implementation

目标：在 metadata 稳定后再实现 Redis/Mongo/Fusion 真实读写。

建议顺序：

1. Redis key builder 和 interface，不引入外部 Redis client。
2. Mongo filter/index metadata，不引入外部 Mongo client。
3. 定义最小 DB adapter interface。
4. 引入可选 adapter 包实现真实 Redis/Mongo。
5. Fusion 使用 adapter + task backend 组合。

原因：

- 新库先把元数据和生成链路做稳。
- 避免一开始被具体 Redis/Mongo client 依赖和集成测试拖住。
- 可以让 tests 目录里的单元测试保持纯内存。

## Testing Policy

用户已确认：仅在 `tests/` 文件夹下生成测试用例。

执行规则：

- 所有 `*_test.go` 只放在 `/Users/a47/workspace/tx/tests`。
- 包目录如 `meta/`、`storage/` 不放测试文件。
- 测试 package 统一使用 `package tests`。
- 测试通过 public API 驱动，避免为了测试暴露内部实现。
- 临时输入文件用 `t.TempDir()` 生成。
- 不依赖外部 Redis/MongoDB；真实 adapter 的集成测试另行确认后再做。

## Milestones

### M1: Tag Annotation And AST Foundation

交付：

- `meta`
- `anno`
- `astscan`
- tests only in `tests/`

完成标准：

- 能从 type-level 注释 tag、field-level struct tag 和 Go AST 得到稳定 type metadata。

### M2: Legacy Migration Removed

交付：

- 不再交付 `migrate` 包。
- 旧注解迁移能力不属于当前 `tx` runtime 边界。

完成标准：

- 当前输入和测试均使用新 Tx 注解语义。

### M3: Runtime Metadata

交付：

- `storage` metadata registry
- Redis key builder
- Mongo filter/index metadata

完成标准：

- 不连接外部 DB 即可验证 runtime metadata。

### M4: Fusion Task Abstraction

交付：

- `storage/task`
- memory backend
- Fusion scheduler skeleton

完成标准：

- Fusion 延迟任务不依赖 Redis ZSet + Lua。

### M5: Generator Bridge

交付：

- `gen` metadata output
- 可供后续 Store generator 消费的中间模型。

完成标准：

- 输入带 type-level 注释 tag 和 field-level struct tag 的 Go package，输出可测试、稳定的生成模型。

## Open Design Details

这些不是阻塞项，可以在 M1 期间定稿：

- 是否支持除 `// @name:"..."` 之外的块状 doc 风格，还是后续补。
- type ref 表达方式：建议 `import/path.TypeName`，避免 package name 冲突。
- field alias 是否允许 type-level 注释 tag 或 field-level struct tag 显式覆盖 AST 推导 alias。
