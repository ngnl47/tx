# Tx storage metadata implementation plan

本文是 `github.com/ngnl47/tx` 中重写 legacy framework tag storage chain 的实施计划。

目标不是兼容式修补 legacy framework，而是把旧链路抽象成一个新的 Go module 能力：

- 使用 Go 注释作为元数据主入口，保持类型定义和存储定义靠近。
- 使用 Go AST 读取 Go 类型结构和注释，建立注释元数据与 Go 类型之间的绑定关系。
- 支持从 legacy framework 旧注解迁移到新的 Tx 语义；语法形态尽量沿用 `// @name:"k:v,flag"`，降低迁移成本。
- 不支持独立 schema 文件；存储元数据只来自 Go 注释、struct tag 和 AST 扫描结果。
- 运行时元数据只允许初始化期注册；不支持隐式运行时追加 tag。
- `fusion` 作为公开存储模式名；旧 `redisDelayMongoDB` 只作为迁移器识别的 legacy alias。
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
  migrate/          # legacy framework legacy tag 到 Tx metadata 的迁移器
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
- 新语义使用 `mode:fusion`；旧 `mode:redisDelayMongoDB` 只作为迁移/兼容 alias 识别。
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
- 迁移器直接输出 `meta.TypeSpec`，不再经过中间 schema 文档格式。
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

## Phase 4: legacy framework Legacy Migrator

目标：把 legacy framework 旧注解规范化为新的 Tx `meta.TypeSpec` 语义。

输入形式：

- Go 源码目录。
- 旧注解样式：
  - `// @storage:"mode:redisDelayMongoDB,delaySaveTime:600,delayDelTime:86400"`
  - `// @redis:"routeType:role,routeBy:roleId,idField:roleId,key:role,omitempty"`
  - `// @mongodb:"routeType:role,routeBy:roleId,idField:roleId,collection:roles,enableSharding"`
  - 字段 tag：`redis:"subKey:items"`、`mongodb:"index,unique"` 等。

实现策略：

- 用 Go AST 定位 struct 和字段。
- 从 `Doc` 注释组中读取 legacy `@storage` / `@redis` / `@mongodb`。
- 字段上的 struct tag 仍可用于迁移输入，且保持为 field-level 主入口。
- 将 `redisDelayMongoDB` 规范化为 `mode:fusion`。
- 保留 `routeType`/`routeBy`/`idField` 作为第一版 canonical 字段名，避免无意义迁移成本。
- 保留 `omitempty`、`enableSharding` 等 flag 形态。
- 直接输出 `meta.TypeSpec` 和 migration report。

验收：

- 对 `GDRole` 示例能生成语义等价的规范化 metadata。
- migration report 包含：
  - 成功迁移的类型数
  - 未识别参数
  - 字段缺失
  - legacy alias 使用情况

测试：

- `tests/migrate_legacy_test.go`
- 覆盖 storage/redis/mongodb 三类注解和字段 tag。

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
- 后续可增加 Redis backend、SQL backend、消息队列 backend。

验收：

- Fusion 只依赖接口，不依赖具体 Redis task 实现。
- memory backend 支持按 due time claim。
- 同一任务可定义幂等 key。

测试：

- `tests/task_backend_memory_test.go`
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

### M2: Legacy Migration

交付：

- `migrate`
- legacy framework legacy 注解规范化到新 Tx 语义。

完成标准：

- 能迁移 `GDRole` 风格注解。
- 输出 migration report。

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
- `mode:fusion` 等新值是否由迁移器自动改写，还是只在报告中提示。
- type ref 表达方式：建议 `import/path.TypeName`，避免 package name 冲突。
- field alias 是否允许 type-level 注释 tag 或 field-level struct tag 显式覆盖 AST 推导 alias。
- migration CLI 名称：建议后续放在 `cmd/tx`，但第一阶段可先做 library API。
