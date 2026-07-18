# tx 使用手册

本文记录 `github.com/ngnl47/tx` 当前已实现并验证的运行时能力。手册会随实现持续维护；仍未决策的行为会明确标注。

## 定位

`tx` 是运行时基础库，负责：

- 解析和保存类型元数据。
- 追踪字段级变更。
- 将 `ChangeSet` 落地到 Redis、Mongo 或 Fusion 策略。
- 提供 Redis 延迟任务 backend 和 Fusion 后台回写所需组件。

生成代码依赖 `tx`，`tx` 不依赖生成器工程。

## 元数据与字段别名

字段别名用于生成 JSON/BSON/Msgpack tag，也用于 `ChangeSet` 路径、Redis 文档字段名和 Mongo 字段名。

当前 AST 扫描别名优先级：

```text
alias tag > json tag > lower-camel Go 字段名
```

例如：

```go
Key int64 `json:"k"`
```

会把字段 alias 解析为 `k`。序列化后使用 `{"k":...}`，反序列化时 Go 标准库会根据 `json` tag 回填到 `Key` 字段。

## ChangeSet

`change.Tracker` 记录字段级变更，字段可以通过编号解析为 alias：

- `TouchField` 记录普通 set。
- `TouchFieldPath` 记录子路径 set。
- `ReplaceFieldPath` 记录整体或子路径 replace。
- `UnsetFieldPath` 记录 unset。
- `FieldChange.FullPath(resolver)` 将字段编号和子路径拼成完整 alias path。

同一保存周期内，顶层字段变更会覆盖同字段下已有或后续追加的子路径变更。例如先记录 `items.1001.quantity`，再记录 `items` 整字段 dirty，最终 `ChangeSet` 只保留 `items`。这用于避免大集合字段在整字段保存时继续保留大量 key 级 path 记录。

生成代码应传入生成的 `FieldResolver`，避免运行时反射字段名。

## Redis 存储模型

Redis 使用 string JSON 文档模型。

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

- `ID` 来自 `idField`。
- `ShardKey` 来自 `routeBy`，仅在 `routeBy != idField` 时需要。
- Redis Cluster 批量读取必须保证每条记录携带正确路由字段，避免跨槽问题。

## Mongo 存储模型

Mongo adapter 当前支持：

- `ApplyChangesWithOptions`：将 `ChangeSet` 转成 Mongo update 文档。
- `LoadDocument`：按 ID 读取单条文档，可带 projection 字段。
- `LoadDocuments`：按 ID 批量读取并保持返回顺序。
- `ReplaceDocument`：整文档替换，供 Fusion 延迟保存回写使用。
- `ApplyDelete`：显式物理删除接口，由业务代码决定是否调用。

Mongo TTL index 的字段级 `expireAfter` 会删除整条文档，不是删除单个字段。生成阶段应限制同一类型最多一个 TTL 字段。

## Fusion 流程

Fusion 策略的当前保存路径：

1. 业务变更生成 `ChangeSet`。
2. Redis 同步写入热数据。
3. Redis delayed task backend 写入延迟保存任务。
4. Scheduler 到期 claim 任务。
5. StorageTaskHandler 根据任务定位重新读取 Redis 热文档。
6. Handler 清理 nil 删除标记后 replace Mongo 文档。
7. 任务成功后通过 Lua complete 删除该任务在 Redis 队列中的状态。

延迟任务队列使用 bucket hash tag，支持多个服务实例并发消费。语义是 at-least-once，Mongo 回写必须保持幂等。

Fusion 显式删除不会进入延迟队列。删除路径应先删除 Redis 热数据，再立即删除 Mongo 文档；如果只绕过 Fusion 删除 Mongo，而 Redis 热数据和延迟保存任务仍存在，后续保存任务可能按 Redis 当前数据重新写回 Mongo。

## TTL 与删除

`delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写。Fusion 延迟保存队列使用这个时间。

`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据；写入后会续期。显式传入 `ApplyOptions.TTL` 时，以显式值为准。

`delayDelTime` 不代表 Mongo 删除延迟，也不会触发 Mongo 删除。业务是否执行 Mongo 物理删除，仍由显式 delete 接口和业务代码决定。

## 测试

```bash
env GOCACHE=/private/tmp/tx-go-build GOTMPDIR=/private/tmp /Users/a47/sdk/go1.25.6/bin/go test ./... -count=1
```
