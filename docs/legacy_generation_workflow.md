# legacy framework 脚手架生成流程分析

本文记录 `evo-server/data/run.sh` 触发的 legacy framework/datagen 脚手架生成流程。后续重写 legacy framework 相关能力时，以这里拆出的阶段和产物为功能清单。

## 入口

`/Users/a47/workspace/evo/evo-server/data/run.sh` 只执行一条命令：

```sh
go generate
```

实际生成入口在 `/Users/a47/workspace/evo/evo-server/data/gen.go`：

```go
//go:generate go run ./gentag/main.go
//go:generate go run ./main/server/main.go
//go:generate go run ./main/client/main.go
```

因此完整工作流分三段执行：

1. `gentag/main.go`：扫描模板源码中的注解，生成 `tags.go` 元数据注册文件。
2. `main/server/main.go`：基于已注册元数据生成服务端 Go 代码、管理后台 TS 代码、GM 权限枚举、服务端 tags。
3. `main/client/main.go`：基于同一套元数据生成客户端和机器人使用的 TS 代码。

## 输入目录

主要输入是 `evo-data` 模块下的脚手架模板源码：

- `data/data/*.go`：业务数据结构、枚举、存储注解。
- `data/cmd/*.go`：客户端/服务端协议请求响应结构。
- `data/gm/cmd/*.go`：GM 服务定义，方法注解会转成请求/响应类型。
- `data/gm/data/*.go`：GM 专用数据结构。
- `data/room/data/*.go`、`data/room/cmd/*.go`：房间模块数据和协议。
- `data/custom/*.go`：本地自定义 tag 定义和自定义 TS 生成器。
- `data/gm/customtag/tag_def.go`：GM 权限 tag 定义。
- legacy framework/datagen 内置 tag：通过 `legacy/v3/tags.Register()` 和 `datagen/v3/tags.Register()` 注册。
- legacy framework datadef 外部模型：`leaderboard`、`oim`、`omail`、`opay` 等通过 `common.Import()` 导入。

模板文件中常见注解能力包括：

- 类型分组：`genData`、`genEnum`、`genReq`、`genResp`、`genService`。
- 可见性与导出：`ignoreClient`、`customExport`。
- 文档：`comment`。
- 协议：`linkResp`、`server`、`useHttp`、`httpPrefix`、`hint`、`useCmdHash`。
- 存储：`storage`、`redis`、`mongodb`，字段级还有 `redis:"subKey:..."`、`mongodb:"index,unique"`。
- GM 权限：`gmPerm`。

## 阶段一：生成 data 模块 tags

入口：`/Users/a47/workspace/evo/evo-server/data/gentag/main.go`

核心调用：

```go
gentag.GenTagsEx("evo-data", []string{"./room"}, "./room/tags.go")
gentag.GenTagsEx("evo-data", []string{"./gm"}, "./gm/tags.go")
gentag.GenTagsEx("evo-data", []string{"./data", "./cmd"}, "./tags/tags.go")
```

`gentag` 的工作方式：

- 遍历指定目录下的 `.go` 文件。
- 识别 `// @typeTag:"name"`、`// @fieldTag:"name"`、`// @methodTag:"name"`，生成 `reflectex.Register*TagParser(...)`。
- 识别普通 `// @xxx:"..."` 注解，并绑定到后续最近的 struct、method 或 field。
- 生成 `reflectex.TypeInfoOfObj(...)` 和 `AddTag(...)` 调用。

本阶段输出：

- `/Users/a47/workspace/evo/evo-server/data/room/tags.go`
- `/Users/a47/workspace/evo/evo-server/data/gm/tags.go`
- `/Users/a47/workspace/evo/evo-server/data/tags/tags.go`

这些文件不直接定义业务类型，而是把源码注释转换为运行时可查询的 `reflectex` 元数据注册逻辑。后续生成器依赖这些 `Register()` 函数恢复注解信息。

## 阶段二：服务端生成

入口：`/Users/a47/workspace/evo/evo-server/data/main/server/main.go`

执行顺序：

1. `common.Import()`
   - 注册 legacy framework 内置 tags。
   - 注册 datagen 内置 tags。
   - 执行 `data/tags.Register()`。
   - 导入 `leaderboard`、`oim`、`omail`、`opay` 外部 datadef。
   - 执行 `room.Register()`。
2. `gm.Register()`
   - 注册 GM 模块 tag。
   - 把 `gm/cmd` 中的服务、方法注解注册到 `reflectex`。
3. 创建 `v31.GenGo`。
4. 设置 `OnPostFillTypes`，补全 GM 权限 tag：
   - 如果 `gmPerm.id` 为空，根据类型名去掉 `GM` 前缀、按 `_` 拆分并转成小写路径。
   - 如果 `gmPerm.title` 为空，从 `comment` tag 取标题。
   - 把补全结果写回类型 tag：`gmPerm:"id:...,title:..."`。
5. 调用 `genGo.Gen(...)` 生成服务端 Go 代码。
6. 调用 `v31.GenTs` 生成管理后台 TS。
7. 调用 `GenTsPerms` 生成管理后台 GM 权限枚举。
8. 调用 `gentag.GenTags("../server/", "../server/tags/tags.go")`，为生成后的服务端 Go 代码再生成一份 tags。

服务端 Go 生成配置：

```go
v31.GenGoConfig{
    PathRoot:         "../server",
    Module:           "evo-server",
    GenNameOfVersion: "v3",
    GenStore:         true,
    ExportTags:       []string{"gmPerm"},
}
```

本阶段可观察到的输出：

- `/Users/a47/workspace/evo/evo-server/server/data/*.go`
- `/Users/a47/workspace/evo/evo-server/server/cmd/*.go`
- `/Users/a47/workspace/evo/evo-server/server/data/*Store.go`
- `/Users/a47/workspace/evo/evo-server/server/cmd/*Store.go`
- `/Users/a47/workspace/evo/evo-server/server/tags/tags.go`
- `/Users/a47/workspace/evo/evo-manager/src/gen/index.ts`
- `/Users/a47/workspace/evo/evo-manager/src/gen/perms.ts`

当前本地统计：

- `server/data` 下约 638 个 Go 文件。
- `server/cmd` 下约 1396 个 Go 文件。
- `server` 下约 806 个 `*Store.go` 文件。

### 服务端 Go 类型追加能力

生成后的 `server/data/*.go` 和 `server/cmd/*.go` 在原始模板基础上追加了：

- 包级 `T<TypeName> = TypeInfoOfObj((*TypeName)(nil))`。
- 类型 doc 中保留 storage、redis、mongodb 等注解。
- 嵌入基础类型：
  - 数据结构嵌入 `GDBase`。
  - 请求结构嵌入 `CmdBase`。
  - 响应结构嵌入 `RespBase`。
- 字段追加序列化 tag：
  - `json:"field,omitempty"`
  - `bson:"field,omitempty"`
  - `msgpack:"field,omitempty"`
- 字段保留 `comment`、`redis`、`mongodb`、`ignoreClient` 等业务 tag。
- 字段尾部追加中文注释。
- 数据结构生成 `New<TypeName>()` 构造函数。
- 数据结构生成字段级 touch 方法：
  - `Touch<Field>()`
  - `UnTouch<Field>()`
  - `Touch<Field>Recursive()`
  - `UnTouch<Field>Recursive()`

### Store 文件追加能力

开启 `GenStore: true` 后，生成器会为数据结构、响应结构和复合容器生成 Store/Provider 辅助代码。典型能力包括：

- `Value` 保存原始值。
- `Dirty` 数组记录字段是否变更。
- `Store` 数组保存子 Store。
- `AllDirty`、`StoreSet`、`DirtySet`、`Replaced` 等状态。
- 字段索引映射：
  - `FieldIndex(name string) int`
  - `FieldName(idx int) string`
- 字段 getter/setter：
  - `Get<Field>()`
  - `Set<Field>(value)`
  - 数值字段生成 `Inc<Field>()`、`Dec<Field>()`。
- touch 控制：
  - `Touch<Field>()`
  - `UnTouch<Field>()`
  - `AllTouch()`
  - `AllUnTouch()`
  - `AllUnTouchKeepSubStore()`
- 变更判断和增量序列化：
  - `IsChanged(filter)`
  - `GenChanged(options)`
  - `GetBonBody()`（响应 Provider 中可见）
- 指针 Store：
  - `Ptr<TypeName>Store`
  - 对 nil、替换、子字段变更做包装。
- 响应 Provider：
  - `GetCmd()`
  - 字段 setter。
  - 生成给客户端的 BON body，结合 `GetChangedForClientFilter` 过滤字段。

这些能力是 legacy framework 当前“增量同步”和“字段脏标记”的核心。

### 服务端 tags 追加能力

`server/tags/tags.go` 再次扫描生成后的 `server` 目录，把生成后的类型、方法和导出 tag 注册到服务端模块。它会：

- 注册服务端自定义 tag parser，例如 `gmPerm`。
- 给 `server/cmd` 中的请求/响应类型补回 `httpPrefix`、`ignoreClient`、`gmPerm` 等 tag。
- 把 `gmPerm` 的补全结果固化到服务端 tags 中。

## 阶段三：管理后台 TS 生成

入口仍在 `main/server/main.go`。

配置：

```go
v31.GenTsConfig{
    PathRoot:      "../../evo-manager/src/gen",
    TimeToNumber:  true,
    Filename:      "index.ts",
    GenHelper:     true,
    IncludeIgnore: true,
}
```

输出：

- `/Users/a47/workspace/evo/evo-manager/src/gen/index.ts`

追加能力：

- 生成 TypeScript enum。
- 生成 TypeScript 数据类、请求类、响应类。
- 根据 `comment` 输出 JSDoc。
- 时间类型按数字输出。
- 生成 helper，用于按服务/动作发送请求。
- `IncludeIgnore: true` 表示管理后台可以包含被 `ignoreClient` 标记的字段或类型。

## 阶段四：GM 权限 TS 生成

入口：`/Users/a47/workspace/evo/evo-server/data/gm/gen/gen.go`

输入：`genGo.SortedAllTypes` 中分组为请求类型且带 `gmPerm` tag 的类型。

模板：`/Users/a47/workspace/evo/evo-server/data/gm/gen/perm-enum.gohtml`

输出：

- `/Users/a47/workspace/evo/evo-manager/src/gen/perms.ts`

输出形式：

```ts
export const enum Perms {
    /**
     * 权限标题
     */
    GM_Xxx = "/permission/path",
}
```

追加能力：

- 把 GM 请求类型映射为前端可直接引用的权限枚举。
- 枚举值使用 `gmPerm.id`。
- JSDoc 使用 `gmPerm.title`。

## 阶段五：客户端和机器人 TS 生成

入口：`/Users/a47/workspace/evo/evo-server/data/main/client/main.go`

配置一：客户端

```go
v31.GenTsConfig{
    PathRoot:        "../../evo-client/evo-client/assets/game/scripts/orange/generated",
    TimeToNumber:    true,
    Filename:        "data-index.ts",
    GenHelper:       true,
    GenIsolate:      true,
    CallDecorators:  false,
    GenCmdConstEnum: true,
}
```

配置二：机器人

```go
v31.GenTsConfig{
    PathRoot:        "../../evo-robot/src/generated",
    TimeToNumber:    true,
    Filename:        "data-index.ts",
    GenHelper:       true,
    GenIsolate:      true,
    CallDecorators:  false,
    GenCmdConstEnum: true,
}
```

目标输出：

- `evo-client/evo-client/assets/game/scripts/orange/generated/data-index.ts`
- `evo-robot/src/generated/data-index.ts`

当前本地 checkout 中没有观察到这两个目标目录，但脚本配置会向这些路径写入。

追加能力：

- 生成客户端/机器人可用的数据类、请求类、响应类、枚举。
- 时间类型输出为数字。
- 生成协议 helper。
- `GenIsolate` 生成隔离命名/隔离模块形式，避免和业务代码混在一起。
- `GenCmdConstEnum` 生成命令常量枚举。
- `CallDecorators: false` 表示不为 Cocos3 风格调用生成 decorator。

## 生成器内部的类型分组

从 `datagen/v3/tools/v31` 和本地 `custom/gen_base.go` 可以归纳出核心分组：

- `genEnum` -> enum。
- `genData` -> 数据结构。
- `genReq` -> 请求结构。
- `genResp` -> 响应结构。
- `genService` -> 服务类型。

服务类型会展开为请求/响应：

- 每个服务 method 生成一个请求类型，命名形如 `Service_Method`。
- 如果 method 有返回值，则使用返回值作为响应类型。
- 如果返回值是匿名结构，则 clone 后命名为 `Service_MethodR`。
- 请求类型追加 `server` tag。
- 请求类型根据 method tag 复制 `hint`、`useCmdHash`、`useHttp`、`comment`、`macro` 等。
- 请求类型追加 `linkResp` tag 指向响应类型。

## 存储相关校验和生成

生成 Go 代码时会检查 storage tag：

- Redis tag 的 `idField` 必须能匹配字段 alias。
- Redis tag 的 `shardBy/routeBy` 必须能匹配字段 alias。
- MongoDB tag 的 `idField` 必须能匹配字段 alias。
- MongoDB tag 的 `shardBy/routeBy` 必须能匹配字段 alias。
- `GenGo` 默认还会检查 Redis key / Mongo collection 重复定义，除非 `IgnoreDBKeyDupCheck` 开启。

生成结果中，存储语义主要体现在：

- 生成后的 Go 类型保留 storage/redis/mongodb 注解。
- `*Store.go` 为数据对象生成字段级 dirty tracking 与增量输出。
- Redis/Mongo 真实读写辅助由 legacy framework storage runtime 和生成 Store 共同承担。

## 后续重写功能清单

为了逐步替换 legacy framework 库，可以按以下顺序重写：

1. 注解定义与解析
   - type-level struct tag 注解。
   - field-level Go struct tag 注解。
   - method-level 注解。
   - 自定义注解定义注册。

2. AST 元数据收集
   - 扫描 package/type/field/method。
   - 建立 type、field、method 与注解的绑定。
   - 字段 alias 规则。

3. tags.go 生成替代
   - 旧 legacy framework 通过 `reflectex.TypeInfoOfObj` 和 `AddTag` 恢复运行时元数据。
   - 新实现可以直接输出静态 metadata 或 registry 初始化代码。

4. 类型分组
   - enum/data/req/resp/service。
   - service method 展开 req/resp。
   - linkResp/server/http/comment/useCmdHash 等协议元数据。

5. 存储元数据
   - storage/redis/mongodb type tag。
   - redis subKey 字段 tag。
   - mongo index 字段 tag。
   - idField/routeBy 字段校验。
   - storage mode、延迟保存/删除、fusion 语义。

6. Go 类型生成
   - data/cmd 包输出。
   - json/bson/msgpack tag 生成。
   - 基础类型嵌入。
   - TypeInfo 或新 registry 变量。
   - 构造函数。
   - Touch/UnTouch 辅助。

7. Store/dirty tracking 生成
   - Store wrapper。
   - PtrStore。
   - map/slice/any store。
   - FieldIndex/FieldName。
   - Set/Get/Inc/Dec。
   - AllTouch/AllUnTouch。
   - IsChanged/GenChanged/GetBonBody。
   - filter 支持。

8. TypeScript 生成
   - enum/data/req/resp。
   - 注释输出。
   - 请求 helper。
   - 管理后台 IncludeIgnore。
   - 客户端隔离输出。
   - 命令常量枚举。

9. GM 权限生成
   - gmPerm 注解解析。
   - id/title 默认补全。
   - `perms.ts` 输出。

10. 生成产物二次 tags
    - 若仍需要运行时 tag registry，需要支持扫描生成后的 server 代码再输出 tags。
    - 若新实现采用静态 metadata，则可以跳过二次 tags，改为一次性生成完整 registry。

