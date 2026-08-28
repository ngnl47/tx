# Command Route Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 txgen 从项目级协议路由配置和 `@cmdRoute` 注解生成稳定 CommandID、目标服务与 Actor 路由元数据，使 Agent 能从 Frame 头直接投递上行请求而不解析业务 BON body。

**Architecture:** `txgame/gen` 保存服务号段和稳定 ID 清单，txgen 只读取它们，不硬编码服务名或号段。请求未声明 `@cmdRoute` 时生成到默认 `game` 路由；声明时必须引用请求顶层的 msgpack 字段作为 `actorKey`。Frame 传递 `CommandID` 和强类型 route key，Agent 及业务适配层只消费生成 registry，业务实体的 owner/binding 仍由项目侧决定。

**Tech Stack:** Go 1.26、gopkg.in/yaml.v3、txgen AST/模板、BON、TCP/WebSocket Agent、TypeScript。

**Spec:** `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`

## Global Constraints

- `tx` 不依赖 `txgen`，也不内置项目服务名、号段或业务 Actor 归属。
- 可选服务规范名对齐参考工程：`game`、`room`、`war`、`agent`、`msgq`、`admin`、`login`、`delay`、`task` 等；项目可扩展新名称。
- `@cmdRoute` 使用 `toService`、`actor`、`actorKey` 三个参数；`placement` 是服务注册策略，不属于消息定义。
- 无 `@cmdRoute` 的请求默认发送给 `game`。
- `actorKey` 只能引用请求顶层的 msgpack 名字段，类型只能是 `int64`、`uint64` 或 `string`；TypeScript number 路由键不得超过 `9007199254740991`。
- CommandID 分配稳定且不可复用，服务号段由项目 YAML 配置；txgen 只校验该配置本身，不验证服务是否实际启动。
- Agent 不解码业务 BON body 决定路由；目标业务边界必须校验 Frame route key 与反序列化请求中的 actorKey 一致。

---

### Task 1: 项目级路由号段与稳定编号清单

**Files:**
- Create: `/Users/a47/workspace/txgen/gen/cmd_route.go`
- Modify: `/Users/a47/workspace/txgen/gen/config.go`
- Modify: `/Users/a47/workspace/txgen/go.mod`
- Test: `/Users/a47/workspace/txgen/tests/cmd_route_test.go`

**Interfaces:**
- Consumes: `Config.CmdRouteConfigPath`, `Config.CmdIDManifestPath`
- Produces: `loadCmdRouteConfig`, `allocateCommandIDs`, `CmdRouteConfig`, `CommandRoute`

- [ ] **Step 1: Write failing tests**

```go
func TestAllocateCommandIDsUsesConfiguredServiceRangeAndPreservesExistingID(t *testing.T) {
    config := writeRouteConfig(t, `services:\n  game: {start: 10000, end: 19999}\n  war: {start: 30000, end: 39999}\n`)
    manifest := writeCommandManifest(t, `commands:\n  Role_Rename: 10001\n`)
    ids, err := allocateCommandIDs(config, manifest, []routeCommand{
        {Name: "Role_Rename", ToService: "game"},
        {Name: "Battlefield_Attack", ToService: "war"},
    })
    if err != nil { t.Fatal(err) }
    if ids["Role_Rename"] != 10001 || ids["Battlefield_Attack"] != 30000 { t.Fatal(ids) }
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^TestAllocateCommandIDsUsesConfiguredServiceRangeAndPreservesExistingID$' -count=1
```

Expected: compilation failure because route allocation APIs do not exist.

- [ ] **Step 3: Implement YAML parsing and manifest allocation**

```go
type CmdRouteConfig struct {
    Services map[string]CommandIDRange `yaml:"services"`
}

type CommandIDRange struct { Start uint32 `yaml:"start"`; End uint32 `yaml:"end"` }
```

Reject overlapping or empty ranges, unconfigured services, duplicate IDs and exhausted ranges. Preserve absent-from-source manifest entries as tombstones so deleting a command never frees its ID.

- [ ] **Step 4: Run focused tests and generator suite**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run 'TestAllocateCommandIDs|TestGenerateCmd' -count=1
```

### Task 2: `@cmdRoute` AST validation and generated Go/TS registry

**Files:**
- Modify: `/Users/a47/workspace/txgen/gen/scan_cmd.go`
- Modify: `/Users/a47/workspace/txgen/gen/model.go`
- Modify: `/Users/a47/workspace/txgen/gen/render_cmd.go`
- Modify: `/Users/a47/workspace/txgen/gen/render_ts.go`
- Modify: `/Users/a47/workspace/txgen/gen/templates/cmd_registry.go.tmpl`
- Modify: `/Users/a47/workspace/txgen/gen/templates/ts_cmd.go.tmpl`
- Test: `/Users/a47/workspace/txgen/tests/generate_cmd_test.go`
- Test: `/Users/a47/workspace/txgen/tests/typescript_generation_test.go`

**Interfaces:**
- Consumes: parsed `@cmdRoute`, allocated CommandID
- Produces: generated `CmdDescriptor.CommandID`, `CmdDescriptor.Route`, TS `CommandSchemas` route metadata

- [ ] **Step 1: Write failing generator tests**

```go
// @genReq
// @cmdRoute:`toService:"war" actor:"battlefield" actorKey:"battlefieldId"`
type Battlefield_Attack struct {
    BattlefieldId int64 `msgpack:"battlefieldId"`
}
```

Assert generated Go descriptor contains the configured ID, `ToService: "war"`, `Actor: "battlefield"`, key kind `int64`, and TypeScript `CommandSchemas` exports the same metadata. Add invalid cases for a missing key field and `float64` key field.

- [ ] **Step 2: Run focused tests and verify they fail**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run 'TestGenerateCmd.*Route|TestGenerateTypeScript.*Route' -count=1
```

Expected: generated registry has no route metadata.

- [ ] **Step 3: Implement directive parsing and metadata generation**

`@cmdRoute` is valid only on `@genReq`. `actorKey` resolves against `cmdFieldModel.ProtocolName`, not Go field spelling. The generated registry must retain lookup by command name and add lookup by non-zero CommandID. No route annotation defaults to `toService:"game"` and no actor key.

- [ ] **Step 4: Run focused tests and full txgen suite**

Run:

```bash
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
```

### Task 3: Frame 路由头与 Agent Dispatch 元数据

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/agent/frame.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/session.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/types.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/tcp.go`
- Modify: `/Users/a47/workspace/tx/modules/agent/websocket.go`
- Modify: `/Users/a47/workspace/txclient/src/protocol/frame.ts`
- Test: `/Users/a47/workspace/tx/tests/agent_frame_test.go`
- Test: `/Users/a47/workspace/txclient/src/protocol/frame.test.ts`

**Interfaces:**
- Consumes: `CommandID`, `RouteKey` from Frame header
- Produces: `DispatchRequest.CommandID`, `DispatchRequest.RouteKey`

- [ ] **Step 1: Write failing cross-language frame tests**

```go
frame := agent.Frame{
    Kind: agent.FrameUpMsg, Seq: 1, CommandID: 30001,
    RouteKey: agent.Int64RouteKey(47001), Payload: []byte{1, 2},
}
decoded, err := agent.DecodeFrame(encoded, 1024)
if err != nil || decoded.CommandID != 30001 || decoded.RouteKey.Int64() != 47001 { t.Fatal(...) }
```

Cover `int64`/`uint64`/`string`, no-key default route, malformed key length, and v1 rejection after protocol version changes.

- [ ] **Step 2: Run focused tests and verify failure**

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^TestFrame.*Route' -count=1
```

- [ ] **Step 3: Implement versioned header and dispatch propagation**

Use a fixed routing header plus raw key bytes before the BON payload. `RouteKey` is a tagged value with exact `int64`, `uint64`, or UTF-8 string representation. Business Frame requires a non-zero CommandID; handshake/outbound/ack retain a zero route. Avoid parsing BON in tx Agent.

- [ ] **Step 4: Run tx and txclient tests**

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txclient
npm test
```

### Task 4: txgame 协议配置、默认 game 与 direct Actor adapter

**Files:**
- Create: `/Users/a47/workspace/txgame/gen/protocol/routes.yml`
- Create: `/Users/a47/workspace/txgame/gen/protocol/command_ids.yml`
- Modify: `/Users/a47/workspace/txgame/gen/main/server/main.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/agent/actor_dispatcher.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/agent/bon_dispatcher.go`
- Test: `/Users/a47/workspace/txgame/server/internal/agent/*_test.go`
- Test: `/Users/a47/workspace/txgame/server/cmd/server/main_test.go`

**Interfaces:**
- Consumes: generated `cmd.LookupCmdDescriptorByID`, Frame route metadata
- Produces: default-game dispatch and direct target route context

- [ ] **Step 1: Write failing project adapter tests**

Use one request without `@cmdRoute` and one `war/battlefield` request. Assert that the first resolves the configured default game dispatcher, the second exposes `toService`, `actor`, `CommandID`, `RouteKey`, and does not decode payload before selecting its target resolver. After decode, assert mismatch between header key and request `battlefieldId` returns an error.

- [ ] **Step 2: Run focused tests and verify failure**

```bash
cd /Users/a47/workspace/txgame/server
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./internal/agent -run 'Route|Actor' -count=1
```

- [ ] **Step 3: Implement project configuration and adapters**

Pass `routes.yml` and `command_ids.yml` into both Go and TS generation. The command manifest is refreshed deterministically and stays committed under `txgame/gen/protocol`. Project adapter maps only generated descriptors to project-selected resolvers; owner binding, logical slot lookup and service lifecycle remain project code.

- [ ] **Step 4: Regenerate and verify all modules**

```bash
cd /Users/a47/workspace/txgame/gen
GO_BIN=/Users/a47/sdk/go1.26.5/bin/go ./run.sh
cd /Users/a47/workspace/txgame/server
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txclient
npm test
```

### Task 5: 文档、生成物审计与全量验证

**Files:**
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/tx/docs/usage.md`
- Modify: `/Users/a47/workspace/txgen/docs/usage.md`
- Modify: `/Users/a47/workspace/txgame/README.md`

- [ ] **Step 1: Update documentation with runnable examples**

Document the YAML service ranges, immutable command manifest, `@cmdRoute` syntax, default `game` behavior, Frame key contract, direct-route authorization boundary, and `bound` versus `balanced` service policy ownership.

- [ ] **Step 2: Scan for outdated protocol descriptions**

Run:

```bash
rg -n '24 字节|BON body.*路由|全部.*game|CommandID.*0|cmdRoute' /Users/a47/workspace/tx /Users/a47/workspace/txgen /Users/a47/workspace/txgame
```

Update only conflicting current-path descriptions.

- [ ] **Step 3: Run full verification**

```bash
cd /Users/a47/workspace/tx
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txgen
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txgame/gen
GO_BIN=/Users/a47/sdk/go1.26.5/bin/go ./run.sh
cd /Users/a47/workspace/txgame/server
env GOCACHE=/private/tmp/tx-go126-build GOTMPDIR=/private/tmp GOTOOLCHAIN=local /Users/a47/sdk/go1.26.5/bin/go test ./... -count=1
cd /Users/a47/workspace/txclient
npm test
```

## Self-Review

- Stable IDs are allocated only from project YAML ranges and retained in a committed manifest.
- Frame routing never requires tx Agent to deserialize BON payload.
- A missing or mismatched actor key fails before business mutation.
- Service placement and entity ownership remain project-defined.
- Existing unannotated commands retain the default game route.
