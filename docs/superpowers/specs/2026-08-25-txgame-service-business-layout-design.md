# txgame 服务与业务包结构设计

## 目标

`txgame/server` 按可独立部署的 L4 Service 纵向组织项目代码，使服务专属 SPI、路由、Handler、Actor 和业务模块在目录上具有明确所有者，同时保持 `tx/server` 的运行时能力黑盒化。

## 依赖方向

项目依赖固定为：

```text
cmd/server
  -> internal/app
      -> internal/config
      -> internal/db
      -> services/*
          -> services/<name>/handler
              -> services/<name>/internal/procedure
                  -> services/<name>/modules
                      -> modules/internal/<domain>
          -> tx public API
```

`server/internal` 是进程范围的 Go 可见性边界，不是业务代码汇总包。它只保存进程组装、配置转换和基础设施 facade。服务专属 SPI、Handler、路由和 Actor 注册归属 `server/services/<name>`；多个 L4 Service 共同使用但不属于 tx 的项目机制放入 `server/services/internal`。

## 目录结构

```text
server/
├── cmd/
│   ├── server/
│   └── txctl/
├── config/
│   ├── debug/
│   ├── test/
│   └── release/
├── internal/
│   ├── app/
│   ├── config/
│   └── db/
└── services/
    ├── internal/
    │   ├── actorcmd/
    │   └── commandroute/
    ├── agent/
    ├── msgq/
    ├── game/
    │   ├── handler/
    │   ├── internal/
    │   │   └── procedure/
    │   └── modules/
    │       ├── role_service.go
    │       ├── item_service.go
    │       └── internal/
    │           ├── role/
    │           └── item/
    ├── room/
    └── battlefield/
```

只创建当前有实际实现或示例价值的目录，不用空包预占 `msgq`、`room` 或 `battlefield`。

## 业务编排

`modules/*_service.go` 是 Handler 和 Procedure 共用的稳定业务能力层。它使用包级函数暴露强类型业务 API，
不创建无状态业务能力实例，也不暴露 writer、Store、`txn.Runtime` 或完整 `server.Context`。
这些函数不判断当前是否处于事务，不负责选择根调用或子调用，也不负责最终刷盘。

`modules/internal/<domain>` 保存单领域算法和数据操作。兄弟领域实现禁止互相导入，跨领域的稳定业务能力由
`modules/*_service.go` 组合。

`services/game/internal/procedure/P*.go` 保存需要本机锁、回滚、最终刷盘或事务内复用的业务流程。
Procedure 单向依赖并组合 `modules` 的稳定业务能力，`modules` 不反向依赖 Procedure。该位置允许 game Handler
调用 Procedure，同时通过 Go `internal` 规则禁止其他 L4 Service 越界使用 game 的事务流程。

有状态 Handler 知道自己位于事务外，使用 `txn.Executor.Call` 启动根 Procedure。父 Procedure 需要复用另一个完整事务
流程时，使用当前 `*txn.Tx.Call` 创建子 savepoint；根 Procedure 必须提前声明完整锁集。无状态 Handler 直接调用
`modules` 业务能力，并在成功后显式保存根 GD。运行模型由 txgame 组合根在服务启动前确定，不能在单次请求中动态切换。

该设计借鉴 rj3d 中“Procedure 组合稳定业务能力”的依赖方向，但保留 Go 侧显式区分根调用和子调用：tx 不依赖
线程局部状态猜测当前事务，错误的嵌套根调用会直接返回错误。

跨 L4 Service 的调用必须经过目标服务的 Actor/RPC client；每个目标服务在本地执行自己的 Procedure。即使多个 Service 聚合在同一进程，也不能借此扩大单机事务边界。

## 启动与配置

`server/cmd/server` 是唯一游戏进程入口。它通过 `--env=debug|test|release` 选择 `<config-root>/<env>/app.yml`，
缺省环境为 `debug`、缺省配置根目录为当前工作目录下的 `config`；独立发布的二进制可以使用 `--config-root` 或
`TXGAME_CONFIG_ROOT` 指向绝对目录。进程再通过 `--services` 选择 L4 Service，YAML 不保存服务选择。

`gen/run.sh` 是脚手架稳定入口，内部命令目录命名为 `gen/main/txgen`。项目运维脚本放在 `server/cmd/txctl`，只包装 tx 已提供的控制命令，不复制实现。仓库根下不保留第二个 `server/main.go`。

## 质量约束

- Handler 可以依赖 `modules` 和所属 Service 的 `internal/procedure`，不能导入 `modules/internal`。
- `services/game/modules` 可以导入自己的 `modules/internal`；其他 Service 不能导入。
- `services/game/internal/procedure` 可以导入 `services/game/modules`；`modules` 禁止反向导入 Procedure。
- `services/*` 不导入 `internal/app`，避免组合根反向依赖。
- 服务专属配置在对应 Service 被选择后才转换和校验。
- 所有移动必须保持现有 Agent、Actor、命令路由和运行时行为不变。
- 使用包边界测试、完整 `go test ./...`、`go vet ./...` 和真实 Redis/Mongo/etcd 集成回归验证迁移。
