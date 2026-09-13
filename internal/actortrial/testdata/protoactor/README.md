# 旧 ProtoActor 后端对照夹具

本目录保存 tx `29c5f63` 的 Actor 后端与关键测试，仅供历史回归和性能对照。生产包禁止导入；公开 `modules/actor` 已采用 BehaviorFactory 与统一实体宿主。源码与旧测试保留在一起，迁移不是删除可靠性证据，也不是双后端配置。

在 tx 根目录显式运行：

```sh
go test ./internal/actortrial/testdata/protoactor ./internal/actortrial/testdata/agent
```

需要真实 etcd 的旧测试按原环境变量显式启用。旧 Provider 已知 race 不代表新后端 race 结果。`go test ./...` 不递归 testdata 的测试，但比较基准和 `tests` 的旧后端测试仍可导入此夹具。完整上行比较通过相邻 `uplink/build.py` 的临时 overlay 构建；只运行 proto/hybrid，禁止重启已排除的全 NATS 上行组。冻结 I2 基线和原始归档保持不变。
