# Actor 共用路由与直连上行实施计划

> 使用 superpowers:executing-plans 串行实施，不使用子代理，不提交 Git。

目标：在 `internal/actortrial` 验证 agent 直连 gRPC 与 Actor NATS Call 使用同一份槽位发现、同一个实体和排水边界。用户已确认本轮实施；正式包及两个示例的整体迁移另行验收。

## 已确认的设计

- 发现、传输分离：复用 `discovery.SlotResolver`、Watch 缓存和已有首次分配/交接；消息不逐条读 etcd。未分配返回明确错误，不在高频入口自动分配或重投。
- `go-rendezvous` 是已有虚拟选址试验依赖，稳定 assignment 路径保持 SlotAllocator 的现行算法。成员变化不重算已激活实体归属，不新增哈希库或第二套控制面。
- 直连复用 `rpc.Host`、`rpc.Sender` 和既有 DeliveryService；正常载荷嵌套现行 Actor protobuf，框架错误以独立错误结果传递。连接池、凭据和关闭仍由 rpc 组件负责，不手写另一套 gRPC 客户端管理。
- 两种入口进入同一 Runtime；SlotHost 的加载前权威读取、容量及卸载不因传输变化而复制。请求携带目标启动实例 ID，旧地址被新进程复用时必须拒绝旧身份。
- Agent 复用现行 Delivery 转换和 Dispatcher SPI，等待精确 ProcessedUpSeq；上下文取消只结束等待。保留 BON/Frame/Seq/Ack、msgq、四种 NATS 方法语义。
- 上行直连减少 broker 转发的方向已确认；本批只证明可运行性和职责复用，不宣称跨机吞吐、云流量价格或完整生产验收。

## 实施与验收

- [x] 修改前相关包基线；新增 `routing_test.go`：双入口命中同一激活、ACK 等到业务完成、共享排水和旧地址身份拒绝。先运行观察新增能力缺失。
- [x] 新增 `routing.go`：`RoutedCaller.Call(ctx, Identity, Delivery)` 只读同一 SlotResolver，两个传输实现同一 Call 契约；高级 NATS API 仍由 Transport 保留。
- [x] 新增 `direct.go`：直接调用及接收适配，不新增生成协议；新增 `agent.go`：将项目命令解析与可信会话身份/ACK 校验衔接到共用路由。
- [x] 真实 NATS + etcd：交接前旧 owner 拒绝新请求、收尾失败不晋升、成功后两种路径指向新 owner。用现有 SlotDirectory Watch 维护共享缓存；失效后明确拒绝，不绕过缓存回源。
- [x] 覆盖取消后业务继续完成、框架错误分类及业务响应不能冒充 ACK；执行相关包、真实依赖 race、全量 tx 测试。新增基准仅在测试文件测已缓存路由开销，不把它称为端到端吞吐。
- [x] 更新试验计划、总计划及本轮报告，执行一次 ponytail-review。统计本轮与累计未提交变更，报告正式接线和部署成本模型的剩余工作。
