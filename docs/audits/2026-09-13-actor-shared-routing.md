# 共用实体路由与直连上行验收

本轮完成 `internal/actortrial` 内部节点：Agent 直连 gRPC 与 Actor NATS Call 使用同一个 SlotResolver 和实体 Runtime。正式 ProtoActor、框架装配及两个 game 示例尚未切换；JetStream 尚未实施。没有提交 Git。

## 修改原因与边界

用户确认高流量上行优先直连，以减少 broker 转发；NATS 用于 Actor 间 RPC 和后续可靠任务。成员发现和实体归属不能由两个传输入口各自实现。

现行事实：`placement.go` 的 go-rendezvous 只用于早期虚拟选址试验。稳定归属已有 SlotStore CAS、SlotAllocator、SlotHost 与正常交接，本轮没有替换其算法；新成员不会改变已有 assignment。首次无归属仍由低频分配方处理，普通路由返回未分配错误，不隐式分配或重投。

改动位置：

- `routing.go`：RoutedCaller 复用 SlotResolver，Caller 接口由现有 NATS Transport 和新增 DirectTransport 两个实现提供。组合根决定传输，业务调用只带 Identity 与 Delivery；高级 NATS 四种方法保留。
- `direct.go`：复用 `rpc.Host`、`rpc.Sender`、DeliveryService。目标启动实例 ID 保存在精确实例路由，实体类型和 key 通过 gRPC 二进制 metadata 传递；正常载荷嵌套已有 Actor protobuf，错误结果独立保存 RemoteError 分类。没有新建协议、连接池、哈希库或自动重试层。
- `agent.go`：命令解析仍通过项目 SPI，tx 注入可信会话身份、UpSeq 和 deadline，等待精确 ProcessedUpSeq。失败或业务响应不能当作 ACK。
- 两种接收路径均进入同一个 Runtime，加载前使用 SlotHost 的权威归属检查、容量预算与共享激活，排水计数不复制。共享 gRPC Host 的生命周期仍归组合根管理，SlotHost 不关闭它。

例如已经绑定到实例 a 的 `world/51`：Agent 的加速命令经 gRPC 进入 a；另一个 Actor 的查询经 NATS 进入同一个 a 的实体邮箱。加入实例 b 不改归属；a 收尾成功并 CAS 晋升后，槽位 Watch 使两种调用共同选择 b。

## 本轮验证

环境：Go 1.27.0，模块语言版本维持 go1.26；macOS amd64，i9-9880H。真实依赖使用现有 NATS 2.14.6、etcd 3.5.15、Redis 8.8 的本地映射端口，没有改容器配置。etcd 每个测试创建带 ownership marker 的随机 `/tx/tests/actortrial/` 前缀；成功精确清理，失败保留。

新增测试先因缺失直连和共用路由入口而无法编译；实现后，测试清理阶段复现 `context canceled`，原因是复用了在 Cleanup 前取消的测试上下文，已改为独立有界清理上下文。

- 修改前相关四包基线通过。
- `go test ./... -count=1 -json`：1147 pass / 61 skip / 0 fail，包含子测试。未开启的外部集成入口不计作通过。
- 显式配置 NATS/etcd/Redis 的原型全套 `-race`：69 pass / 1 skip / 0 fail；skip 为外部编排比较进程入口。其后补强直连错误 owner 断言，再运行本轮三个测试及子测试 `-race`：8 pass / 0 skip / 0 fail。
- `go vet ./internal/actortrial ./modules/agent ./modules/discovery ./modules/rpc` 通过。

关键证据：真实 TCP gRPC 与 NATS 共用激活和邮箱；被直连业务占用时，NATS 已接受请求排队等待；关闸后两种路径均拒绝新请求，已排队请求仍完成。模拟 Flush 失败不晋升，重试成功后真实 etcd Slot Watch 更新两种调用。缓存失效后明确拒绝，不回源绕过；直连旧实例身份以及加载到错误 owner 均被拒绝。取消不会撤销或重投已接受工作，业务错误码与 ACK 互斥保留。

直连请求进入业务、尚未开始交接控制时，目标 NATS 连接的 InMsgs/OutMsgs 未增长。这证明该请求绕过 NATS，不是总链路字节量或云账单测量。

## 新增基线与局限

独立执行五轮 `-benchtime=1s -benchmem`，默认 GOMAXPROCS=16；采集只在 `_test.go`。下表取五轮各列中位数：

| 基准 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 缓存归属到传输调用边界 | 187.5 | 8 | 1 |
| 1KiB 请求、完成 ACK、同进程 TCP gRPC | 151027 | 19516 | 199 |

第一项包含示例 worldSlot 字符串构造、真实 SlotDirectory/ServiceDirectory 缓存读取和传输分派，不含网络或业务；持久端留空，若误回源会直接失败。第二项预热后顺序调用一个实体，含当前复用通用 RPC 的嵌套信封开销；调用方和接收方都在同一个 Go 进程，分配计入两侧。不是 Agent/MsgQ 完整上行，也不是并发吞吐或 P99。

不与历史 Docker NATS 样本直接相除，不据此宣称直连性能领先或无退化。本轮正式生产 Go 路径未修改；新增内部试验 Go +634/-0，其中适配代码 166 行、测试和基准 468 行。加上此前未提交 T3d–T3f，累计 Go +1634/-3，净增 1631 行；这是新能力及实验投入，不是瘦身收益。

原始材料：`/private/tmp/tx-routing-{baseline.log,red.log,integration.log,race.jsonl,final-race.jsonl,full.jsonl,benchmark.log}`。Red 日志最初还包含已修正的测试结构字面量语法错误；随后独立执行已确认只剩新增能力未定义错误。

## 复查与下一项

后置 ponytail-review 范围仅本轮差异：保留双实现 Caller、项目命令解析 SPI 和关键可靠性测试，不新增永久双 Actor 后端。结论：Lean already. Ship. 没有本轮范围内可直接删除的多余抽象；正式服务注册与性能取舍保留为下一阶段事项。

下一项建议：为完整 Agent→Game 上行建立同环境直连/NATS 对照，保持 Seq/Ack、MsgQ 和处理完成语义；分别统计平均/峰值流量、实际链路字节与跨可用区比例，再决定正式 T4 接线和容量验收预算。本批未接 Discovery 实例租约、双 Watch 生命周期和 GD/Fusion 示例，不把它们标为完成；现有 gRPC 服务注册的组合方式也由 T4 在真实服务装配时确认。部署地域及云产品尚未确定，不能承诺云流量费节省比例。
