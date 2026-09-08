# GoAkt + etcd 可行性实验

## 结论

本轮不建议将 GoAkt 直接替换进 tx。它已证明能使用现有 etcd 完成两进程发现、远程请求和正常关闭后的 Actor 重建，但没有证明可以收敛 tx 当前的严格排水与持久化失败重试。普通 Actor 与 Grain 的关闭行为不同，不能笼统认为更换运行时就解决了关闭一致性。

自定义 BON 编码能接入并完成真实跨进程请求/响应，但本轮通用 BON 信封实现明显慢于当前 Protobuf 信封。因此不迁移通信编码，也不以减少两次对象转换为依据宣称性能提升。

这是选型实验，不是生产适配验收；未修改 tx 运行时代码或第三方源码。只有本报告进入仓库，临时探针不加入常规测试集。

## 环境与边界

- tx 基线：`7587f3c`。开始时六个工程工作区干净。
- GoAkt 固定 `v4.5.4`，Go `1.27.0`，darwin/amd64，Intel i9-9880H。
- 独立模块：`/private/tmp/tx-goakt-spike-WbSiWI`，通过 replace 引用本地 tx；不改变 tx 的 go.mod/go.sum。
- 真实 etcd：`127.0.0.1:2379`。两次运行分别使用独立 ActorSystemName：`tx-goakt-probe-1788869971291039000`、`tx-goakt-probe-1788870133932804000`。
- 每次启动两个独立操作系统进程，动态本地端口和独立工作目录。通过 HTTP 控制探针，业务请求使用 GoAkt 远程 Actor 通道。
- 实验配置 replica=1、minimumPeersQuorum=1、partitionCount=17，仅验证两节点功能，不代表生产高可用配置或网络分区安全性。
- 本轮不读写 Redis/Mongo 业务数据，不扫描任何 Redis key。未测试网络分区、异常崩溃恢复、长期运行、生产吞吐或跨进程 GD 持久化。

## 生命周期结果

| 探针 | 观察 | 对 tx 的意义 |
| --- | --- | --- |
| 普通 Actor 在途请求与系统 Stop | 连续三次观察到 Receive 尚未释放时 PostStop 已执行，Stop 返回 nil；迟到请求返回 actor is not alive | 不能直接把持久化收尾放进 PostStop 并认为它与业务处理串行 |
| Grain 在途请求与系统 Stop | 连续三次等待 OnReceive 完成后执行 OnDeactivate，未观察到重叠 | Grain 的邮箱停用路径更接近需要的串行边界，但仅覆盖这个正常关闭场景 |
| 全局关闭钩子持续报错 | 初次调用+两次重试，共三次后 Stop 返回错误；Running=false、Actor 已停止；再次 Stop 返回未运行 | 关闭钩子失败不是资源保留屏障，不能复用当前 tx 的失败后重试收尾语义 |
| Grain OnDeactivate 报错 | 连续三次：钩子已调用，系统 Stop 返回 nil，Running=false，再次 Stop 返回未运行 | 不能以系统 Stop 的成功返回代替业务数据落地成功确认 |
| 两进程正常关闭重建 | B 请求 A 上的 role-b，A 正常关闭后 B 上出现同名 Actor | 远程寻址和正常重建可用；重建不是恢复业务内存 |

普通 Actor 探针保留失败断言，不把不符合 tx 目标的行为改成通过：

```text
late request while stopping=actor is not alive
accepted request=<nil>; stop=<nil>
completed business messages=1; PostStop before handler release=true
FAIL: PostStop overlapped active handler
```

简单例子：角色正在执行“钻石 +100”，此时 PostStop 若开始保存 GD，保存可能与修改重叠。tx 当前是在停止底层 Actor 系统之前先关闭业务准入、等已接收业务结束、完成必要的持久化收尾。换成 GoAkt 普通 Actor 后，这层业务排水仍不能直接删除。

Grain 的串行停用改善了上述时序，但 OnDeactivate 保存失败时，系统停止仍可能成功返回。本轮注入点是返回一个错误，不是实际 Mongo 故障。因此结论是“运行时关闭结果不足以表达持久化结果”，不是“真实业务数据库已发生丢失”。

固定版本源码与实测一致：

- [actorSystem.shutdown](https://github.com/Tochemey/goakt/blob/v4.5.4/actor/actor_system.go#L3205)：收集关闭钩子错误后仍继续释放 Actor、集群和远程通信资源。
- [PID.Shutdown](https://github.com/Tochemey/goakt/blob/v4.5.4/actor/pid.go#L1611) 和 [PID.doStop](https://github.com/Tochemey/goakt/blob/v4.5.4/actor/pid.go#L2889)：普通 Actor 的外部停止路径直接调用 PostStop。
- [Grain 关闭入口](https://github.com/Tochemey/goakt/blob/v4.5.4/actor/actor_system.go#L3285)：通过邮箱投递停用消息，并受关闭期限约束。
- [grainPID.deactivate](https://github.com/Tochemey/goakt/blob/v4.5.4/actor/grain_pid.go#L260)：失败时也会标记非激活并发出停用完成信号。

本轮没有修改第三方源码，没有执行 race 检测；这里的“重叠”是两个钩子的时序证据，不是 Go race 检测结论。

## 两进程与自定义序列化

默认 Protobuf 请求：

```text
before=8669:1 stop=stop=<nil> after=8670:1
PASS
```

自定义 BON 请求（1 KiB Payload 与 Session 身份）：

```text
before=8943:1 stop=stop=<nil> after=8946:1
PASS
```

冒号左侧是进程 PID，右侧是 Actor 内存计数。`A:1 → B:1` 表明 B 创建的是新对象，不能将其解释为 A 的业务内存迁移。GD 的加载、WAL、checkpoint 和世界槽位归属仍需 tx 适配。

自定义 Serializer 通过 `remote.WithSerializers` 注册实验信封类型，发送与响应均走真实远程调用，接收端检查 Payload 与 SessionID。它证明不必将业务信封转换成 protobuf.Message；不证明可把带存储状态的活跃 GD 对象直接传给别的 Actor，也不代表消除了 GoAkt 所有内部协议和缓冲区复制。

## 编码基准

比较当前 `Delivery.ToProto → proto.Marshal → proto.Unmarshal → DeliveryFromProto` 与实验 `Delivery → BON 信封 → Delivery`。两边均执行基本有效性检查并验证回读数据、源 Payload 不被解码结果修改。

自定义信封仅覆盖本轮数值路由、正常 Session 身份和已有字段，不是所有路由类型、异常输入及协议版本的完整实现。基准不含网络、运行时外层帧、压缩、业务 BON 解码或 Handler。

每组运行三次、每次 300ms，表中时间为中位数：

| Payload | 当前 Protobuf | 自定义通用 BON | BON/Protobuf | 当前分配 | BON 分配 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1.282 μs/op | 6.161 μs/op | 4.81 倍 | 880 B、10 次 | 4480 B、56 次 |
| 1 KiB | 2.180 μs/op | 12.575 μs/op | 5.77 倍 | 4784 B、10 次 | 8800 B、58 次 |
| 16 KiB | 14.774 μs/op | 118.783 μs/op | 8.04 倍 | 68144 B、10 次 | 122592 B、67 次 |

额外隔离 Protobuf 对象适配步骤后，仅 Marshal/Unmarshal 的中位数为 0.901、1.318、7.595 μs/op，均为 5 次分配。这个下界说明适配/校验和显式 Payload 复制值得单独测量，但不表示可以在不改变所有权契约的条件下直接删掉复制。

当前 Payload 已是业务 BON 字节，不存在完整 GD 先编码成 BON、再整体转换成 Protobuf 对象、再反转两次的流程。可省的是外层信封适配和部分复制，而不是两遍完整业务序列化。反射式 BON 的实际开销超过了本轮节省量。

单机微基准不是集群吞吐结论，也不排除未来专用生成编码器更快。暂不为了追求这一可能性新增编码格式或改造 txgen。

## 复现入口

临时模块有四个文件：`lifecycle_test.go`、`grain_test.go`、`codec_test.go`、`serializer_test.go`。它们是实验探针，不作为 tx 常规测试集；临时目录会受系统清理策略影响。

```sh
cd /private/tmp/tx-goakt-spike-WbSiWI
go test -tags=hashicorpmetrics -run 'TestFailedHook|TestGrain|TestCodec' -v -count=3 -timeout 30s
go test -tags=hashicorpmetrics -run '^TestInFlightShutdown$' -v -count=3 -timeout 30s
SPIKE_ETCD=1 go test -tags=hashicorpmetrics -run '^TestEtcdTwoProcessesRelocation$' -v -count=1 -timeout 120s
SPIKE_ETCD=1 SPIKE_BON=1 go test -tags=hashicorpmetrics -run '^TestEtcdTwoProcessesRelocation$' -v -count=1 -timeout 120s
go test -tags=hashicorpmetrics -run '^TestCodecRoundTrip$' -bench '^BenchmarkEnvelope$' -benchmem -count=3 -benchtime=300ms
```

第二条命令预期失败，用于复现目标语义不满足；其余本轮均通过。不要用全目录测试是否全绿来替代对实验现象的判断。

## 下一步建议（待确认）

先讨论是否仍有独立于排水问题的迁移动机。若主要目标是省去严格排水与失败重试，本轮证据不足以支持迁移，建议继续使用当前运行时和已验收策略。若仍希望采用 GoAkt，应先选择 Grain 还是普通 Actor，再评审 tx 的槽位归属、虚拟激活、持久化关闭屏障、远程身份与错误语义如何适配；不要直接替换全工程。

Protobuf 信封复制优化是另一项独立工作，只有性能基线表明收益足够且所有权边界清楚时再实施。未批准开始正式迁移或编码改造。
