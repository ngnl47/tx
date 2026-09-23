# 四机压测暂停与新实例续接 Handoff

记录日期：2026-09-21。用户因额度不足，准备销毁当前四台云实例；额度恢复后重新租四台继续。**本次不继续压测，不自动创建云资源，不执行 Git commit。** 实例销毁由用户操作，本文不代表已经销毁。

2026-09-23已按本文在新四机恢复测试；新地址、短测结果和长测准备阶段故障见[续接记录](2026-09-23-four-host-uplink.md)。本文以下保留9月21日交接快照。

## 续接入口与当前结论

先读各工程 AGENTS.md、`docs/IMPLEMENTATION_PLAN.md` 和本文，再核对实际 Git 状态。不要从旧对话或旧公网地址推断新环境；不恢复 ProtoActor，不改变上下行确认契约，不为提高数字替换测试业务。串行实施，不使用子代理；每轮验证后做 ponytail-review，完整 audit 仅在用户明确要求时进行。

当前上下行使用客户端→Agent→gRPC→Game→MsgQ→Agent→客户端。万连对应万名角色、各自的 Actor 邮箱，不强行改成共享144协程池。Core NATS Actor RPC、JetStream可靠任务容量属于另一组测试，本次Ping通过不能外推为它们的容量。压测工具与采样继续隔离于生产路径，复用现有客户端和示例业务，不另写压测专用业务实现。

已完成：

- 修复 `Sender.PubRole` 的旧归属缓存问题。只有接收端在邮箱快照/入队前返回 `ErrRoleBindingChanged`，发送端才清理对应Generation、重新寻址并重试一次；gRPC以ErrorInfo保留该标识。
- 普通会话错误、超时、断连、取消、未知结果、部分投递不据此重发；不重新执行Handler或Procedure。最终下行失败不返回ProcessedUpSeq。
- 四机同一批八服务进程上，角色480100连续重登录5次，每次等待旧Redis归属自然释放、不清库、不重启，均通过3/3；原来第二次登录首条Pong超时不再复现。
- 随后万连×1次/s×30秒完成300000/300000，错误0、漏发0，P50/P95/P99=0.684/1.452/2.844ms，最大10.439ms；两个Agent各5000会话。前一版同档P99=2.873ms，未见明显退化，不宣称单次差值是性能收益。
- 四机完整窗口秒级采样CPU平均空闲：接入76.23%、Game82.57%、MsgQ及依赖76.93%、客户端94.30%，steal均0。跨机时钟未独立校准，不据此计算精确单跳延迟。
- 测后八Go进程均正常排水退出，Exit=0；客户端、资源采样、临时分发服务已结束。最后核验时中间件仍运行，云主机仍存在；销毁后无需保留Redis/Mongo测试数据，按夹具重建。

**下一项明确：新机器先建立同档基线，再测四机万连×1.5/s及2/s的30秒筛选；通过档位再做600秒稳态。** 暂定验收零错误、零漏发、P99≤100ms；失败档位不直接做长测，先看资源与实际链路。更高频率尚未验收；不要再把单机资源竞争的结果解释为代码容量上限。

## 代码及证据锚点

本轮实际测试代码：tx `20bcf48`、txgame `e7df427`、txclient `f127aa7`。六工程完整提交号保存在本地备份的 `revisions.json`。续接时重新确认是否有新提交/未提交改动，保留用户改动；若换代码必须记录版本变化，不把旧测试当新验证。

云端实际构建：Go1.27.0、linux/amd64、CGO_ENABLED=0、`-tags txbench`，二进制SHA-256：

```text
72d79864c85baae5315b5d1ea70e9e6e3d2b7008dda6f556eeb631658905763f
```

构建路径会影响二进制哈希；重建后要求三台服务机彼此一致，并记录新哈希，不强求与此哈希相同。上次公网上传大二进制很慢，采用核对源码、差量上传、云端编译、私网分发。临时分发只能提供目标文件、绑定私网并限时关闭，不暴露整个工作区。

**关键证据已由/tmp移至持久本地目录，不依赖旧云盘：**

```text
/Users/a47/workspace/tx-artifacts/cloud-handoff-20260921/
```

其中：

- `tx-fixed-evidence-20260921.tgz`：本轮五次重登录、万连报告、原始延迟、四机资源、日志、进程身份、构建及退出证据。SHA-256=`369a511f8cc036c0df86498ad29a1648630b32fd5364321381fa2879f6877a19`。
- `tx-fourhost-evidence-20260921.tar.gz`：修复前四机基线。
- `tx-session-relogin-20260921.tar.gz`：修复前第一次通过、第二次失败、第三次恢复的原故障证据。
- `tx-cloud*evidence*.tar.gz`：已经下载的单机原始基线；详细结果见[单机记录](2026-09-21-cloud-uplink-baseline.md)。
- `compose.yml`、`nats.conf`：旧实例实际中间件配置；`deployment/`：实际app.yml、进程表、拓扑及start.py。
- `tx-relogin-check.py`、`tx-10k-fixed.sh`：复用正式客户端CLI的临时编排，**含旧IP，必须替换后使用**。
- `SHA256SUMS`：备份全部文件的校验清单。私钥不在备份中。

该目录不属于tx Git仓库，不随文档commit上传；保留本机目录即可续接，不要只保留/tmp中的副本。详见[四机验证记录](2026-09-21-four-host-uplink.md)。

## 新机器布局与替换清单

四台尽量保持腾讯云北京六区、标准型SA9、8vCPU/16GiB、Ubuntu Server24.04 LTS x86_64、50GiB增强型SSD，处于同一个VPC及可私网互通的子网。硬件/可用区变化要记录并重建基线，不直接换算性能增益。公网用于SSH/部署，正式压力全部走私网，不经本地VPN公网链路。

| 职责 | 旧公网（仅识别历史） | 旧私网（必须替换） | gRPC端口 | 控制端口 |
| --- | --- | --- | --- | --- |
| Login×2、Agent×2 | 49.232.109.103 | 172.21.16.10 | Agent19210/19214；Login19250/19260 | 19212/19216；19252/19262 |
| MsgQ×2、Docker依赖 | 43.138.13.224 | 172.21.16.15 | 19220/19224 | 19222/19226 |
| Game×2 | 62.234.122.205 | 172.21.16.14 | 19230/19240 | 19232/19242 |
| 客户端进程×2 | 118.89.239.186 | 172.21.16.12 | 无 | 无 |

新租赁完成后需要用户提供四台公网/私网IP、角色分配及SSH密钥路径。原SSH用户ubuntu；原密钥在 `/Users/a47/.ssh/tx-perf-20260921`，可复用但先确认新实例已关联对应公钥。不要读取或写入文档任何私钥内容。旧known_hosts是 `/tmp/tx-perf-20260921-known-hosts`，新实例使用新的记录文件，不盲目信任地址复用后的旧主机身份。

替换 `deployment/` 所有YAML、topology.json、processes.json、脚本内四个旧私网IP，以及Compose的端口绑定和etcd advertise-client-urls。Agent TCP19000/19002、WS19001/19003；Login HTTP19004/19005。Agent对客户端公布的地址必须是新私网地址，WebSocket origins也核对。用文本搜索确认旧地址没有残留，再从客户端机器限时探测私网端口。此前安全组未放通导致超时，需放通组内实例私网通信；不能以SSH成功代替私网验收。

## 重建顺序

1. 在新机器确认CPU/内存、OS、磁盘、时钟及SSH，检查是否有其他负载。记录实际配置。
2. MsgQ机器安装Docker及Compose。用本地备份的实际Compose替换IP后启动：`sudo docker compose -p tx-perf up -d --wait`。固定Redis8.8.0、Mongo8.0.26、etcd3.5.15、NATS2.14.6-alpine；不直接升级latest。旧Docker29.1.3，安装版本若不同也记录。NATS开启JetStream，64MB内存/1GB文件、sync_interval=always；本轮不是NATS高可用验收。
3. 宿主机安装Go1.27.0（构建主机）与Node26.0.0（客户端），按go.sum/package-lock.json恢复依赖。发送tx、txgame、txclient当前代码，保留相邻目录，满足txgame/server/go.mod的`../../tx`替换；包含go:embed的JSON/Lua文件，不传Git历史、node_modules或旧数据。核对文件哈希。
4. 在txgame/server构建：`CGO_ENABLED=0 go build -tags txbench -o <部署目录>/bin/server ./cmd/server`，另构建 `go build -o <部署目录>/cluster-fixture ./cmd/cluster-fixture`。三台服务机使用相同服务二进制。客户端运行`npm ci`，按package.json执行测试/构建；失败先排查。
5. 按备份结构布置 `~/tx-perf/fourhost/`。start.py优先找bin/server，否则bin/txgame-server，统一使用bin/server即可。每服务GOMAXPROCS=2、LimitNOFILE65536、warn日志；TXGAME_BUILD_VERSION=cluster-e2e，TXGAME_INSTANCE_ENDPOINT/TXGAME_RPC_LISTEN为各自私网RPC地址。保留独立agent-b/login-b assets。关闭pprof、调度/GC日志等主动采样，`txbench`诊断构建口径与旧轮一致。
6. **只有八服务全部停止时**运行 `./cluster-fixture --action prepare`。该工具仅允许Redis DB12、Mongo `txgame_cluster_e2e`、etcd `/tx/e2e/txgame/`命名空间。不得清生产数据，不在活跃测试中prepare/cleanup。
7. 按MsgQ→Game→Login/Agent启动（start.py使用systemd临时单元）。核对八服务active、监听地址/发现信息和四依赖健康。Login就绪后在MsgQ机器运行 `./cluster-fixture --action roles --login http://<接入私网>:19004 --count 10000 --role-base 480100`。
8. 先小规模冒烟与连续重登录。`tx-relogin-check.py`中的IP需替换；各轮等待角色归属键自然消失，不把清库或重启当修复。新的结果目录必须独立，避免脚本mkdir失败或旧start文件提前放行。
9. 再运行万连短测，核对准备屏障有10000个唯一SessionID、角色范围、Agent分布。启动资源采样后才释放start屏障，正式窗口结束再停采样。记录客户端单调时钟端到端延迟；跨机阶段诊断有时钟误差。
10. 测后按Login/Agent→Game→MsgQ排水，记录每个退出码；结束采样及客户端，归档报告/配置/代码版本/哈希。依赖可保留供下一档重用；需要重置夹具必须等服务全停。

## 万连执行参数与晋级

在客户端源码目录执行（路径以新部署为准）：

```sh
ulimit -n 65536
TXCLIENT_PERF_TIMINGS=1 node --import tsx src/cli/cluster-perf.ts \
  --login http://<接入私网>:19004 \
  --processes 2 --concurrency 10000 --role-base 480100 \
  --rate-per-session 1 --connect-rate 50 \
  --warmup-ms 2000 --duration-ms 30000 \
  --request-timeout-ms 5000 --setup-timeout-ms 900000 \
  --artifact <新结果目录>/client.json \
  --ready <新结果目录>/ready.json --start <新结果目录>/start
```

命令等待start文件；等待ready存在、核对身份并启动采样后由编排侧创建start。总建连50/s，万连准备本就需要数分钟；不要把准备阶段计入正式吞吐。报告须同时检查status、planned/completed/failed/notSent，不能仅看shell退出码或正式failed=0，准备阶段超时也算失败。

新机器1/s短测通过后仅将rate-per-session改为1.5、2，分别应计划450000、600000次；其他参数不变，每档使用独立目录。通过档位再将duration-ms改600000。正式资源采样应覆盖完整测量，保留每秒完成量、延迟原始样本、vmstat及进程CPU；出现瓶颈再有界启用pprof，不把profile开销与无采样基线混为一组。

当前四机只通过1/s短测，尚未测1.5/s、2/s及四机长测。单机曾在1.5/s漏发4、2/s漏发20185；这些是单机资源竞争结果，不能作为四机结论。不要跳到用户最初10/s或20/s高频目标，不外推未测容量。

## 本次交接的提交边界

本轮没有代码改动或新压测；只追加handoff、验证记录和实施计划。上轮五次登录与万连测试已完成，不重复声称为handoff本轮新测试。交付前检查文档差异与备份校验；不提交Git。后续提交建议按实际未提交内容累计且分工程提供。
