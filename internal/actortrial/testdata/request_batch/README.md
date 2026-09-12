# 请求封包实验

T3f 仅在 `go test -overlay` 构建中接入 `comparison_batch*_test.go`。正式源码、API、依赖版本和默认配置不变。设计和停止条件见 [评估稿](../../../../docs/audits/2026-09-13-nats-request-batch-assessment.md)，结果见 [实验报告](../../../../docs/audits/2026-09-13-nats-request-batch.md)。

在 tx 根目录执行，生成 Linux/amd64 的候选测试二进制：

```sh
python3 internal/actortrial/testdata/request_batch/build.py /private/tmp/tx-batch64 --limit 64
python3 internal/actortrial/testdata/request_batch/build.py /private/tmp/tx-batch1 --limit 1
```

本机正确性与 race 检查只需生成 overlay。`tx_nats_batch` 标记必须与 overlay 一起使用；普通测试仅包含不依赖候选接线的发送器检查。

```sh
python3 internal/actortrial/testdata/request_batch/build.py /private/tmp/tx-batch-check --overlay-only
TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222 \
TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379 \
TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379 \
go test -race -tags=tx_nats_batch -overlay=/private/tmp/tx-batch-check/overlay.json ./internal/actortrial -count=1
```

实验 protobuf 使用 go.mod 锁定的 v1.36.11 生成器。更改定义后从输入重新生成，不手改生成物：

```sh
mkdir -p /private/tmp/tx-batch-proto
go build -mod=readonly -o /private/tmp/tx-batch-proto/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
protoc --plugin=protoc-gen-go=/private/tmp/tx-batch-proto/protoc-gen-go \
  --go_out=/private/tmp/tx-batch-proto --go_opt=paths=source_relative \
  -I internal/actortrial/testdata/request_batch \
  internal/actortrial/testdata/request_batch/request_batch.proto
cp /private/tmp/tx-batch-proto/request_batch.pb.go internal/actortrial/comparison_batch.pb_test.go
```

`build.py` 只覆盖 `nats.go` 与 `slots.go` 的实验接线：请求入队、同订阅拆包、pending 锁内清理、两条排水路径。逐项回复仍由原 worker 直接 Publish。计数器仅在报告指向的独立诊断构建中启用，不能混入正式吞吐样本。
