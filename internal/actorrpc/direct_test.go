package actorrpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// 一个进程的多个实体类型共用同一 DeliveryService 和宿主，不能按 Kind 重复注册服务。
func TestDirectSharedHostKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	slowRelease := make(chan struct{}, 1)
	defer close(slowRelease)
	local := actorhost.New(128, time.Second, func(c txactor.Context) (*txactor.Behavior, error) {
		if c.Identity.Kind == "deny" {
			return nil, actorhost.ErrActivationDenied
		}
		count := 0
		return &txactor.Behavior{Receive: func(_ txactor.Context, _ any) (any, error) {
			if c.Identity.Kind == "slow" {
				<-slowRelease
			}
			count++
			return &actorpb.Result{MessageType: c.Identity.Kind, CommandId: uint64(count)}, nil
		}}, nil
	})
	defer local.Drain(ctx)
	var calls atomic.Int64
	host, err := rpc.NewHost("127.0.0.1:0", grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == rpcpb.DeliveryService_Call_FullMethodName {
			calls.Add(1)
		}
		return handler(ctx, request)
	}), grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == rpcpb.DeliveryService_CallBatch_FullMethodName {
			calls.Add(1)
		}
		return handler(server, stream)
	}))
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { host.Stop(context.Background()); root.Cancel(); root.Wait() }()
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver("a", local))
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{
		{ID: "a", Services: []string{"game", "world"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning},
		{ID: "b", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning},
	}); err != nil {
		t.Fatal(err)
	}
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	transport := DirectTransport{Sender: sender}
	for round := 1; round <= 2; round++ {
		for _, kind := range []string{"role", "world"} {
			result, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: kind, Key: "same-key"}}, &actorpb.Delivery{MessageType: "ping"})
			if err != nil {
				t.Fatal(err)
			}
			if result.MessageType != kind || result.CommandId != uint64(round) {
				t.Fatalf("实体状态串扰: %+v", result)
			}
		}
	}
	before := calls.Load()
	batch := NewBatchingDirectTransport(sender)
	batch.window = 40 * time.Millisecond
	type outcome struct {
		kind   string
		result *actorpb.Result
		err    error
	}
	responses := make(chan outcome, 2)
	start := make(chan struct{})
	for _, kind := range []string{"role", "world"} {
		go func() {
			<-start
			result, err := batch.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: kind, Key: "batch-key"}}, &actorpb.Delivery{MessageType: "ping"})
			responses <- outcome{kind: kind, result: result, err: err}
		}()
	}
	close(start)
	for range 2 {
		response := <-responses
		if response.err != nil || response.result == nil || response.result.MessageType != response.kind {
			t.Fatalf("批量直连结果错误: %+v", response)
		}
	}
	if got := calls.Load() - before; got != 1 {
		t.Fatalf("两个 Actor 调用发起 %d 次 gRPC，期望 1 次", got)
	}
	responses = make(chan outcome, 2)
	start = make(chan struct{})
	for _, kind := range []string{"role", "deny"} {
		go func() {
			<-start
			result, err := batch.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: kind, Key: "error-key"}}, &actorpb.Delivery{MessageType: "ping"})
			responses <- outcome{kind: kind, result: result, err: err}
		}()
	}
	close(start)
	for range 2 {
		response := <-responses
		if response.kind == "deny" {
			if !errors.Is(response.err, actorhost.ErrActivationDenied) {
				t.Fatalf("批内拒绝分类错误: %+v", response)
			}
		} else if response.err != nil || response.result == nil || response.result.MessageType != "role" {
			t.Fatalf("批内其他项应成功: %+v", response)
		}
	}
	if got := calls.Load() - before; got != 2 {
		t.Fatalf("两个批次发起 %d 次 gRPC，期望 2 次", got)
	}
	responses = make(chan outcome, 2)
	start = make(chan struct{})
	for _, kind := range []string{"role", "slow"} {
		go func() {
			<-start
			result, err := batch.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: kind, Key: "latency-key"}}, &actorpb.Delivery{MessageType: "ping"})
			responses <- outcome{kind: kind, result: result, err: err}
		}()
	}
	close(start)
	select {
	case response := <-responses:
		if response.kind != "role" || response.err != nil {
			t.Fatalf("快请求被慢 Actor 阻塞: %+v", response)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("快请求等待了同批慢 Actor 的完成")
	}
	slowRelease <- struct{}{}
	if response := <-responses; response.kind != "slow" || response.err != nil {
		t.Fatalf("慢请求结束后结果错误: %+v", response)
	}
	batch.window = 10 * time.Second
	countCtx, countCancel := context.WithTimeout(ctx, 2*time.Second)
	defer countCancel()
	completed := make(chan error, 100)
	for range 100 {
		go func() {
			_, err := batch.Call(countCtx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: "role", Key: "count-key"}}, &actorpb.Delivery{MessageType: "ping"})
			completed <- err
		}()
	}
	for range 100 {
		if err := <-completed; err != nil {
			t.Fatalf("满 100 条未立即发送: %v", err)
		}
	}
	if got := calls.Load() - before; got != 4 {
		t.Fatalf("第四批满 100 条后累计发起 %d 次 gRPC，期望 4 次", got)
	}
	batch.window = 40 * time.Millisecond
	responses = make(chan outcome, 2)
	start = make(chan struct{})
	large := &actorpb.Delivery{MessageType: "ping", Payload: make([]byte, 1<<20)}
	for _, key := range []string{"large-1", "large-2"} {
		go func() {
			<-start
			result, err := batch.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: "role", Key: key}}, large)
			responses <- outcome{result: result, err: err}
		}()
	}
	close(start)
	for range 2 {
		if response := <-responses; response.err != nil || response.result == nil {
			t.Fatalf("大载荷分批调用失败: %+v", response)
		}
	}
	if got := calls.Load() - before; got != 6 {
		t.Fatalf("超过字节上限后累计发起 %d 次 gRPC，期望 6 次", got)
	}
	expiredBatch, err := proto.Marshal(&actorpb.DirectCallBatch{Items: []*actorpb.DirectCallItem{
		{Kind: "role", Key: "expired", Delivery: &actorpb.Delivery{MessageType: "ping"}, DeadlineSlackNano: int64(6 * time.Second)},
		{Kind: "role", Key: "current", Delivery: &actorpb.Delivery{MessageType: "ping"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make(map[uint64]*actorpb.DirectCallOutcome)
	err = sender.CallBatch(ctx, rpc.Delivery{Route: rpc.InstanceRoute("a"), MessageType: "actor.batchcall", Payload: expiredBatch}, func(item rpc.Result) error {
		var outcome actorpb.DirectCallOutcome
		if err := proto.Unmarshal(item.Payload, &outcome); err != nil {
			return err
		}
		outcomes[item.CommandID] = &outcome
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 || outcomes[0].GetFailure() == nil || outcomes[1].GetResult() == nil {
		t.Fatalf("逐项截止时间未隔离: %+v", outcomes)
	}
	if _, err := batch.Call(ctx, actorhost.Target{Instance: "b", Identity: txactor.Identity{Kind: "role", Key: "stale"}}, &actorpb.Delivery{MessageType: "ping"}); !errors.Is(err, actorhost.ErrNotOwner) {
		t.Fatalf("错误归属未保留 ErrNotOwner 分类: %v", err)
	}
	local.BeginDrain()
	for _, kind := range []string{"role", "world"} {
		_, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: txactor.Identity{Kind: kind, Key: "same-key"}}, &actorpb.Delivery{MessageType: "ping"})
		if !errors.Is(err, actorhost.ErrDraining) {
			t.Fatalf("排水未覆盖 %s: %v", kind, err)
		}
	}
}
