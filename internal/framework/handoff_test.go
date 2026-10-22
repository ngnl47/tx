package framework

import (
	"context"
	"crypto/rand"
	"errors"
	txactor "github.com/ngnl47/tx/modules/actor"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorrpc"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/control"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestPrepareHandoffRetainsRPCAndRetriesPersistence(t *testing.T) {
	var flushes, stops atomic.Int32
	taskStopped := make(chan struct{})
	want := errors.New("写回暂时失败")
	mod := &retryFlushCMod{flush: func() error {
		select {
		case <-taskStopped:
		default:
			t.Error("后台业务尚未结束就冲刷状态")
		}
		if flushes.Add(1) == 1 {
			return want
		}
		return nil
	}}
	app, err := New(Config{AppID: "prepare-handoff"}, WithCMod(mod), WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{CModRPC, mod.Spec().ID}},
		start: func(c Context) error {
			if err := c.RPC().Register(func(r grpc.ServiceRegistrar) error {
				healthpb.RegisterHealthServer(r, health.NewServer())
				return nil
			}); err != nil {
				return err
			}
			return c.Tasks().Go(func(ctx context.Context) { <-ctx.Done(); close(taskStopped) })
		},
		stop: func(context.Context) error { stops.Add(1); return nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Start(t.Context(), RunOptions{Services: []string{"game"}, ControlListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if app.Stop(context.Background()) != nil {
			_ = app.Stop(context.Background())
		}
	})
	// 本测试只隔离身份来源；真实租约与缓存由 I1 的组合测试覆盖。
	app.runtime.setInstance(discovery.Instance{ID: "startup-a"})
	root := app.runtime.lifecycle
	conn, err := grpc.NewClient(app.runtime.rpc.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	controlConn, err := grpc.NewClient(app.control.endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer controlConn.Close()
	client := controlpb.NewHandoffServiceClient(controlConn)
	if _, err := client.PrepareHandoff(t.Context(), &controlpb.PrepareHandoffRequest{RuntimeInstanceId: "startup-old"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("控制面未拒绝旧启动身份: %v", err)
	}
	if err := app.PrepareHandoff(t.Context(), "startup-old"); err == nil {
		t.Fatal("旧启动身份进入交接")
	}
	if flushes.Load() != 0 {
		t.Fatal("身份错误仍执行收尾")
	}
	if err := app.PrepareHandoff(t.Context(), "startup-a"); !errors.Is(err, want) {
		t.Fatalf("首次准备: %v", err)
	}
	if stops.Load() != 0 || mod.stopped || root.Err() != nil {
		t.Fatal("失败提前释放资源")
	}
	for range 2 {
		if err := app.PrepareHandoff(t.Context(), "startup-a"); err != nil {
			t.Fatal(err)
		}
	}
	if response, err := client.PrepareHandoff(t.Context(), &controlpb.PrepareHandoffRequest{RuntimeInstanceId: "startup-a"}); err != nil || response.GetRuntimeInstanceId() != "startup-a" {
		t.Fatalf("控制面没有确认源实例收尾: %v %v", response, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("准备成功关闭了共享 RPC: %v", err)
	}
	if stops.Load() != 0 || mod.stopped || root.Err() != nil || flushes.Load() != 2 {
		t.Fatal("准备阶段释放资源或重复冲刷")
	}
	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if stops.Load() != 1 || !mod.stopped || root.Err() == nil || flushes.Load() != 2 {
		t.Fatal("最终停止未复用已完成阶段")
	}
	if err := app.PrepareHandoff(t.Context(), "startup-a"); err == nil {
		t.Fatal("停止后仍确认可交接")
	}
}

func TestPrepareHandoffCancellationKeepsCompletedFlushForStop(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	var flushes, stops atomic.Int32
	mod := &retryFlushCMod{flush: func() error {
		flushes.Add(1)
		close(entered)
		<-release
		return nil
	}}
	app, err := New(Config{AppID: "prepare-stop"}, WithCMod(mod), WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}},
		stop: func(context.Context) error { stops.Add(1); return nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock.Do(func() { close(release) }); _ = app.Stop(context.Background()) })
	app.runtime.setInstance(discovery.Instance{ID: "startup-a"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	prepared, stopped := make(chan error, 1), make(chan error, 1)
	go func() { prepared <- app.PrepareHandoff(ctx, "startup-a") }()
	<-entered
	go func() { stopped <- app.Stop(t.Context()) }()
	short, end := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer end()
	if err := app.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop 没有等待正在写回的准备阶段: %v", err)
	}
	if stops.Load() != 0 {
		t.Fatal("写回完成前释放资源")
	}
	cancel()
	unblock.Do(func() { close(release) })
	if err := <-prepared; !errors.Is(err, context.Canceled) {
		t.Fatalf("调用方取消仍确认成功: %v", err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if flushes.Load() != 1 || stops.Load() != 1 {
		t.Fatal("Stop 重复冲刷或未释放资源")
	}
}

func TestPrepareHandoffIdentityLossNeverConfirms(t *testing.T) {
	var app *Server
	want := errors.New("准备期间租约丢失")
	mod := &retryFlushCMod{flush: func() error { app.reportFatal(want); return nil }}
	var err error
	app, err = New(Config{AppID: "prepare-fatal"}, WithCMod(mod), WithService(testService{spec: Spec{Name: "game", Requires: []CModID{mod.Spec().ID}}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	app.runtime.setInstance(discovery.Instance{ID: "startup-a"})
	if err := app.PrepareHandoff(t.Context(), "startup-a"); !errors.Is(err, want) {
		t.Fatalf("身份失效后仍确认准备成功: %v", err)
	}
	waitClosed(t, app.stopDone)
	if !mod.stopped || !errors.Is(app.stopErr, want) {
		t.Fatal("未走致命身份失效退出")
	}
}

func TestPrepareHandoffWithEtcdRetainsLeaseUntilStop(t *testing.T) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 验证正式宿主双入口交接")
	}
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_ENDPOINT 验证正式宿主交接租约")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	owner := rand.Text()
	prefix := "/tx/tests/framework-handoff/" + owner
	marker := prefix + "/owner"
	created, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Version(marker), "=", 0)).Then(clientv3.OpPut(marker, owner)).Commit()
	if err != nil || !created.Succeeded {
		t.Fatalf("创建测试范围: %v", err)
	}
	t.Log("etcd 交接测试范围:", prefix)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		cleanup, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
		result, err := client.Txn(cleanup).If(clientv3.Compare(clientv3.Value(marker), "=", owner)).Then(clientv3.OpDelete(prefix+"/", clientv3.WithPrefix())).Commit()
		if err != nil || !result.Succeeded {
			t.Errorf("清理范围失败或 ownership 不匹配: %v", err)
			return
		}
		left, err := client.Get(cleanup, prefix+"/", clientv3.WithPrefix())
		if err != nil || len(left.Kvs) != 0 {
			t.Errorf("测试范围仍有残留: %v", err)
		}
	})
	var unloaded atomic.Int32
	newServer := func() *Server {
		kind := txactor.MustType("role", func(txactor.Context) (*txactor.Behavior, error) {
			var count uint64
			return &txactor.Behavior{
				Receive: func(txactor.Context, any) (any, error) {
					count++
					return txactor.Result{MessageType: "count", CommandHash: uint32(count)}, nil
				},
				OnDeactivateAfter: func(txactor.Context) error { unloaded.Add(1); return nil },
			}, nil
		}, txactor.WithSlotMapping(func(id txactor.Identity) string { return "game-" + id.ID }))
		app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", testService{spec: Spec{Name: "game", ActorTypes: []txactor.Type{kind}}})
		app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
		return app
	}

	app := newServer()
	if err := app.Start(ctx, RunOptions{Services: []string{"game"}, ControlListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	id := app.Context().Instance().ID
	key := prefix + "/instances/" + id
	before, err := client.Get(ctx, key)
	if err != nil || len(before.Kvs) != 1 {
		t.Fatalf("实例注册不存在: %v", err)
	}
	// 在真实 RPC 成功返回后注入调用方可见的确认丢失，不改生产控制服务。
	var loseConfirmation atomic.Bool
	var confirmations atomic.Int32
	conn, err := grpc.NewClient(app.control.endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
			err := invoke(ctx, method, req, reply, cc, options...)
			if err == nil && method == controlpb.HandoffService_PrepareHandoff_FullMethodName {
				confirmations.Add(1)
				if loseConfirmation.Swap(false) {
					return status.Error(codes.Unavailable, "测试注入：成功确认丢失")
				}
			}
			return err
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// L2 归属组件通过正式控制面复用源实例收尾；同一源实例可以逐槽晋升。
	target := newServer()
	if err := target.Start(ctx, RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := target.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})

	store := app.discovery.AuthoritativeSlots()
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(app.discovery.Directory(), dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	direct := &actorrpc.DirectTransport{Sender: sender}
	remote := target.runtime.actorRPC.(*actorrpc.NATSTransport)
	for _, key := range []string{"1", "2"} {
		if err := store.CreateSlot(ctx, discovery.SlotAssignment{SlotID: "game-" + key, RuntimeInstanceID: id, Generation: 1}); err != nil {
			t.Fatal(err)
		}
		destination := actorhost.Target{Instance: id, Identity: actorhost.Identity{Type: "role", ID: key}}
		result, err := remote.Call(ctx, destination, &actorpb.Delivery{MessageType: "counter"})
		if err != nil || result.CommandHash != 1 {
			t.Fatalf("远端 NATS 激活: %+v %v", result, err)
		}
		result, err = direct.Call(ctx, destination, &actorpb.Delivery{MessageType: "counter"})
		if err != nil || result.CommandHash != 2 {
			t.Fatalf("gRPC 未共用实体状态: %+v %v", result, err)
		}
	}
	targetID := target.Context().Instance().ID
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if instance, ok := app.discovery.Directory().LookupInstance(targetID); ok && instance.Status == discovery.StatusRunning {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	ownership := app.discovery.Ownership()
	preparer := control.NewHandoffClient(conn)
	for _, slot := range []string{"game-1", "game-2"} {
		current := discovery.SlotAssignment{SlotID: slot, RuntimeInstanceID: id, Generation: 1}
		// 第一槽丢确认时不能晋升；第二槽丢确认时不能回滚已完成的第一槽。
		loseConfirmation.Store(true)
		_, err := ownership.Handoff(ctx, "game", current, targetID,
			func(context.Context, string) (bool, error) { return true, nil }, preparer.PrepareHandoff)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("确认丢失被掩盖: %v", err)
		}
		if unloaded.Load() != 2 {
			t.Fatal("确认丢失前源实体未完成卸载")
		}
		for _, pendingSlot := range []string{"game-1", "game-2"} {
			want := discovery.SlotAssignment{SlotID: pendingSlot, RuntimeInstanceID: id, Generation: 1}
			if slot == "game-2" && pendingSlot == "game-1" {
				want.RuntimeInstanceID, want.Generation = targetID, 2
			}
			actual, err := store.ResolveSlot(ctx, pendingSlot)
			if err != nil || actual != want {
				t.Fatalf("确认丢失后归属错误: %+v，期望 %+v: %v", actual, want, err)
			}
		}
		member, err := client.Get(ctx, key)
		if err != nil || len(member.Kvs) != 1 || member.Kvs[0].Lease != before.Kvs[0].Lease {
			t.Fatalf("部分交接提前释放源租约: %v", err)
		}
		destination := actorhost.Target{Instance: id, Identity: actorhost.Identity{Type: "role", ID: "2"}}
		if _, err := remote.Call(ctx, destination, &actorpb.Delivery{MessageType: "counter"}); !errors.Is(err, actorhost.ErrDraining) {
			t.Fatalf("失败交接重新开放源: %v", err)
		}
		// 编排方显式重试同一源；运行时复用成功收尾，不重复卸载或重投业务。
		next, err := ownership.Handoff(ctx, "game", current, targetID,
			func(context.Context, string) (bool, error) { return true, nil },
			preparer.PrepareHandoff)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := store.ResolveSlot(ctx, slot)
		if err != nil || actual != next || next.RuntimeInstanceID != targetID || next.Generation != 2 {
			t.Fatalf("晋升结果: %v %v %v", actual, next, err)
		}
	}
	if confirmations.Load() != 4 || unloaded.Load() != 2 {
		t.Fatalf("交接隐式重试或重复卸载: confirmations=%d unloaded=%d", confirmations.Load(), unloaded.Load())
	}
	for _, key := range []string{"1", "2"} {
		destination := actorhost.Target{Instance: targetID, Identity: actorhost.Identity{Type: "role", ID: key}}
		result, err := app.runtime.actorRPC.(*actorrpc.NATSTransport).Call(ctx, destination, &actorpb.Delivery{MessageType: "counter"})
		if err != nil || result.CommandHash != 1 {
			t.Fatalf("新 owner 懒加载失败: %+v %v", result, err)
		}
		destination.Instance = id
		if _, err := remote.Call(ctx, destination, &actorpb.Delivery{MessageType: "counter"}); !errors.Is(err, actorhost.ErrDraining) {
			t.Fatalf("旧 owner 重新开放: %v", err)
		}
	}
	after, err := client.Get(ctx, key)
	if err != nil || len(after.Kvs) != 1 || after.Kvs[0].Lease != before.Kvs[0].Lease {
		t.Fatalf("准备阶段丢失或替换了租约: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, err := client.Get(ctx, key)
	if err != nil || len(stopped.Kvs) != 0 {
		t.Fatalf("停止后实例仍注册: %v", err)
	}
}
