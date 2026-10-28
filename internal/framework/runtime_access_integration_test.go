package framework

import (
	"context"
	"crypto/rand"
	"errors"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	txruntime "github.com/ngnl47/tx/runtime"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"testing"
	"time"
)

// 使用正式 Server 生命周期验证同进程不同服务不会借用调用方能力，停止后旧上下文失效。
func TestFormalServiceAccessLifecycle(t *testing.T) {
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Skip("设置 NATS/etcd 地址验证正式服务作用域")
	}
	owner := rand.Text()
	prefix := "/tx/tests/runtime-access/" + owner
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	marker := prefix + "/owner"
	result, err := client.Txn(t.Context()).If(clientv3.Compare(clientv3.Version(marker), "=", 0)).Then(clientv3.OpPut(marker, owner)).Commit()
	if err != nil || !result.Succeeded {
		t.Fatalf("隔离范围: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if !t.Failed() {
			if _, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Value(marker), "=", owner)).Then(clientv3.OpDelete(prefix+"/", clientv3.WithPrefix())).Commit(); err != nil {
				t.Error(err)
			}
		}
	}()
	var sender, received context.Context
	var callback bool
	kind := txactor.MustType("role", func(c txactor.Context) (*txactor.Behavior, error) {
		received = c.Context
		if txruntime.Actor(c) == nil {
			t.Error("激活未绑定 Actor 能力")
		}
		return &txactor.Behavior{Receive: func(c txactor.Context, _ any) (any, error) {
			if txruntime.ProviderFrom(c) == txruntime.ProviderFrom(sender) {
				t.Error("接收端继承了调用方能力")
			}
			if txruntime.Actor(c) == nil {
				t.Error("消息回调未绑定 Actor 能力")
			}
			return txactor.Result{MessageType: "done"}, nil
		}, OnDeactivateBefore: func(c txactor.Context) error {
			callback = true
			if txruntime.Actor(c) == nil {
				t.Error("排水提前失去能力")
			}
			return nil
		}}, nil
	})
	app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "",
		testService{spec: Spec{Name: "sender"}, start: func(c Context) error { sender = c.Tasks(); return nil }},
		testService{spec: Spec{Name: "receiver", ActorTypes: []txactor.Type{kind}, Requires: []CModID{CModActor}}})
	app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := app.Start(ctx, RunOptions{Services: []string{"sender", "receiver"}, RPCListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	defer app.Stop(context.Background())
	rejected := func(ctx context.Context) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("不应允许取得 Actor 能力")
			}
		}()
		txruntime.Actor(ctx)
	}
	rejected(sender)
	callCtx, callCancel := context.WithTimeout(sender, 5*time.Second)
	defer callCancel()
	if _, err := app.runtime.actor.Call(callCtx, txactor.Identity{Type: "role", ID: "1"}, txactor.Delivery{MessageType: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !callback {
		t.Fatal("未执行排水回调")
	}
	rejected(received)
}

// 验证正式通信入口已就绪时，启动钩子可加载 Actor，但本地与远端业务仍被拒绝。
func TestFormalActorStartupAdmission(t *testing.T) {
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Skip("设置 NATS/etcd 地址验证正式启动准入")
	}
	namespace := rand.Text()
	prefix := "/tx/tests/service-startup/" + namespace
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer func() {
		if !t.Failed() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.Delete(ctx, prefix+"/", clientv3.WithPrefix())
			if err != nil {
				t.Error(err)
			}
		}
	}()
	kind := txactor.MustType("world", func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) { return txactor.Result{MessageType: "done"}, nil }}, nil
	})
	id := txactor.Identity{Type: "world", ID: "1"}
	entered, release := make(chan struct{}), make(chan struct{})
	var actors txactor.Runtime
	world := phasedService{testService: testService{spec: Spec{Name: "world", Requires: []CModID{CModActor}, ActorTypes: []txactor.Type{kind}}},
		initHook: func(c Context) error { actors = c.Actor(); return nil }, startHook: func(ctx context.Context) error {
			if _, err := actors.EnsureActivation(ctx, id); err != nil {
				return err
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", world)
	app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: namespace}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- app.Start(ctx, RunOptions{Services: []string{"world"}, RPCListen: "127.0.0.1:0"}) }()
	select {
	case <-entered:
	case err := <-started:
		t.Fatalf("启动钩子未到达: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer app.Stop(context.Background())
	defer close(release)
	if _, err := actors.Call(ctx, id, txactor.Delivery{MessageType: "test"}); !errors.Is(err, lifecycle.ErrNotReady) {
		t.Fatalf("本地启动准入: %v", err)
	}
	// 第二个正式 Runtime 只发送，不承载 world；复用同一共享规格和 NATS 命名空间。
	sender := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", world, testService{spec: Spec{Name: "sender", Requires: []CModID{CModActor}}})
	sender.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: namespace}
	if err := sender.Start(ctx, RunOptions{Services: []string{"sender"}, RPCListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	defer sender.Stop(context.Background())
	if _, err := sender.runtime.actor.Call(ctx, id, txactor.Delivery{MessageType: "test"}); !errors.Is(err, lifecycle.ErrNotReady) {
		t.Fatalf("远端启动准入: %v", err)
	}
	// 退出测试前释放启动钩子，等待正式生命周期正常停止。
	release <- struct{}{}
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if _, err := sender.runtime.actor.Call(ctx, id, txactor.Delivery{MessageType: "test"}); err != nil {
		t.Fatal(err)
	}
}
