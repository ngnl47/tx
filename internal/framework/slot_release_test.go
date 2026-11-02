package framework

import (
	"context"
	"encoding/json"
	"errors"
	txactor "github.com/ngnl47/tx/modules/actor"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"testing"
	"time"
)

func TestStopReleasesSlotsOnlyAfterPersistence(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "handoff"}[prepare], func(t *testing.T) {
			config, _ := ownershipTestConfig(t)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			failing := true
			app := startOwnershipServer(t, ctx, config, func(txactor.Context) (*txactor.Behavior, error) { return nil, errors.New("本例不激活") }, func() error {
				if failing {
					return errors.New("写回失败")
				}
				return nil
			})
			// 使用独立客户端读取 Stop 关闭 discovery 连接后的最终记录。
			registry, err := discovery.NewEtcdRegistry(ctx, config.Discovery.Etcd)
			if err != nil {
				t.Fatal(err)
			}
			defer registry.Close()
			slots := registry.Slots()
			current := discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: app.runtime.instanceSnapshot().ID, Generation: 1}
			if err = slots.CreateSlot(ctx, current); err != nil {
				t.Fatal(err)
			}
			closeWork := func() error {
				if prepare {
					return app.PrepareHandoff(ctx, current.RuntimeInstanceID)
				}
				return app.Stop(ctx)
			}
			if err = closeWork(); err == nil {
				t.Fatal("失败写回未阻断")
			}
			if got, _ := slots.ResolveSlot(ctx, current.SlotID); got != current {
				t.Fatalf("失败提前释放: %+v", got)
			}
			failing = false
			if err = closeWork(); err != nil {
				t.Fatal(err)
			}
			if prepare {
				if got, _ := slots.ResolveSlot(ctx, current.SlotID); got != current {
					t.Fatal("交接准备不能释放")
				}
				if err = app.Stop(ctx); err != nil {
					t.Fatal(err)
				}
			}
			got, err := slots.ResolveSlot(ctx, current.SlotID)
			if err != nil || (prepare && got != current) || (!prepare && (!got.Released() || got.Generation != 2)) {
				t.Fatalf("关闭归属: %+v %v", got, err)
			}
			if err = app.Stop(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStopDoesNotReleaseWhileAcceptedActorWorkIsRunning(t *testing.T) {
	config, _ := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	app := startOwnershipServer(t, ctx, config, func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) {
			close(entered)
			<-release
			return txactor.Result{MessageType: "done"}, nil
		}}, nil
	}, func() error { return nil })
	defer unblock()
	future, err := app.Context().Actor().CallFuture(ctx, txactor.Identity{Type: "world", ID: "1"}, txactor.Delivery{MessageType: "block"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	registry, err := discovery.NewEtcdRegistry(ctx, config.Discovery.Etcd)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	before, err := registry.Slots().ResolveSlot(ctx, "world-1")
	if err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	err = app.Stop(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未等待执行中的邮箱: %v", err)
	}
	if got, err := registry.Slots().ResolveSlot(ctx, "world-1"); err != nil || got != before {
		t.Fatalf("工作未结束却释放: %+v %v", got, err)
	}
	unblock()
	if _, err := future.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.Slots().ResolveSlot(ctx, "world-1"); err != nil || !got.Released() || got.Generation != before.Generation+1 {
		t.Fatalf("工作结束后未释放: %+v %v", got, err)
	}
}

// World 启动可以先登记归属再加载业务；加载失败不能被当作成功停服而释放。
func TestStartupFailureKeepsRegisteredSlot(t *testing.T) {
	config, _ := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	registry, err := discovery.NewEtcdRegistry(ctx, config.Discovery.Etcd)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	failure := errors.New("业务加载失败")
	var current discovery.SlotAssignment
	app, err := New(config, WithService(testService{
		spec: Spec{Name: "world", Requires: []CModID{CModDiscovery}},
		start: func(c Context) error {
			current = discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: c.Instance().ID, Generation: 1}
			if err := registry.Slots().CreateSlot(ctx, current); err != nil {
				return err
			}
			return failure
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(ctx, RunOptions{Services: []string{"world"}, InstanceEndpoint: "127.0.0.1:9000"}); !errors.Is(err, failure) {
		t.Fatalf("启动失败被隐藏: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.Slots().ResolveSlot(ctx, current.SlotID); err != nil || got != current {
		t.Fatalf("启动失败释放归属: %+v %v", got, err)
	}
}

func TestControlSlotWritesRejectDrainingTargetWithStaleCache(t *testing.T) {
	config, client := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	app := startOwnershipServer(t, ctx, config, func(txactor.Context) (*txactor.Behavior, error) { return nil, errors.New("无需激活") }, func() error { return nil })
	// 控制面缓存仍 Running，但目标已进入 Draining，复现 Watch 尚未送达的窗口。
	instance := discovery.Instance{ID: "target", Endpoint: "target:1", Services: []string{"world"}, Status: discovery.StatusRunning}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{instance}); err != nil {
		t.Fatal(err)
	}
	app.runtime.directory = directory
	instance.Status = discovery.StatusDraining
	payload, _ := json.Marshal(instance)
	if _, err := client.Put(ctx, config.Discovery.Etcd.Prefix+"/instances/target", string(payload)); err != nil {
		t.Fatal(err)
	}
	if err := app.startControl("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(app.control.endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_, err = controlpb.NewSlotServiceClient(connection).CreateSlot(ctx, &controlpb.CreateSlotRequest{SlotId: "world-1", RuntimeInstanceId: "target"})
	if err == nil {
		t.Fatal("正式控制面把槽位写给已排水的目标")
	}
	if _, err := app.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-1"); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatalf("拒绝后仍产生归属: %v", err)
	}
}

func TestStopReleasesOnlyAfterServiceBackgroundQueueExits(t *testing.T) {
	config, _ := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	registry, err := discovery.NewEtcdRegistry(ctx, config.Discovery.Etcd)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	var current discovery.SlotAssignment
	var closed bool
	app, err := New(config, WithService(testService{
		spec: Spec{Name: "game", Requires: []CModID{CModDiscovery}},
		start: func(c Context) error {
			current = discovery.SlotAssignment{"slot", c.Instance().ID, 1}
			return registry.Slots().CreateSlot(ctx, current)
		},
		stop: func(context.Context) error {
			got, err := registry.Slots().ResolveSlot(ctx, current.SlotID)
			if err != nil || got != current {
				t.Errorf("业务收尾前提前释放: %+v %v", got, err)
			}
			closed = true
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(ctx, RunOptions{Services: []string{"game"}, InstanceEndpoint: "127.0.0.1:9000"}); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.Slots().ResolveSlot(ctx, current.SlotID); err != nil || !closed || !got.Released() {
		t.Fatalf("未正常释放: %+v %v", got, err)
	}
}
