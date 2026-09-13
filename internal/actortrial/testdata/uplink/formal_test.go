//go:build unix

package actortrial

import (
	"context"
	"strconv"
	"time"

	"github.com/ngnl47/tx/bee"
	legacy "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	"github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
)

type uplinkService struct{}

func (uplinkService) Start(context.Context) error { return nil }

// startFormalUplink 仅替换框架装配；业务处理、GD、会话及采样窗口沿用冻结夹具。
// 每进程真实启动 Bee，目录、首次 CAS、权威加载校验和两个调用视图均来自生产实现。
func startFormalUplink(config uplinkConfig, factory func() func(context.Context, legacy.Delivery) (legacy.Result, error)) (
	string, agent.Dispatcher, func(context.Context, legacy.Delivery) (legacy.Result, error), func() error, error,
) {
	var runtime bee.Context
	var registrations []bee.ServiceRegistration
	for _, name := range []string{"game", "world", "agent"} {
		spec := bee.ServiceSpec{Name: name, Requires: []bee.CModID{bee.CModActor}}
		if name != "agent" {
			spec.ActorKinds = []actor.Kind{actor.MustKind(name, func(actor.Context) (*actor.Behavior, error) {
				receive := factory()
				return &actor.Behavior{Receive: func(c actor.Context, message any) (any, error) {
					d := message.(actor.Delivery)
					result, err := receive(c, legacy.Delivery{MessageType: d.MessageType, CommandID: d.CommandID, RouteKey: d.RouteKey, Payload: d.Payload, Identity: d.Identity, UpSeq: d.UpSeq})
					return actor.Result{MessageType: result.MessageType, Payload: result.Payload, ProcessedUpSeq: result.ProcessedUpSeq}, err
				}}, nil
			})}
		}
		registrations = append(registrations, bee.ServiceRegistration{Spec: spec, Factory: func(c bee.Context) (bee.Service, error) {
			runtime = c
			return uplinkService{}, nil
		}})
	}
	server, err := bee.New(bee.Config{
		AppID:     config.Namespace,
		Log:       bee.LogConfig{Level: "error"},
		Actor:     bee.ActorConfig{Runtime: actor.Config{NATSURL: config.URL, Namespace: config.Namespace, MailboxCapacity: 1024, MaxPendingCalls: 4096}},
		Discovery: bee.DiscoveryConfig{Etcd: discovery.EtcdConfig{Endpoints: []string{config.Etcd}, Prefix: "/tx/uplink/" + config.Namespace, LeaseTTL: 5 * time.Second, DialTimeout: time.Second}},
	}).Services(registrations...).Build()
	if err != nil {
		return "", nil, nil, nil, err
	}
	stop := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Stop(ctx)
	}
	if err := server.Start(context.Background(), bee.RunOptions{Services: config.Mode, RPCListen: "127.0.0.1:0"}); err != nil {
		_ = stop()
		return "", nil, nil, nil, err
	}
	dispatch, err := agent.NewActorDispatcher(agent.ActorDispatcherOptions{Caller: runtime.CModActorDirect(), CallTimeout: time.Second,
		Resolver: agent.ActorDispatchResolverFunc(func(_ context.Context, request agent.DispatchRequest) (agent.ActorDispatch, error) {
			return agent.ActorDispatch{Target: actor.Identity{Kind: "game", Key: strconv.FormatInt(request.Identity.RoleID, 10)}, Delivery: actor.Delivery{MessageType: "game.up", CommandID: uint64(request.CommandID), RouteKey: request.RouteKey, Payload: request.Payload}}, nil
		}),
	})
	if err != nil {
		_ = stop()
		return "", nil, nil, nil, err
	}
	cross := func(ctx context.Context, d legacy.Delivery) (legacy.Result, error) {
		key, _ := d.RouteKey.Int64()
		result, err := runtime.CModActor().Call(ctx, actor.Identity{Kind: "world", Key: strconv.FormatInt(key, 10)}, actor.Delivery{MessageType: d.MessageType, CommandID: d.CommandID, RouteKey: d.RouteKey, Payload: d.Payload})
		return legacy.Result{MessageType: result.MessageType, Payload: result.Payload}, err
	}
	return runtime.Instance().Endpoint, dispatch, cross, stop, nil
}
