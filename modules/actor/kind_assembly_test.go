package actor

import (
	"context"
	"fmt"
	"github.com/ngnl47/tx/lifecycle"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
)

func TestKindDeclarationDefersBackendAssembly(t *testing.T) {
	calls := 0
	kind := MustKind("role", func() protoactor.Actor { calls++; return fixedHostProbeActor{} })
	if kind.cluster != nil {
		t.Fatal("普通 Kind 声明提前创建了集群对象")
	}
	config := Config{MailboxCapacity: 8, IdlePassivation: map[string]time.Duration{"role": time.Hour}}
	for i := 0; i < 2; i++ {
		configured, _, err := configureKinds(config, []Kind{kind}, &mailboxLifecycle{})
		if err != nil {
			t.Fatal(err)
		}
		if len(configured) != 1 || configured[0].Kind != "role" || configured[0].Props == nil {
			t.Fatal("未装配 Kind")
		}
	}
	if calls != 0 {
		t.Fatal("规格与后端装配阶段调用了业务工厂")
	}
}

func TestAdaptedKindPreservesNativeStrategy(t *testing.T) {
	native := cluster.NewKind("native", protoactor.PropsFromProducer(func() protoactor.Actor { return fixedHostProbeActor{} }))
	strategyCalls := 0
	native.StrategyBuilder = func(*cluster.Cluster) cluster.MemberStrategy { strategyCalls++; return nil }
	kind, err := AdaptKind(native)
	if err != nil {
		t.Fatal(err)
	}
	configured, _, err := configureKinds(Config{MailboxCapacity: 8}, []Kind{kind}, &mailboxLifecycle{})
	if err != nil {
		t.Fatal(err)
	}
	if configured[0].StrategyBuilder == nil {
		t.Fatal("原生策略丢失")
	}
	configured[0].StrategyBuilder(nil)
	if strategyCalls != 1 {
		t.Fatal("原生策略未保留")
	}
	if configured[0].Props == native.Props {
		t.Fatal("装配应克隆原生 Props，不得原地改写")
	}
	_, _, err = configureKinds(Config{MailboxCapacity: 8, IdlePassivation: map[string]time.Duration{"native": time.Hour}}, []Kind{kind}, &mailboxLifecycle{})
	if err == nil {
		t.Fatal("原生 Kind 无工厂时不能追加空闲卸载包装")
	}
}

func testKindProps(t testing.TB, kind Kind) *protoactor.Props {
	t.Helper()
	props, err := kind.protoProps(0)
	if err != nil {
		t.Fatal(err)
	}
	return props
}

func TestConfiguredKindInitializesClusterOnce(t *testing.T) {
	for _, adapted := range []bool{false, true} {
		t.Run(fmt.Sprint(adapted), func(t *testing.T) {
			producer := func() protoactor.Actor { return &kindInitProbe{} }
			kind := MustKind("role", producer)
			if adapted {
				var err error
				kind, err = AdaptKind(cluster.NewKind("role", protoactor.PropsFromProducer(producer)))
				if err != nil {
					t.Fatal(err)
				}
			}
			root := lifecycle.NewWaitGroupContext(context.Background())
			defer func() { root.Cancel(); root.Wait() }()
			mailboxes := &mailboxLifecycle{}
			mailboxes.bind(root)
			configured, _, err := configureKinds(Config{MailboxCapacity: 8}, []Kind{kind}, mailboxes)
			if err != nil {
				t.Fatal(err)
			}
			system := protoactor.NewActorSystem()
			// 初始化中间件读取集群扩展；此测试不启动网络或成员发现。
			system.Extensions.Register(&cluster.Cluster{ActorSystem: system})
			defer system.Shutdown()
			pid := system.Root.Spawn(configured[0].Props)
			defer func() { system.Root.StopFuture(pid).Wait() }()
			count, err := system.Root.RequestFuture(pid, "count", time.Second).Result()
			if err != nil || count != 1 {
				t.Fatalf("集群初始化次数: %v, err=%v", count, err)
			}
		})
	}
}

type kindInitProbe struct{ initialized int }

func (a *kindInitProbe) Receive(ctx protoactor.Context) {
	switch ctx.Message().(type) {
	case *protoactor.Started:
		cluster.SetClusterIdentity(ctx, (*cluster.ClusterIdentity)(nil))
	case *cluster.ClusterInit:
		a.initialized++
	case string:
		ctx.Respond(a.initialized)
	}
}
