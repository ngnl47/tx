package actorrpc

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
)

func TestOneWayRouteFeedbackDoesNotReplay(t *testing.T) {
	url := natsURL(t)
	for _, mode := range []string{"draining", "ownership", "business"} {
		for _, direct := range []bool{false, true} {
			name := "nats"
			if direct {
				name = "grpc"
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				ns := "route-feedback-" + name + time.Now().Format("150405.000000000")
				source := peer(t, url, ns, "source", echoRuntime())
				executed := make(chan struct{}, 1)
				r := actorhost.New(4, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
					return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { executed <- struct{}{}; return nil, actorhost.ErrNotOwner }}, nil
				})
				if mode == "ownership" {
					r.BindSlots("target", fixedRouteReader{}, actorhost.SlotMapping(func(actorhost.Identity) string { return "slot" }).Resolver())
				}
				targetPeer := peer(t, url, ns, "target", r)
				if mode == "draining" {
					r.BeginDrain()
				}
				faults := make(chan discovery.SlotAssignment, 4)
				source.BindRouteInvalidator(func(a discovery.SlotAssignment) { faults <- a })
				target := actorhost.Target{Instance: "target", Identity: actorhost.Identity{Type: "world", ID: "1"}, SlotID: "slot", Generation: 1}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if direct {
					host, err := rpc.NewHost("127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					err = host.Register(func(reg grpc.ServiceRegistrar) error {
						receiver := NewDirectReceiver("target", r)
						receiver.RejectRoute = targetPeer.RejectRoute
						return rpc.RegisterDeliveryService(reg, receiver, receiver)
					})
					if err != nil {
						t.Fatal(err)
					}
					startTestHost(t, host)
					members := discovery.NewServiceDirectory()
					routeMembers(t, members, map[string]*rpc.Host{"target": host})
					sender := directSender(t, members)
					sender.SourceInstance = "source"
					sender.InvalidateRoute = source.InvalidateRoute
					if err := sender.Enqueue(ctx, target, &actorpb.Delivery{MessageType: "read"}); err != nil {
						t.Fatal(err)
					}
				} else if err := source.Tell(ctx, target, &actorpb.Delivery{MessageType: "read"}); err != nil {
					t.Fatal(err)
				}
				if mode == "business" {
					select {
					case <-executed:
					case <-ctx.Done():
						t.Fatal("业务未执行")
					}
				} else {
					select {
					case got := <-faults:
						if got != target.Assignment() {
							t.Fatal(got)
						}
					case <-ctx.Done():
						t.Fatal("未收到路由拒绝反馈")
					}
				}
				select {
				case got := <-faults:
					t.Fatalf("未重发请求不应重复通知: %+v", got)
				case <-time.After(20 * time.Millisecond):
				}
			})
		}
	}
}

type fixedRouteReader struct{}

func (fixedRouteReader) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	return discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "other", Generation: 2}, nil
}

func startTestHost(t *testing.T, host *rpc.Host) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	t.Cleanup(func() { _ = host.Stop(context.Background()); root.Cancel(); root.Wait() })
	if err := host.Start(root, nil); err != nil {
		t.Fatal(err)
	}
}
