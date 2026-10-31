package framework

import (
	"context"
	"fmt"
	txruntime "github.com/ngnl47/tx/runtime"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorresult"
	"github.com/ngnl47/tx/internal/actorrpc"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

func (server *Server) actorCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModActor, Requires: []CModID{CModRPC, CModDiscovery}},
		validate: func(CModContext) error {
			config, err := server.config.Actor.Runtime.Normalize()
			if err != nil {
				return err
			}
			if config.Capacity.Scope == txactor.CapacityAssignments && config.Capacity.Maximum > 0 {
				for _, actorType := range server.runtime.actorTypes {
					if !actorhost.HasIndependentSlots(actorType) {
						return fmt.Errorf("actor: assignment capacity requires independent slots for actorType %q", actorType.Name())
					}
				}
			}
			return nil
		},
		start: func(CModContext) error { return server.startActor() },
	}
}
func (server *Server) startActor() error {
	config, err := server.config.Actor.Runtime.Normalize()
	if err != nil {
		return err
	}
	if config.Namespace == "" {
		config.Namespace = server.config.AppID
	}
	r := server.runtime
	r.placement, err = newActorPlacement(r)
	if err != nil {
		return err
	}
	factory, err := actorhost.TypeFactory(r.actorTypes, config.IdlePassivation)
	if err != nil {
		return err
	}
	local := actorhost.New(config.MailboxCapacity, config.CallbackTimeout, func(c actorhost.Context) (*actorhost.Behavior, error) {
		b, err := factory(c)
		if b != nil && b.Receive != nil {
			receive := b.Receive
			b.Receive = func(c actorhost.Context, message any) (any, error) {
				envelope, remote := message.(*actorpb.Delivery)
				if !remote {
					return receive(c, message)
				}
				delivery, err := txactor.DeliveryFromProto(envelope)
				if err != nil {
					return nil, err
				}
				value, err := receive(c, delivery)
				if err != nil {
					return nil, err
				}
				if !delivery.WantsResult() {
					return nil, nil
				}
				result, ok := value.(txactor.Result)
				if !ok {
					return nil, txactor.ErrInvalidResponse
				}
				return actorresult.ToProto(result), nil
			}
		}
		return b, err
	})
	local.BindAdmission(func(id actorhost.Identity, activation bool) error { return r.actorAdmission(id.Type, activation) })
	local.BindContext(func(ctx context.Context, id actorhost.Identity) context.Context {
		owner := r.actorCatalog[id.Type].service
		return txruntime.Bind(ctx, r.serviceAccess[owner])
	})
	local.SetActivationLimits(actorhost.ActivationLimits{Actors: config.Capacity.Maximum, Initializations: config.Capacity.Initializations})
	mapping := func(ctx context.Context, id actorhost.Identity) (string, error) {
		return actorhost.SlotFor(ctx, r.actorCatalog[id.Type].actorType, id)
	}
	if r.placement != nil {
		mapping = r.placement.resolve
	}
	local.BindSlots(r.instanceSnapshot().ID, server.discovery.AuthoritativeSlots(), mapping)
	if err := server.startEntityTransport(local, config.NATSURL, config.Namespace, config.MaxPendingCalls); err != nil {
		return err
	}
	resolver, err := discovery.NewSlotResolver(r.slots, r.directory)
	if err != nil {
		return err
	}
	ownership, err := discovery.NewSlotOwnership(server.discovery.AuthoritativeSlots(), r.directory)
	if err != nil {
		return err
	}
	router := &actorrpc.EntityRouter{Resolver: resolver, Ownership: ownership, Transport: r.actorRPC.(*actorrpc.NATSTransport), SlotID: mapping,
		ExplicitAssignment: func(name string) bool { return r.actorCatalog[name].actorType.RequiresExplicitAssignment() },
		Service: func(name string) (string, error) {
			spec, ok := r.actorCatalog[name]
			if !ok {
				return "", fmt.Errorf("actor: unknown actorType %q", name)
			}
			return spec.service, nil
		}}
	r.actor = actorAccess{messenger: &actorrpc.NATSMessenger{Router: router}, routeTimeout: config.CallbackTimeout}
	r.actorDirect = directActorAccess{caller: &actorrpc.RoutedCaller{Router: router, Transport: &actorrpc.DirectTransport{Sender: r.rpcSender}}}
	return nil
}
