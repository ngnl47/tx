package framework

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorplacement"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type placementKey struct{ service, policy string }

// actorPlacement 由进程持有；本地提供方在外部入口启动前一次初始化。
// Identity→SlotID 是有界静态缓存，SlotID→实例仍由 Discovery 权威归属维护。
type actorPlacement struct {
	actorpb.UnimplementedPlacementQueryServer
	ready     atomic.Bool
	closeOnce sync.Once
	closed    chan struct{}
	catalog   map[string]actorTypeSpec
	providers map[placementKey]txactor.GroupResolver
	clients   map[string]actorpb.PlacementQueryClient
	cache     *actorplacement.Cache
	tasks     *lifecycle.ChildWaitGroupContext
	ingress   *lifecycle.Ingress
}

func newActorPlacement(r *runtime) (*actorPlacement, error) {
	grouped := false
	for _, spec := range r.actorCatalog {
		if actorhost.PlacementName(spec.actorType) != "" {
			grouped = true
			break
		}
	}
	if !grouped {
		return nil, nil
	}
	tasks := lifecycle.NewChildWaitGroupContext(r.lifecycle)
	p := &actorPlacement{catalog: r.actorCatalog, providers: make(map[placementKey]txactor.GroupResolver), clients: make(map[string]actorpb.PlacementQueryClient), tasks: tasks, ingress: lifecycle.NewIngress(r.lifecycle)}
	cache, err := actorplacement.NewCache(tasks, p.loadSlot)
	if err != nil {
		tasks.Cancel()
		return nil, err
	}
	p.cache = cache
	for _, spec := range r.actorCatalog {
		if actorhost.PlacementName(spec.actorType) != "" {
			p.clients[spec.service] = actorpb.NewPlacementQueryClient(r.rpcSender.Client(rpc.ServiceRoute(spec.service)))
		}
	}
	if err := r.rpc.Register(func(registrar grpc.ServiceRegistrar) error {
		actorpb.RegisterPlacementQueryServer(registrar, p)
		return nil
	}); err != nil {
		_ = p.close(context.Background())
		return nil, err
	}
	return p, nil
}

func (p *actorPlacement) initialize(r *runtime, services []Service) error {
	ctx := newServiceContextWithTasks(r, p.ingress, p.tasks)
	for _, service := range services {
		spec := service.Spec()
		for _, declaration := range spec.Placements {
			resolve, err := declaration.Factory(ctx)
			if err != nil {
				return fmt.Errorf("placement: initialize %s/%s: %w", spec.Name, declaration.Name, err)
			}
			if resolve == nil {
				return fmt.Errorf("placement: nil resolver %s/%s", spec.Name, declaration.Name)
			}
			p.providers[placementKey{spec.Name, declaration.Name}] = resolve
		}
	}
	p.ready.Store(true)
	return nil
}

func (p *actorPlacement) resolve(ctx context.Context, id actorhost.Identity) (string, error) {
	if !p.ready.Load() {
		return "", fmt.Errorf("placement: providers are not ready")
	}
	spec, ok := p.catalog[id.Type]
	if !ok {
		return "", fmt.Errorf("actor: unknown actorType %q", id.Type)
	}
	if actorhost.PlacementName(spec.actorType) == "" {
		return actorhost.SlotFor(ctx, spec.actorType, id)
	}
	return p.cache.Resolve(ctx, id)
}

func (p *actorPlacement) loadSlot(ctx context.Context, id actorhost.Identity) (string, error) {
	spec := p.catalog[id.Type]
	policy := actorhost.PlacementName(spec.actorType)
	var group string
	var err error
	if resolve, local := p.providers[placementKey{spec.service, policy}]; local {
		done, admitErr := p.ingress.Enter()
		if admitErr != nil {
			return "", admitErr
		}
		defer done()
		group, err = resolve(ctx, id)
	} else {
		reply, callErr := p.clients[spec.service].ResolvePlacement(ctx, &actorpb.PlacementRequest{ActorType: id.Type, ActorId: id.ID})
		if callErr != nil {
			return "", callErr
		}
		if reply.GetSlotId() == "" {
			return "", fmt.Errorf("placement: empty slot for %v", id)
		}
		return reply.SlotId, nil
	}
	if err != nil {
		return "", err
	}
	if group == "" {
		return "", fmt.Errorf("placement: empty group for %v", id)
	}
	return actorhost.PlacementSlot(spec.service, policy, group), nil
}

// ResolvePlacement 只接收本地策略的查询，不能继续代理到远端形成寻址环。
func (p *actorPlacement) ResolvePlacement(ctx context.Context, request *actorpb.PlacementRequest) (*actorpb.PlacementResponse, error) {
	if !p.ready.Load() {
		return nil, status.Error(codes.Unavailable, "placement providers are not ready")
	}
	spec, ok := p.catalog[request.GetActorType()]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown Actor type")
	}
	policy := actorhost.PlacementName(spec.actorType)
	if _, ok := p.providers[placementKey{spec.service, policy}]; !ok {
		return nil, status.Error(codes.FailedPrecondition, "placement provider is not local")
	}
	done, err := p.ingress.Enter()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, actorplacement.QueryTimeout)
	defer cancel()
	slot, err := p.resolve(ctx, actorhost.Identity{Type: request.ActorType, ID: request.ActorId})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	// 返回完整槽位以避免再次解释业务分组；调用方只接收目标服务的结果。
	return &actorpb.PlacementResponse{SlotId: slot}, nil
}

// close 的超时只结束等待，任务登记和资源由关闭重试继续持有。
func (p *actorPlacement) close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closed = make(chan struct{})
		p.ingress.BeginDrain()
		p.tasks.Cancel()
		go func() {
			p.tasks.Wait()
			_ = p.ingress.WaitContext(context.Background())
			close(p.closed)
		}()
	})
	select {
	case <-p.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
