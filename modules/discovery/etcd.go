package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"

	txetcd "github.com/ngnl47/tx/etcd"
)

var (
	ErrEtcdClientNil    = errors.New("discovery: etcd client is nil")
	ErrEtcdWatchClosed  = errors.New("discovery: etcd watch closed")
	ErrEtcdLeaseLost    = errors.New("discovery: etcd lease lost")
	ErrRegistrationDone = errors.New("discovery: registration is closed")
)

type EtcdRegistry struct {
	client *txetcd.Client
	config EtcdConfig
}

func NewEtcdRegistry(ctx context.Context, config EtcdConfig) (*EtcdRegistry, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	client, err := txetcd.New(ctx, config.clientConfig())
	if err != nil {
		return nil, err
	}
	registry, err := NewEtcdRegistryWithClient(client, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return registry, nil
}

func NewEtcdRegistryWithClient(client *txetcd.Client, config EtcdConfig) (*EtcdRegistry, error) {
	if client == nil {
		return nil, ErrEtcdClientNil
	}
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	return &EtcdRegistry{client: client, config: config}, nil
}

func (r *EtcdRegistry) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

func (r *EtcdRegistry) Register(ctx context.Context, instance Instance) (*EtcdRegistration, error) {
	if err := r.config.ValidateInstance(instance); err != nil {
		return nil, err
	}
	lease, err := r.client.Grant(ctx, r.config.LeaseTTL)
	if err != nil {
		return nil, err
	}
	registration := &EtcdRegistration{
		registry: r,
		instance: instance,
		lease:    lease,
		done:     make(chan struct{}),
	}
	if err := registration.write(ctx); err != nil {
		revokeErr := lease.Revoke(context.WithoutCancel(ctx))
		return nil, errors.Join(err, revokeErr)
	}
	go registration.keepAlive()
	return registration, nil
}

// Watch 持续用 etcd 快照更新目录，直到 ctx 取消或 etcd 返回错误。
func (r *EtcdRegistry) Watch(ctx context.Context, directory *ServiceDirectory) error {
	if directory == nil {
		return ErrInvalidInstance
	}
	prefix := r.watchPrefix()
	snapshot, err := r.client.Snapshot(ctx, prefix)
	if err != nil {
		return err
	}
	instances, err := r.decodeSnapshot(snapshot.Values)
	if err != nil {
		return err
	}
	if err := directory.Replace(instances); err != nil {
		return err
	}
	current := make(map[string]Instance, len(instances))
	for _, instance := range instances {
		current[instance.ID] = instance
	}
	watch := r.client.WatchPrefix(ctx, prefix, snapshot.Revision+1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, ok := <-watch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrEtcdWatchClosed
			}
			if response.Err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return mapEtcdWatchError(response.Err)
			}
			for _, event := range response.Events {
				switch event.Type {
				case txetcd.EventPut:
					instance, err := r.decodeInstance(event.Value)
					if err != nil {
						return err
					}
					current[instance.ID] = instance
				case txetcd.EventDelete:
					delete(current, strings.TrimPrefix(event.Value.Key, prefix))
				default:
					return ErrEtcdWatchClosed
				}
			}
			instances = instances[:0]
			for _, instance := range current {
				instances = append(instances, instance)
			}
			if err := directory.Replace(instances); err != nil {
				return err
			}
		}
	}
}

type EtcdRegistration struct {
	registry *EtcdRegistry

	mu       sync.Mutex
	instance Instance
	lease    *txetcd.Lease
	done     chan struct{}
	doneOnce sync.Once
	doneErr  error
	close    sync.Once
	closeErr error
}

func (r *EtcdRegistration) Done() <-chan struct{} {
	return r.done
}

func (r *EtcdRegistration) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.doneErr
}

func (r *EtcdRegistration) SetStatus(ctx context.Context, status Status) error {
	if !status.Valid() {
		return ErrInvalidInstance
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.doneErr != nil {
		return ErrRegistrationDone
	}
	r.instance.Status = status
	return r.write(ctx)
}

func (r *EtcdRegistration) Close(ctx context.Context) error {
	r.close.Do(func() {
		r.finish(ErrRegistrationDone)
		deleteErr := r.registry.client.Delete(ctx, r.registry.instanceKey(r.instance.ID))
		revokeErr := r.lease.Revoke(ctx)
		r.closeErr = errors.Join(deleteErr, revokeErr)
	})
	return r.closeErr
}

func (r *EtcdRegistration) keepAlive() {
	<-r.lease.Done()
	if errors.Is(r.lease.Err(), txetcd.ErrLeaseLost) {
		r.finish(ErrEtcdLeaseLost)
		return
	}
	r.finish(ErrRegistrationDone)
}

func (r *EtcdRegistration) finish(err error) {
	r.doneOnce.Do(func() {
		r.mu.Lock()
		r.doneErr = err
		r.mu.Unlock()
		close(r.done)
	})
}

func (r *EtcdRegistration) write(ctx context.Context) error {
	payload, err := json.Marshal(r.instance)
	if err != nil {
		return err
	}
	return r.lease.Put(ctx, r.registry.instanceKey(r.instance.ID), payload)
}

func (r *EtcdRegistry) instanceKey(id string) string {
	return r.config.Prefix + "/" + id
}

func (r *EtcdRegistry) watchPrefix() string {
	return r.config.Prefix + "/"
}

func (r *EtcdRegistry) decodeSnapshot(values []txetcd.KeyValue) ([]Instance, error) {
	instances := make([]Instance, 0, len(values))
	for _, value := range values {
		instance, err := r.decodeInstance(value)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].ID < instances[j].ID
	})
	return instances, nil
}

func (r *EtcdRegistry) decodeInstance(value txetcd.KeyValue) (Instance, error) {
	var instance Instance
	if json.Unmarshal(value.Value, &instance) != nil {
		return Instance{}, ErrInvalidInstance
	}
	if err := r.config.ValidateInstance(instance); err != nil {
		return Instance{}, err
	}
	if r.instanceKey(instance.ID) != value.Key {
		return Instance{}, ErrInvalidInstance
	}
	return instance, nil
}

func mapEtcdWatchError(err error) error {
	if errors.Is(err, txetcd.ErrWatchClosed) {
		return ErrEtcdWatchClosed
	}
	return err
}
