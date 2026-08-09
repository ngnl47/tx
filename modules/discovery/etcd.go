package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrEtcdClientNil    = errors.New("discovery: etcd client is nil")
	ErrEtcdWatchClosed  = errors.New("discovery: etcd watch closed")
	ErrEtcdLeaseLost    = errors.New("discovery: etcd lease lost")
	ErrRegistrationDone = errors.New("discovery: registration is closed")
)

type EtcdRegistry struct {
	client *clientv3.Client
	config EtcdConfig
}

func NewEtcdRegistry(ctx context.Context, config EtcdConfig) (*EtcdRegistry, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})
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

func NewEtcdRegistryWithClient(client *clientv3.Client, config EtcdConfig) (*EtcdRegistry, error) {
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
	lease, err := r.client.Grant(ctx, int64(r.config.LeaseTTL.Seconds()))
	if err != nil {
		return nil, err
	}
	registrationCtx, cancel := context.WithCancel(ctx)
	registration := &EtcdRegistration{
		registry: r,
		instance: instance,
		leaseID:  lease.ID,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	if err := registration.write(ctx); err != nil {
		cancel()
		_, revokeErr := r.client.Revoke(context.WithoutCancel(ctx), lease.ID)
		return nil, errors.Join(err, revokeErr)
	}
	responses, err := r.client.KeepAlive(registrationCtx, lease.ID)
	if err != nil {
		_ = registration.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	go registration.keepAlive(responses)
	return registration, nil
}

// Watch 持续用 etcd 快照更新目录，直到 ctx 取消或 etcd 返回错误。
func (r *EtcdRegistry) Watch(ctx context.Context, directory *ServiceDirectory) error {
	if directory == nil {
		return ErrInvalidInstance
	}
	prefix := r.watchPrefix()
	response, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	instances, err := r.decodeSnapshot(response.Kvs)
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
	watch := r.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(response.Header.Revision+1))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response, ok := <-watch:
			if !ok {
				return ErrEtcdWatchClosed
			}
			if err := response.Err(); err != nil {
				return err
			}
			for _, event := range response.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					instance, err := r.decodeInstance(event.Kv)
					if err != nil {
						return err
					}
					current[instance.ID] = instance
				case clientv3.EventTypeDelete:
					delete(current, strings.TrimPrefix(string(event.Kv.Key), prefix))
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
	leaseID  clientv3.LeaseID
	cancel   context.CancelFunc
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
		_, deleteErr := r.registry.client.Delete(ctx, r.registry.instanceKey(r.instance.ID))
		_, revokeErr := r.registry.client.Revoke(ctx, r.leaseID)
		r.closeErr = errors.Join(deleteErr, revokeErr)
	})
	return r.closeErr
}

func (r *EtcdRegistration) keepAlive(responses <-chan *clientv3.LeaseKeepAliveResponse) {
	for range responses {
	}
	r.finish(ErrEtcdLeaseLost)
}

func (r *EtcdRegistration) finish(err error) {
	r.doneOnce.Do(func() {
		r.mu.Lock()
		r.doneErr = err
		r.mu.Unlock()
		r.cancel()
		close(r.done)
	})
}

func (r *EtcdRegistration) write(ctx context.Context) error {
	payload, err := json.Marshal(r.instance)
	if err != nil {
		return err
	}
	_, err = r.registry.client.Put(ctx, r.registry.instanceKey(r.instance.ID), string(payload), clientv3.WithLease(r.leaseID))
	return err
}

func (r *EtcdRegistry) instanceKey(id string) string {
	return r.config.Prefix + "/" + id
}

func (r *EtcdRegistry) watchPrefix() string {
	return r.config.Prefix + "/"
}

func (r *EtcdRegistry) decodeSnapshot(values []*mvccpb.KeyValue) ([]Instance, error) {
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

func (r *EtcdRegistry) decodeInstance(value *mvccpb.KeyValue) (Instance, error) {
	var instance Instance
	if value == nil || json.Unmarshal(value.Value, &instance) != nil {
		return Instance{}, ErrInvalidInstance
	}
	if err := r.config.ValidateInstance(instance); err != nil {
		return Instance{}, err
	}
	if r.instanceKey(instance.ID) != string(value.Key) {
		return Instance{}, ErrInvalidInstance
	}
	return instance, nil
}
