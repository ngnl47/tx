// Package discovery 提供服务实例发现基础能力。
package discovery

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

var ErrInvalidInstance = errors.New("discovery: invalid instance")

type Status string

const (
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusDraining Status = "draining"
	StatusStopping Status = "stopping"
)

type Instance struct {
	ID            string   `json:"id"`
	Services      []string `json:"services"`
	Endpoint      string   `json:"endpoint"`
	ActorEndpoint string   `json:"actorEndpoint,omitempty"`
	Version       string   `json:"version,omitempty"`
	Status        Status   `json:"status"`
}

func (i Instance) Validate() error {
	if i.ID == "" || i.ID != strings.TrimSpace(i.ID) ||
		i.Endpoint == "" || i.Endpoint != strings.TrimSpace(i.Endpoint) ||
		i.ActorEndpoint != strings.TrimSpace(i.ActorEndpoint) ||
		!i.Status.Valid() {
		return ErrInvalidInstance
	}
	if len(i.Services) == 0 {
		return ErrInvalidInstance
	}
	seen := make(map[string]struct{}, len(i.Services))
	for _, service := range i.Services {
		if service == "" || service != strings.TrimSpace(service) {
			return ErrInvalidInstance
		}
		if _, exists := seen[service]; exists {
			return ErrInvalidInstance
		}
		seen[service] = struct{}{}
	}
	return nil
}

func (s Status) Valid() bool {
	return s == StatusStarting || s == StatusRunning || s == StatusDraining || s == StatusStopping
}

type ServiceDirectory struct {
	mu        sync.Mutex
	byID      map[string]Instance
	byService map[string][]Instance
	next      map[string]uint64
}

// Directory 是路由器和应用模块使用的只读服务发现契约，快照替换仍由 discovery runtime 负责。
type Directory interface {
	NextInstance(service string) (Instance, bool)
	RunningInstances(service string) []Instance
	LookupInstance(id string) (Instance, bool)
}

var _ Directory = (*ServiceDirectory)(nil)

func NewServiceDirectory() *ServiceDirectory {
	return &ServiceDirectory{
		byID:      make(map[string]Instance),
		byService: make(map[string][]Instance),
		next:      make(map[string]uint64),
	}
}

// Replace 应用一份完整的发现快照。
func (d *ServiceDirectory) Replace(instances []Instance) error {
	byID := make(map[string]Instance, len(instances))
	byService := make(map[string][]Instance)
	for _, instance := range instances {
		if err := instance.Validate(); err != nil {
			return err
		}
		if _, exists := byID[instance.ID]; exists {
			return ErrInvalidInstance
		}
		instance.Services = append([]string(nil), instance.Services...)
		byID[instance.ID] = instance
		for _, service := range instance.Services {
			byService[service] = append(byService[service], instance)
		}
	}
	for service := range byService {
		sort.Slice(byService[service], func(i, j int) bool {
			return byService[service][i].ID < byService[service][j].ID
		})
	}

	d.mu.Lock()
	d.byID = byID
	d.byService = byService
	for service := range d.next {
		if _, exists := byService[service]; !exists {
			delete(d.next, service)
		}
	}
	d.mu.Unlock()
	return nil
}

// NextInstance 按轮转策略返回下一个 running 状态的服务实例。
func (d *ServiceDirectory) NextInstance(service string) (Instance, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	instances := d.byService[service]
	if len(instances) == 0 {
		return Instance{}, false
	}
	for offset := range len(instances) {
		index := int(d.next[service] % uint64(len(instances)))
		d.next[service]++
		instance := instances[index]
		if instance.Status == StatusRunning {
			return cloneInstance(instance), true
		}
		_ = offset
	}
	return Instance{}, false
}

// RunningInstances 返回指定服务的全部 running 实例快照，保持实例 ID 的稳定排序。
// 调用方可据此实现业务自己的随机、哈希或负载策略；目录不承担实体归属决策。
func (d *ServiceDirectory) RunningInstances(service string) []Instance {
	d.mu.Lock()
	defer d.mu.Unlock()
	instances := d.byService[service]
	result := make([]Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.Status == StatusRunning {
			result = append(result, cloneInstance(instance))
		}
	}
	return result
}

// LookupInstance 为精确路由返回已知的 running、draining 或 stopping 状态实例。
func (d *ServiceDirectory) LookupInstance(id string) (Instance, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	instance, ok := d.byID[id]
	if !ok || (instance.Status != StatusRunning && instance.Status != StatusDraining && instance.Status != StatusStopping) {
		return Instance{}, false
	}
	return cloneInstance(instance), true
}

func cloneInstance(instance Instance) Instance {
	instance.Services = append([]string(nil), instance.Services...)
	return instance
}
