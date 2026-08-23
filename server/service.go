package server

import (
	"context"
	"fmt"
	"strings"
)

// Capabilities 声明服务需要的框架资源。
type Capabilities uint32

const (
	CapabilityStorage Capabilities = 1 << iota
	CapabilityRPC
	CapabilityDiscovery
	CapabilityActor
	CapabilityTransaction
	CapabilityWAL
)

func (capabilities Capabilities) String() string {
	if capabilities == 0 {
		return "none"
	}
	known := []struct {
		value Capabilities
		name  string
	}{
		{CapabilityStorage, "storage"},
		{CapabilityWAL, "wal"},
		{CapabilityRPC, "rpc"},
		{CapabilityDiscovery, "discovery"},
		{CapabilityActor, "actor"},
		{CapabilityTransaction, "transaction"},
	}
	names := make([]string, 0, len(known)+1)
	remaining := capabilities
	for _, capability := range known {
		if remaining&capability.value == 0 {
			continue
		}
		names = append(names, capability.name)
		remaining &^= capability.value
	}
	if remaining != 0 {
		names = append(names, fmt.Sprintf("unknown(%d)", remaining))
	}
	return strings.Join(names, ",")
}

// Spec 是可选服务的稳定身份和 runtime 契约。
type Spec struct {
	Name     string
	Requires Capabilities
}

// Service 表示一个可独立选择的进程服务。
type Service interface {
	Spec() Spec
	Start(Context) error
}

// Drainer 让服务停止接受新的外部工作。
type Drainer interface {
	Drain(context.Context) error
}

// Stopper 在排水完成后释放服务持有的资源。
type Stopper interface {
	Stop(context.Context) error
}
