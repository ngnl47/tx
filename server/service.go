package server

import (
	"context"
	"fmt"
	"strings"
)

// Capabilities declares framework resources required by a service.
type Capabilities uint32

const (
	CapabilityStorage Capabilities = 1 << iota
	CapabilityRPC
	CapabilityDiscovery
	CapabilityActor
	CapabilityTransaction
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

// Spec is the stable identity and runtime contract of one selectable service.
type Spec struct {
	Name     string
	Requires Capabilities
}

// Service is one independently selectable process service.
type Service interface {
	Spec() Spec
	Start(Context) error
}

// Drainer stops a service from accepting new external work.
type Drainer interface {
	Drain(context.Context) error
}

// Stopper releases service-owned resources after drain completes.
type Stopper interface {
	Stop(context.Context) error
}
