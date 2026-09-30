// Package agent 将 Core 玩家入口服务注册到 Bee 框架门面。
package agent

import (
	"fmt"

	"github.com/ngnl47/tx/bee"
	agentservice "github.com/ngnl47/tx/modules/agent/service"
)

// Options 是标准玩家入口的强类型配置。
type Options = agentservice.Options

// LocalMsgQ 提供同进程 msgq 的会话客户端。
type LocalMsgQ = agentservice.LocalMsgQ

// OptionsProvider 在 Agent 服务构造边界创建项目身份解析器、分发器和监听配置。
type OptionsProvider func(bee.Context) (Options, error)

// Registration 使用项目指定的名称创建标准 Bee Agent 服务注册。
func Registration(name string, provider OptionsProvider) (bee.ServiceRegistration, error) {
	if provider == nil {
		return bee.ServiceRegistration{}, fmt.Errorf("bee agent: options provider is nil")
	}
	return bee.ServiceRegistration{
		Spec: bee.ServiceSpec{
			Name: name,
			Requires: []bee.CModID{
				bee.CModRPC,
				bee.CModRedisStorage,
				bee.CModDiscovery,
				bee.CModActor,
			},
		},
		Factory: func(runtime bee.Context) (bee.Service, error) {
			options, err := provider(runtime)
			if err != nil {
				return nil, fmt.Errorf("bee agent: configure: %w", err)
			}
			rpc := runtime.CModRPC()
			redis := runtime.CModRedisStorage()
			directory := runtime.CModDiscovery()
			return agentservice.New(options, agentservice.Dependencies{
				Tasks:     runtime.Tasks(),
				Admission: runtime.Admission(),
				RPC:       rpc,
				Directory: directory,
				Redis:     redis.ScriptRoutes(),
			})
		},
	}, nil
}
