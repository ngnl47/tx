// Package agent 将 Core 玩家入口服务注册到 Ant 框架门面。
package agent

import (
	"fmt"

	"github.com/ngnl47/tx/ant"
	agentservice "github.com/ngnl47/tx/modules/agent/service"
)

// Options 是标准玩家入口的强类型配置。
type Options = agentservice.Options

// LocalMsgQ 提供同进程 msgq 的会话客户端。
type LocalMsgQ = agentservice.LocalMsgQ

// OptionsProvider 在 Agent 服务构造边界创建项目身份解析器、分发器和监听配置。
type OptionsProvider func(ant.Context) (Options, error)

// Registration 使用项目指定的名称创建标准 Ant Agent 服务注册。
func Registration(name string, provider OptionsProvider) (ant.ServiceRegistration, error) {
	if provider == nil {
		return ant.ServiceRegistration{}, fmt.Errorf("ant agent: options provider is nil")
	}
	return ant.ServiceRegistration{
		Spec: ant.ServiceSpec{
			Name: name,
			Requires: []ant.CModID{
				ant.CModRPC,
				ant.CModRedisStorage,
				ant.CModDiscovery,
				ant.CModActor,
			},
		},
		Factory: func(runtime ant.Context) (ant.Service, error) {
			options, err := provider(runtime)
			if err != nil {
				return nil, fmt.Errorf("ant agent: configure: %w", err)
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
