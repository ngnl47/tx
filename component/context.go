// Package component 提供不带业务语义的组件依赖图与生命周期宿主。
package component

import (
	"context"

	"github.com/ngnl47/tx/lifecycle"
)

// Context 向组件提供进程根上下文和当前组件独占的后台任务生命周期。
// 它不提供能力注册表；组件之间的强类型依赖必须在构造时注入。
type Context interface {
	Root() context.Context
	Tasks() lifecycle.Tasks
}

type runtimeContext struct {
	root  context.Context
	tasks lifecycle.Tasks
}

func (runtime runtimeContext) Root() context.Context {
	return runtime.root
}

func (runtime runtimeContext) Tasks() lifecycle.Tasks {
	return runtime.tasks
}
