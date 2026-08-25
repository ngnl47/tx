package actor

import (
	"context"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/extensions"

	txlifecycle "github.com/ngnl47/tx/lifecycle"
)

var actorLifecycleExtensionID = extensions.NextExtensionID()

type mailboxLifecycle struct {
	ingress *txlifecycle.Ingress
}

func (lifecycle *mailboxLifecycle) bind(root *txlifecycle.WaitGroupContext) {
	if lifecycle == nil {
		return
	}
	lifecycle.ingress = txlifecycle.NewIngress(root)
}

func (lifecycle *mailboxLifecycle) begin() error {
	if lifecycle == nil || lifecycle.ingress == nil {
		return ErrLifecycleContextRequired
	}
	return lifecycle.ingress.BeginWork()
}

func (lifecycle *mailboxLifecycle) end() {
	if lifecycle == nil || lifecycle.ingress == nil {
		return
	}
	lifecycle.ingress.EndWork()
}

func (lifecycle *mailboxLifecycle) beginDrain() {
	if lifecycle == nil || lifecycle.ingress == nil {
		return
	}
	lifecycle.ingress.BeginDrain()
}

func (lifecycle *mailboxLifecycle) waitDrained(ctx context.Context) error {
	if lifecycle == nil || lifecycle.ingress == nil {
		return ErrLifecycleContextRequired
	}
	return lifecycle.ingress.WaitContext(ctx)
}

func (lifecycle *mailboxLifecycle) pending() int {
	if lifecycle == nil || lifecycle.ingress == nil {
		return 0
	}
	return lifecycle.ingress.Pending()
}

type actorLifecycleExtension struct {
	context context.Context
}

func (*actorLifecycleExtension) ExtensionID() extensions.ExtensionID {
	return actorLifecycleExtensionID
}

type actorSystemContext interface {
	ActorSystem() *protoactor.ActorSystem
}

// LifecycleContext 返回 Actor Runtime 启动时绑定的只读标准上下文。业务可以用它
// 取消下游 I/O，但无法开始关闭或等待全局生命周期。
func LifecycleContext(actorContext actorSystemContext) context.Context {
	if actorContext == nil {
		return context.Background()
	}
	system := actorContext.ActorSystem()
	if system == nil || system.Extensions == nil {
		return context.Background()
	}
	extension, _ := system.Extensions.Get(actorLifecycleExtensionID).(*actorLifecycleExtension)
	if extension == nil || extension.context == nil {
		return context.Background()
	}
	return extension.context
}

func actorLifecycleRoot(ctx context.Context) (*txlifecycle.WaitGroupContext, bool) {
	switch value := ctx.(type) {
	case *txlifecycle.WaitGroupContext:
		return value, value != nil
	case *txlifecycle.ChildWaitGroupContext:
		if value == nil {
			return nil, false
		}
		return value.WaitGroupContext, value.WaitGroupContext != nil
	default:
		return nil, false
	}
}
