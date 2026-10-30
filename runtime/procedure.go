package runtime

import (
	"context"

	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
)

// Call 使用当前服务的 Procedure Runtime 同步执行根事务及其提交后续过程。
// 事务内嵌套应使用 tx.Call；成功表示内存提交，不等于已最终落盘。
func Call(ctx context.Context, procedure txn.Procedure) error {
	return Procedure(ctx).Call(ctx, procedure)
}

// Submit 独立异步执行 Procedure；只传递服务和缓存能力，不携带请求操作范围。
// 接纳前检查调用上下文，接纳后由 Runtime 施加独立期限；不继承 Handler 返回时的取消。
func Submit(ctx context.Context, procedure txn.Procedure) (*txn.Future, error) {
	if tx, _ := txn.FromContext(ctx); tx != nil {
		return nil, txn.ErrSubmitInTransaction
	}
	provider := require(ctx)
	values := Bind(context.Background(), provider)
	if cache := entitycache.FromContext(ctx); cache != nil {
		values = entitycache.Bind(values, cache)
	}
	return provider.Procedure().Submit(submissionContext{Context: ctx, values: values}, procedure)
}

type submissionContext struct {
	context.Context
	values context.Context
}

func (ctx submissionContext) Value(key any) any { return ctx.values.Value(key) }
