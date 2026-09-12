package actortrial

import (
	"context"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/agent"
)

// NewAgentDispatcher 只把项目命令解析留给组合根；会话注入、deadline 和完成 ACK 由 tx 维护。
// caller 的直连传输在组装时确定，业务不用选择网络路径。
func NewAgentDispatcher(caller *RoutedCaller, resolve func(context.Context, agent.DispatchRequest) (Identity, txactor.Delivery, error), timeout time.Duration) agent.Dispatcher {
	if timeout <= 0 {
		panic("actortrial: invalid agent call timeout")
	}
	return agent.DispatchFunc(func(ctx context.Context, request agent.DispatchRequest) (agent.DispatchResult, error) {
		id, delivery, err := resolve(ctx, request)
		if err != nil {
			return agent.DispatchResult{}, err
		}
		delivery.Identity, delivery.UpSeq = &request.Identity, request.Seq
		message, err := delivery.ToProto()
		if err != nil {
			return agent.DispatchResult{}, err
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		result, err := caller.Call(ctx, id, message)
		if err != nil {
			return agent.DispatchResult{}, err
		}
		if result.ProcessedUpSeq != request.Seq || result.MessageType != "" || result.CommandId != 0 || len(result.Payload) != 0 {
			return agent.DispatchResult{}, agent.ErrInvalidDispatchResult
		}
		return agent.DispatchResult{ProcessedUpSeq: result.ProcessedUpSeq}, nil
	})
}
