package goakteval

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	txlog "github.com/ngnl47/tx/log"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"
)

var ErrPendingFull = errors.New("goakt evaluation: pending capacity full")
var ErrInvalidResponse = errors.New("goakt evaluation: invalid response")

type CallError struct {
	Code    int32
	Message string
}

func (e *CallError) Error() string    { return strconv.FormatInt(int64(e.Code), 10) + ": " + e.Message }
func (e *CallError) ErrorCode() int32 { return e.Code }

// Future 只有单次调用的完成状态，没有全局关联表。关闭 done 后内容只读。
type Future struct {
	done   chan struct{}
	result *actorpb.Result
	err    error
}

func (f *Future) Await(ctx context.Context) (*actorpb.Result, error) {
	if _, err := deadline(ctx); err != nil {
		return nil, err
	}
	select {
	case <-f.done:
		if f.err != nil || f.result == nil {
			return nil, f.err
		}
		return proto.Clone(f.result).(*actorpb.Result), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Runtime) Tell(message *actorpb.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), actor.DefaultGrainRequestTimeout)
	_, err := r.submit(ctx, message, actorpb.DeliveryMode_DELIVERY_MODE_TELL, cancel)
	if err != nil {
		cancel()
	}
	return err
}
func (r *Runtime) TellAccepted(ctx context.Context, message *actorpb.Delivery) error {
	f, err := r.submit(ctx, message, actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED, nil)
	if err != nil {
		return err
	}
	_, err = f.Await(ctx)
	return err
}
func (r *Runtime) CallFuture(ctx context.Context, message *actorpb.Delivery) (*Future, error) {
	return r.submit(ctx, message, actorpb.DeliveryMode_DELIVERY_MODE_CALL, nil)
}

// 每个未结束的原生 Ask 占一个容量名额和一个等待协程；满时立即拒绝，不创建任务。
func (r *Runtime) submit(ctx context.Context, message *actorpb.Delivery, mode actorpb.DeliveryMode, finish func()) (*Future, error) {
	until, err := deadline(ctx)
	if err != nil {
		return nil, err
	}
	if message == nil || message.RouteKeyKind != actorpb.RouteKeyKind_ROUTE_KEY_INT64 || int64(message.RouteKeyNumeric) <= 0 {
		return nil, errors.New("positive role route required")
	}
	if err = r.ingress.BeginWork(); err != nil {
		return nil, err
	}
	select {
	case r.slots <- struct{}{}:
	default:
		r.ingress.EndWork()
		return nil, ErrPendingFull
	}
	copied := proto.Clone(message).(*actorpb.Delivery)
	copied.Mode = mode
	copied.RequestId = 0 // 关联由 GoAkt 自己承担，不运行旧 request_id 关联协议。
	f := &Future{done: make(chan struct{})}
	go func() {
		if finish != nil {
			defer finish()
		}
		id, err := actor.GrainOf[*roleGrain](ctx, r.system, strconv.FormatInt(int64(copied.RouteKeyNumeric), 10),
			actor.WithGrainMailboxCapacity(r.capacity), actor.WithGrainDeactivateAfter(r.idle))
		if err == nil {
			var response any
			response, err = r.system.AskGrain(ctx, id, copied, time.Until(until))
			if err == nil {
				if mode == actorpb.DeliveryMode_DELIVERY_MODE_CALL {
					f.result, err = decodeResult(response)
				} else {
					ack, ok := response.(*actorpb.Acceptance)
					if !ok || ack.CommandId != copied.CommandId {
						err = ErrInvalidResponse
					}
				}
			}
		}
		f.err = err
		if mode == actorpb.DeliveryMode_DELIVERY_MODE_TELL && err != nil {
			reportDeliveryError(copied, err)
		}
		<-r.slots
		r.ingress.EndWork()
		close(f.done)
	}()
	return f, nil
}

func deadline(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	until, ok := ctx.Deadline()
	if !ok {
		return time.Time{}, errors.New("request deadline required")
	}
	return until, nil
}
func errorResult(err error) *actorpb.Result {
	code := int32(-1)
	var coded interface{ ErrorCode() int32 }
	if errors.As(err, &coded) && coded.ErrorCode() != 0 {
		code = coded.ErrorCode()
	}
	return &actorpb.Result{ErrorCode: code, ErrorMessage: err.Error()}
}
func decodeResult(response any) (*actorpb.Result, error) {
	result, ok := response.(*actorpb.Result)
	if !ok || result == nil {
		return nil, ErrInvalidResponse
	}
	if result.ErrorCode != 0 {
		return nil, &CallError{Code: result.ErrorCode, Message: result.ErrorMessage}
	}
	if result.ErrorMessage != "" || result.ProcessedUpSeq < 0 {
		return nil, ErrInvalidResponse
	}
	if result.ProcessedUpSeq > 0 {
		if result.MessageType != "" || result.CommandId != 0 || len(result.Payload) != 0 {
			return nil, ErrInvalidResponse
		}
	} else if result.MessageType == "" || strings.TrimSpace(result.MessageType) != result.MessageType {
		return nil, ErrInvalidResponse
	}
	return result, nil
}
func reportDeliveryError(message *actorpb.Delivery, err error) {
	txlog.Error("goakt evaluation delivery failed", "roleID", message.RouteKeyNumeric, "commandID", message.CommandId, "err", err)
}
