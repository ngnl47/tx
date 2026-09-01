package actor

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	protoactor "github.com/asynkron/protoactor-go/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

const (
	// CallErrorCodeInternal 表示业务没有提供可公开错误码的内部失败。
	CallErrorCodeInternal int32 = -1
	// CallErrorCodeDispatch 表示 Actor 命令无法通过框架分派契约校验。
	CallErrorCodeDispatch int32 = -2
)

var (
	ErrRequestDeadlineRequired = errors.New("actor: request requires a context deadline")
	ErrPendingCallsFull        = errors.New("actor: pending calls are full")
	ErrInvalidResponse         = errors.New("actor: invalid response")
	ErrResponseNotRequested    = errors.New("actor: delivery does not request a business result")
)

// Result 是 Actor 完成业务处理后返回的不透明结果。普通调用携带消息元数据和
// Payload；Agent 上行处理完成确认只携带 ProcessedUpSeq。
type Result struct {
	MessageType    string
	CommandID      uint64
	Payload        []byte
	ProcessedUpSeq int64
}

// CallError 是远端业务返回的显式错误。
type CallError struct {
	Code    int32
	Message string
}

func (err *CallError) Error() string {
	if err == nil {
		return ""
	}
	if err.Message == "" {
		return strconv.FormatInt(int64(err.Code), 10)
	}
	return strconv.FormatInt(int64(err.Code), 10) + ": " + err.Message
}

// ErrorCode 允许同一个错误在响应边界保留稳定错误码。
func (err *CallError) ErrorCode() int32 {
	if err == nil {
		return 0
	}
	return err.Code
}

// NewCallError 创建可跨 Actor 边界传递的业务错误。
func NewCallError(code int32, cause error) *CallError {
	if code == 0 {
		code = CallErrorCodeInternal
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return &CallError{Code: code, Message: message}
}

// Future 保存一次异步业务调用的本地等待状态。取消 Await 只停止本次等待，
// 不会撤销已经发送到远端 Actor 的业务处理。
type Future struct {
	state *futureState
}

// Await 等待业务结果。每次等待都必须使用带 deadline 的 context。
func (future *Future) Await(ctx context.Context) (Result, error) {
	if err := requireDeadline(ctx); err != nil {
		return Result{}, err
	}
	if future == nil || future.state == nil {
		return Result{}, ErrInvalidResponse
	}
	select {
	case <-future.state.done:
		return future.state.outcome()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

type pendingKind uint8

const (
	pendingAccepted pendingKind = iota + 1
	pendingResult
)

type futureState struct {
	expected  pendingKind
	commandID uint64
	done      chan struct{}

	mu       sync.Mutex
	finished bool
	result   Result
	err      error
	stop     func() bool
}

func (state *futureState) complete(result Result, err error) bool {
	state.mu.Lock()
	if state.finished {
		state.mu.Unlock()
		return false
	}
	state.finished = true
	state.result = result
	state.err = err
	stop := state.stop
	state.stop = nil
	close(state.done)
	state.mu.Unlock()
	if stop != nil {
		stop()
	}
	return true
}

func (state *futureState) setStop(stop func() bool) {
	state.mu.Lock()
	if state.finished {
		state.mu.Unlock()
		stop()
		return
	}
	state.stop = stop
	state.mu.Unlock()
}

func (state *futureState) outcome() (Result, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	result := state.result
	result.Payload = append([]byte(nil), result.Payload...)
	return result, state.err
}

type pendingCalls struct {
	max int
	ids atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]*futureState
}

func newPendingCalls(max int) *pendingCalls {
	return &pendingCalls{max: max, pending: make(map[uint64]*futureState)}
}

func (calls *pendingCalls) add(ctx context.Context, expected pendingKind, commandID uint64) (uint64, *Future, error) {
	if err := requireDeadline(ctx); err != nil {
		return 0, nil, err
	}
	state := &futureState{expected: expected, commandID: commandID, done: make(chan struct{})}
	id := calls.ids.Add(1)
	if id == 0 {
		id = calls.ids.Add(1)
	}
	calls.mu.Lock()
	if len(calls.pending) >= calls.max {
		calls.mu.Unlock()
		return 0, nil, ErrPendingCallsFull
	}
	calls.pending[id] = state
	calls.mu.Unlock()
	state.setStop(context.AfterFunc(ctx, func() {
		calls.fail(id, ctx.Err())
	}))
	return id, &Future{state: state}, nil
}

func (calls *pendingCalls) take(id uint64) (*futureState, bool) {
	calls.mu.Lock()
	state, ok := calls.pending[id]
	if ok {
		delete(calls.pending, id)
	}
	calls.mu.Unlock()
	return state, ok
}

func (calls *pendingCalls) fail(id uint64, err error) {
	state, ok := calls.take(id)
	if !ok {
		return
	}
	if err == nil {
		err = ErrInvalidResponse
	}
	state.complete(Result{}, err)
}

func (calls *pendingCalls) failAll(err error) {
	calls.mu.Lock()
	pending := calls.pending
	calls.pending = make(map[uint64]*futureState)
	calls.mu.Unlock()
	for _, state := range pending {
		state.complete(Result{}, err)
	}
}

func (calls *pendingCalls) accept(message *actorpb.Acceptance) {
	if message == nil {
		return
	}
	state, ok := calls.take(message.GetRequestId())
	if !ok {
		return
	}
	if state.expected != pendingAccepted || state.commandID != message.GetCommandId() {
		state.complete(Result{}, ErrInvalidResponse)
		return
	}
	state.complete(Result{}, nil)
}

func (calls *pendingCalls) resolve(message *actorpb.Result) {
	if message == nil {
		return
	}
	state, ok := calls.take(message.GetRequestId())
	if !ok {
		return
	}
	if state.expected != pendingResult {
		state.complete(Result{}, ErrInvalidResponse)
		return
	}
	result := Result{
		MessageType:    message.GetMessageType(),
		CommandID:      message.GetCommandId(),
		Payload:        append([]byte(nil), message.GetPayload()...),
		ProcessedUpSeq: message.GetProcessedUpSeq(),
	}
	if message.GetErrorCode() == 0 && message.GetErrorMessage() != "" {
		state.complete(Result{}, ErrInvalidResponse)
		return
	}
	if message.GetErrorCode() != 0 {
		state.complete(Result{}, &CallError{Code: message.GetErrorCode(), Message: message.GetErrorMessage()})
		return
	}
	if result.ProcessedUpSeq > 0 {
		if result.MessageType != "" || result.CommandID != 0 || len(result.Payload) != 0 {
			state.complete(Result{}, ErrInvalidResponse)
			return
		}
		state.complete(result, nil)
		return
	}
	if result.ProcessedUpSeq < 0 || result.MessageType == "" || result.MessageType != strings.TrimSpace(result.MessageType) {
		state.complete(Result{}, ErrInvalidResponse)
		return
	}
	state.complete(result, nil)
}

type responseActor struct {
	pending *pendingCalls
}

func (actor responseActor) Receive(ctx protoactor.Context) {
	switch message := ctx.Message().(type) {
	case *actorpb.Acceptance:
		actor.pending.accept(message)
	case *actorpb.Result:
		actor.pending.resolve(message)
	}
}

// Respond 返回当前 Call 的业务结果。Tell 和 TellAccepted 不能调用该方法。
func Respond(ctx protoactor.Context, request Delivery, result Result, resultErr error) error {
	if ctx == nil || ctx.Sender() == nil || !request.WantsResult() || request.requestID == 0 {
		return ErrResponseNotRequested
	}
	message := &actorpb.Result{
		RequestId:      request.requestID,
		MessageType:    result.MessageType,
		CommandId:      result.CommandID,
		Payload:        append([]byte(nil), result.Payload...),
		ProcessedUpSeq: result.ProcessedUpSeq,
	}
	if resultErr != nil {
		message.ErrorCode, message.ErrorMessage = callErrorFields(resultErr)
	}
	ctx.Respond(message)
	return nil
}

func callErrorFields(err error) (int32, string) {
	code := CallErrorCodeInternal
	message := err.Error()
	var callErr *CallError
	if errors.As(err, &callErr) {
		return callErr.Code, callErr.Message
	}
	var coded interface{ ErrorCode() int32 }
	if errors.As(err, &coded) && coded.ErrorCode() != 0 {
		code = coded.ErrorCode()
	}
	return code, message
}

func requireDeadline(ctx context.Context) error {
	if ctx == nil {
		return ErrRequestDeadlineRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrRequestDeadlineRequired
	}
	return nil
}
