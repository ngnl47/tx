package actortrial

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"google.golang.org/protobuf/proto"
)

var ErrPendingFull = errors.New("actortrial: too many pending calls")

// Target 使用启动时唯一实例 ID；路由层负责解析，传输不以 Queue Group 猜测归属。
type Target struct {
	Instance string
	Identity Identity
}

// Transport 是隔离试验适配器。业务载荷复用现行 protobuf，调用方不共享连接所有权。
type Transport struct {
	conn                *nats.Conn
	runtime             *Runtime
	namespace, instance string
	inbox               string
	mu                  sync.Mutex
	nextRequest         uint64
	maxPending          int
	pending             map[string]*pendingCall
}

type pendingCall struct {
	future *Future
	stop   func() bool
}

func Connect(url, namespace, instance string, runtime *Runtime, maxPending int) (*Transport, error) {
	if namespace == "" || instance == "" || runtime == nil || maxPending < 1 {
		panic("actortrial: invalid transport configuration")
	}
	p := &Transport{runtime: runtime, namespace: namespace, instance: instance, inbox: nats.NewInbox() + ".",
		maxPending: maxPending, pending: make(map[string]*pendingCall)}
	// 不在断线期间缓存 RPC；重连恢复的是后续请求，不重发已发请求。
	conn, err := nats.Connect(url, nats.Timeout(time.Second), nats.ReconnectBufSize(-1),
		nats.ClosedHandler(func(*nats.Conn) { p.closeRequests() }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			slog.Error("actortrial NATS asynchronous error", "err", err)
		}))
	if err != nil {
		return nil, err
	}
	p.conn = conn
	if _, err = conn.Subscribe(p.inbox+"*", func(msg *nats.Msg) { p.finishRequest(msg.Subject, msg, nil) }); err == nil {
		_, err = conn.Subscribe(p.subject(instance), p.receive)
	}
	if err == nil {
		err = conn.FlushTimeout(time.Second)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return p, nil
}
func (p *Transport) subject(instance string) string {
	return p.namespace + ".actor." + base64.RawURLEncoding.EncodeToString([]byte(instance))
}

// ResultFuture 的等待取消不撤销业务；远端请求按创建时的 deadline 回收等待资源。
type ResultFuture struct {
	future *Future
	ctx    context.Context
}

func (f *ResultFuture) Await(ctx context.Context) (*actorpb.Result, error) {
	select {
	case <-f.future.done:
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.future.result.err != nil {
		return nil, f.future.result.err
	}
	result, _ := f.future.result.value.(*actorpb.Result)
	return result, resultError(result)
}

// Call 与 CallFuture 使用同一结果协议；本地直达实体队列，不绕过准入。
func (p *Transport) Call(ctx context.Context, target Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	f, err := p.CallFuture(ctx, target, delivery)
	if err != nil {
		return nil, err
	}
	return f.Await(ctx)
}

// CallFuture 立即返回结果句柄；远端等待数有界，本地投递由实体邮箱限流。
func (p *Transport) CallFuture(ctx context.Context, target Target, delivery *actorpb.Delivery) (*ResultFuture, error) {
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	if err := validateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	// 复用现有业务信封的非零 Call 标记；响应关联由 Future/inbox 或 gRPC 请求承担。
	delivery.RequestId = 1
	if target.Instance == p.instance {
		f, err := p.runtime.Submit(target.Identity, delivery)
		if err != nil {
			return nil, err
		}
		return &ResultFuture{f, ctx}, nil
	}
	msg, err := p.message(target, delivery)
	if err != nil {
		return nil, err
	}
	return p.requestFuture(ctx, msg)
}

func (p *Transport) requestFuture(ctx context.Context, msg *nats.Msg) (*ResultFuture, error) {
	p.mu.Lock()
	if p.conn.IsClosed() {
		p.mu.Unlock()
		return nil, nats.ErrConnectionClosed
	}
	if len(p.pending) >= p.maxPending {
		p.mu.Unlock()
		return nil, ErrPendingFull
	}
	p.nextRequest++
	reply := p.inbox + strconv.FormatUint(p.nextRequest, 10)
	f := &Future{done: make(chan struct{})}
	// 取消回调与响应、发送失败共用删除边界；成功后停止回调，不为每个请求保留等待 goroutine。
	p.pending[reply] = &pendingCall{future: f, stop: context.AfterFunc(ctx, func() { p.finishRequest(reply, nil, ctx.Err()) })}
	p.mu.Unlock()
	msg.Reply = reply
	if err := p.conn.PublishMsg(msg); err != nil {
		p.finishRequest(reply, nil, err)
	}
	return &ResultFuture{f, ctx}, nil
}

func (p *Transport) finishRequest(reply string, response *nats.Msg, err error) {
	p.mu.Lock()
	call := p.pending[reply]
	if call != nil {
		delete(p.pending, reply)
		call.stop()
	}
	p.mu.Unlock()
	if call == nil {
		return
	}
	var result actorpb.Result
	if err == nil {
		if len(response.Data) == 0 && response.Header.Get("Status") == "503" {
			err = nats.ErrNoResponders
		} else {
			err = proto.Unmarshal(response.Data, &result)
			if err == nil && result.ErrorCode != 0 {
				err = &RemoteError{Code: result.ErrorCode, Message: result.ErrorMessage, Kind: response.Header.Get("Actor-Error")}
			}
		}
	}
	call.future.finish(&result, err)
}

func (p *Transport) closeRequests() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for reply, call := range p.pending {
		delete(p.pending, reply)
		call.stop()
		call.future.finish(nil, nats.ErrConnectionClosed)
	}
}

// Tell 只投递本地 NATS 客户端，不等待服务端确认。异步投递失败在接收端记录。
func (p *Transport) Tell(ctx context.Context, target Target, delivery *actorpb.Delivery) error {
	return p.publish(ctx, target, delivery, false)
}

// TellAccepted 通过 PING/PONG 确认此前输出到达当前 NATS 服务端，不确认持久化或目标处理。
func (p *Transport) TellAccepted(ctx context.Context, target Target, delivery *actorpb.Delivery) error {
	return p.publish(ctx, target, delivery, true)
}

func (p *Transport) publish(ctx context.Context, target Target, delivery *actorpb.Delivery, confirmed bool) error {
	if delivery == nil {
		return errors.New("actortrial: nil delivery")
	}
	if err := validateRequest(ctx, target, confirmed); err != nil {
		return err
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_TELL
	delivery.RequestId = 0
	msg, err := p.message(target, delivery)
	if err != nil {
		return err
	}
	reconnects := p.conn.Stats().Reconnects
	if err = p.conn.PublishMsg(msg); err != nil || !confirmed {
		return err
	}
	if err = p.conn.FlushWithContext(ctx); err != nil {
		return err
	}
	if p.conn.Stats().Reconnects != reconnects {
		return errors.New("actortrial: confirmation interrupted by reconnect")
	}
	return nil
}

func validateRequest(ctx context.Context, target Target, deadline bool) error {
	if _, ok := ctx.Deadline(); deadline && !ok {
		return errors.New("actortrial: RPC requires deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target.Instance == "" || target.Identity.Kind == "" || target.Identity.Key == "" {
		return errors.New("actortrial: invalid request")
	}
	return nil
}

func (p *Transport) message(target Target, delivery *actorpb.Delivery) (*nats.Msg, error) {
	data, err := proto.Marshal(delivery)
	if err != nil {
		return nil, err
	}
	identity, _ := json.Marshal(target.Identity)
	msg := &nats.Msg{Subject: p.subject(target.Instance), Data: data, Header: nats.Header{}}
	msg.Header.Set("Actor-Identity", base64.RawURLEncoding.EncodeToString(identity))
	return msg, nil
}

// resultError 保持现行上行 ACK 与业务结果互斥，非法结果明确失败而不是等超时。
func resultError(result *actorpb.Result) error {
	if result == nil {
		return errors.New("actortrial: handler returned invalid result")
	}
	if !utf8.ValidString(result.MessageType) || !utf8.ValidString(result.ErrorMessage) {
		return errors.New("actortrial: invalid result text")
	}
	if result.ErrorCode != 0 {
		return &RemoteError{Code: result.ErrorCode, Message: result.ErrorMessage}
	}
	if result.ErrorMessage != "" {
		return errors.New("actortrial: error message without code")
	}
	if result.ProcessedUpSeq > 0 {
		if result.MessageType != "" || result.CommandId != 0 || len(result.Payload) != 0 {
			return errors.New("actortrial: mixed acknowledgement and result")
		}
		return nil
	}
	if result.ProcessedUpSeq < 0 || result.MessageType == "" || result.MessageType != strings.TrimSpace(result.MessageType) {
		return errors.New("actortrial: invalid result")
	}
	return nil
}

// RemoteError 保留业务错误码；框架拒绝仍可用 errors.Is 分类。
type RemoteError struct {
	Code    int32
	Message string
	Kind    string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("actortrial remote (%d): %s", e.Code, e.Message)
}
func (e *RemoteError) ErrorCode() int32 { return e.Code }
func (e *RemoteError) Is(target error) bool {
	switch target {
	case ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity:
		return e.Kind == target.Error()
	}
	return false
}
func remoteError(err error) *RemoteError {
	if previous, ok := err.(*RemoteError); ok {
		copy := *previous
		return &copy
	}
	failure := &RemoteError{Code: -1, Message: err.Error()}
	for _, kind := range []error{ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity} {
		if errors.Is(err, kind) {
			failure.Kind = kind.Error()
			if kind != ErrPanicked {
				failure.Code = -3
			}
			break
		}
	}
	var coded interface{ ErrorCode() int32 }
	if errors.As(err, &coded) {
		failure.Code = coded.ErrorCode()
	}
	return failure
}

func (p *Transport) reply(msg *nats.Msg, result *actorpb.Result, err error) {
	if msg.Reply == "" {
		if err != nil {
			slog.Error("actortrial one-way command failed", "subject", msg.Subject, "err", err)
		}
		return
	}
	response := &nats.Msg{Header: nats.Header{}}
	if err != nil {
		failure := remoteError(err)
		response.Header.Set("Actor-Error", failure.Kind)
		result = &actorpb.Result{ErrorCode: failure.Code, ErrorMessage: failure.Message}
	}
	data, encodeErr := proto.Marshal(result)
	if encodeErr == nil {
		response.Data = data
		encodeErr = msg.RespondMsg(response)
	}
	if encodeErr != nil {
		slog.Error("actortrial RPC response failed", "err", encodeErr)
	}
}
func (p *Transport) receive(msg *nats.Msg) {
	var id Identity
	identity, err := base64.RawURLEncoding.DecodeString(msg.Header.Get("Actor-Identity"))
	if err == nil {
		err = json.Unmarshal(identity, &id)
	}
	if err != nil || id.Kind == "" || id.Key == "" {
		p.reply(msg, nil, errors.New("actortrial: invalid identity"))
		return
	}
	if command := msg.Header.Get("Actor-Control"); command != "" {
		status, err := p.activationControl(id, command)
		data, _ := json.Marshal(status)
		p.reply(msg, &actorpb.Result{MessageType: "actor.status", Payload: data}, err)
		return
	}
	var delivery actorpb.Delivery
	err = proto.Unmarshal(msg.Data, &delivery)
	if err != nil {
		p.reply(msg, nil, errors.New("actortrial: invalid request"))
		return
	}
	_, err = p.runtime.submitRequest(id, &delivery, func(value any, err error) {
		result, _ := value.(*actorpb.Result)
		if err == nil && (msg.Reply != "" || value != nil) {
			err = resultError(result)
		}
		p.reply(msg, result, err)
	})
	if err != nil {
		p.reply(msg, nil, err)
	}
}

// Probe 查询运行时状态，不把探测排在业务邮箱或初始化之后。
func (p *Transport) Probe(ctx context.Context, target Target) (ActivationStatus, error) {
	return p.control(ctx, target, "probe")
}

func (p *Transport) EnsureActivation(ctx context.Context, target Target) (ActivationStatus, error) {
	return p.control(ctx, target, "ensure")
}

func (p *Transport) RetryActivation(ctx context.Context, target Target) (ActivationStatus, error) {
	return p.control(ctx, target, "retry")
}

func (p *Transport) control(ctx context.Context, target Target, command string) (ActivationStatus, error) {
	if err := validateRequest(ctx, target, true); err != nil {
		return ActivationStatus{}, err
	}
	if target.Instance == p.instance {
		return p.activationControl(target.Identity, command)
	}
	identity, _ := json.Marshal(target.Identity)
	msg := &nats.Msg{Subject: p.subject(target.Instance), Header: nats.Header{}}
	msg.Header.Set("Actor-Identity", base64.RawURLEncoding.EncodeToString(identity))
	msg.Header.Set("Actor-Control", command)
	f, err := p.requestFuture(ctx, msg)
	if err != nil {
		return ActivationStatus{}, err
	}
	result, err := f.Await(ctx)
	if err != nil {
		return ActivationStatus{}, err
	}
	var status ActivationStatus
	if result.MessageType != "actor.status" {
		return status, errors.New("actortrial: invalid status response")
	}
	err = json.Unmarshal(result.Payload, &status)
	return status, err
}

func (p *Transport) activationControl(id Identity, command string) (ActivationStatus, error) {
	switch command {
	case "probe":
		return p.runtime.Probe(id), nil
	case "ensure":
		return p.runtime.EnsureActivation(id)
	case "retry":
		return p.runtime.RetryActivation(id)
	default:
		return ActivationStatus{}, errors.New("actortrial: invalid activation control")
	}
}

// Drain 由关闭协调器串行调用。写回失败保持连接，允许在关闭准入的状态重试收尾。
func (p *Transport) Drain(ctx context.Context, flush func(context.Context) error) error {
	if err := p.runtime.Drain(ctx); err != nil {
		return err
	}
	if flush != nil {
		if err := flush(ctx); err != nil {
			return err
		}
	}
	// 此时 worker 已发完结果和卸载前 RPC；Flush 确认输出到达服务端后关闭连接。
	if err := p.conn.FlushWithContext(ctx); err != nil {
		return err
	}
	p.conn.Close()
	return nil
}
