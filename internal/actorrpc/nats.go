package actorrpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"google.golang.org/protobuf/proto"
)

var ErrPendingFull = errors.New("actor rpc: too many pending calls")

// NATSTransport 同实例投递本地邮箱，跨实例通过 Core NATS 转发实体 RPC；与直连入口共用本地宿主。
// 连接由框架组合根持有，不对业务暴露原生 NATS 连接。
type NATSTransport struct {
	conn                *nats.Conn
	runtime             *actorhost.Runtime
	namespace, instance string
	inbox               string
	mu                  sync.Mutex
	nextRequest         uint64
	maxPending          int
	pending             map[string]*pendingCall
}

type pendingCall struct {
	future *actorhost.Future
	stop   func() bool
}

func ConnectNATS(url, namespace, instance string, runtime *actorhost.Runtime, maxPending int) (*NATSTransport, error) {
	if namespace == "" || instance == "" || runtime == nil || maxPending < 1 {
		panic("actor rpc: invalid transport configuration")
	}
	p := &NATSTransport{runtime: runtime, namespace: namespace, instance: instance, inbox: nats.NewInbox() + ".",
		maxPending: maxPending, pending: make(map[string]*pendingCall)}
	// 不在断线期间缓存 RPC；重连恢复的是后续请求，不重发已发请求。
	conn, err := nats.Connect(url, nats.Timeout(time.Second), nats.ReconnectBufSize(-1),
		nats.ClosedHandler(func(*nats.Conn) { p.closeRequests() }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			slog.Error("actor rpc NATS asynchronous error", "err", err)
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
func (p *NATSTransport) subject(instance string) string {
	return p.namespace + ".actor." + base64.RawURLEncoding.EncodeToString([]byte(instance))
}

// ResultFuture 的等待取消不撤销业务；远端请求按创建时的 deadline 回收等待资源。
type ResultFuture struct {
	future *actorhost.Future
	ctx    context.Context
}

func (f *ResultFuture) Await(ctx context.Context) (*actorpb.Result, error) {
	select {
	case <-f.future.Done():
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	value, err := f.future.Await(context.Background())
	if err != nil {
		return nil, err
	}
	result, _ := value.(*actorpb.Result)
	return result, ResultError(result)
}

// Call 与 CallFuture 使用同一结果协议；本地直达实体队列，不绕过准入。
func (p *NATSTransport) Call(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	f, err := p.CallFuture(ctx, target, delivery)
	if err != nil {
		return nil, err
	}
	return f.Await(ctx)
}

// CallFuture 立即返回结果句柄；远端等待数有界，本地投递由实体邮箱限流。
func (p *NATSTransport) CallFuture(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*ResultFuture, error) {
	if delivery == nil {
		return nil, errors.New("actor rpc: nil delivery")
	}
	if err := ValidateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	// 复用现有业务信封的非零 Call 标记；响应关联由 actorhost.Future/inbox 或 gRPC 请求承担。
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

func (p *NATSTransport) requestFuture(ctx context.Context, msg *nats.Msg) (*ResultFuture, error) {
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
	f := actorhost.NewFuture(nil)
	// 取消回调与响应、发送失败共用删除边界；成功后停止回调，不为每个请求保留等待 goroutine。
	p.pending[reply] = &pendingCall{future: f, stop: context.AfterFunc(ctx, func() { p.finishRequest(reply, nil, ctx.Err()) })}
	p.mu.Unlock()
	msg.Reply = reply
	if err := p.conn.PublishMsg(msg); err != nil {
		p.finishRequest(reply, nil, err)
	}
	return &ResultFuture{f, ctx}, nil
}

func (p *NATSTransport) finishRequest(reply string, response *nats.Msg, err error) {
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
				err = &actorhost.RemoteError{Code: result.ErrorCode, Message: result.ErrorMessage, Kind: response.Header.Get("Actor-Error")}
			}
		}
	}
	call.future.Complete(&result, err)
}

func (p *NATSTransport) closeRequests() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for reply, call := range p.pending {
		delete(p.pending, reply)
		call.stop()
		call.future.Complete(nil, nats.ErrConnectionClosed)
	}
}

// Tell 同实例进入本地邮箱，跨实例提交 NATS 客户端；不等待业务执行。
func (p *NATSTransport) Tell(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) error {
	return p.publish(ctx, target, delivery, false)
}

// TellAccepted 同实例确认邮箱接纳，跨实例确认 NATS 到达；不确认业务执行或持久化。
func (p *NATSTransport) TellAccepted(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) error {
	return p.publish(ctx, target, delivery, true)
}

func (p *NATSTransport) publish(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery, confirmed bool) error {
	if delivery == nil {
		return errors.New("actor rpc: nil delivery")
	}
	if err := ValidateRequest(ctx, target, confirmed); err != nil {
		return err
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_TELL
	delivery.RequestId = 0
	if target.Instance == p.instance {
		_, err := p.runtime.SubmitWithReply(target.Identity, delivery, func(value any, err error) {
			if err == nil && value != nil {
				result, _ := value.(*actorpb.Result)
				err = ResultError(result)
			}
			if err != nil {
				slog.Error("actor rpc one-way command failed", "actorType", target.Identity.Type, "actorID", target.Identity.ID, "err", err)
			}
		})
		return err
	}
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
		return errors.New("actor rpc: confirmation interrupted by reconnect")
	}
	return nil
}

func (p *NATSTransport) message(target actorhost.Target, delivery *actorpb.Delivery) (*nats.Msg, error) {
	data, err := proto.Marshal(delivery)
	if err != nil {
		return nil, err
	}
	identity, _ := json.Marshal(target.Identity)
	msg := &nats.Msg{Subject: p.subject(target.Instance), Data: data, Header: nats.Header{}}
	msg.Header.Set("Actor-Identity", base64.RawURLEncoding.EncodeToString(identity))
	return msg, nil
}

func (p *NATSTransport) reply(msg *nats.Msg, result *actorpb.Result, err error) {
	if msg.Reply == "" {
		if err != nil {
			slog.Error("actor rpc one-way command failed", "subject", msg.Subject, "err", err)
		}
		return
	}
	response := &nats.Msg{Header: nats.Header{}}
	if err != nil {
		failure := actorhost.RemoteErrorFrom(err)
		response.Header.Set("Actor-Error", failure.Kind)
		result = &actorpb.Result{ErrorCode: failure.Code, ErrorMessage: failure.Message}
	}
	data, encodeErr := proto.Marshal(result)
	if encodeErr == nil {
		response.Data = data
		encodeErr = msg.RespondMsg(response)
	}
	if encodeErr != nil {
		slog.Error("actor rpc RPC response failed", "err", encodeErr)
	}
}
func (p *NATSTransport) receive(msg *nats.Msg) {
	var id actorhost.Identity
	identity, err := base64.RawURLEncoding.DecodeString(msg.Header.Get("Actor-Identity"))
	if err == nil {
		err = json.Unmarshal(identity, &id)
	}
	if err != nil || id.Type == "" || id.ID == "" {
		p.reply(msg, nil, errors.New("actor rpc: invalid identity"))
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
		p.reply(msg, nil, errors.New("actor rpc: invalid request"))
		return
	}
	_, err = p.runtime.SubmitWithReply(id, &delivery, func(value any, err error) {
		result, _ := value.(*actorpb.Result)
		if err == nil && (msg.Reply != "" || value != nil) {
			err = ResultError(result)
		}
		p.reply(msg, result, err)
	})
	if err != nil {
		p.reply(msg, nil, err)
	}
}

// Probe 查询运行时状态，不把探测排在业务邮箱或初始化之后。
func (p *NATSTransport) Probe(ctx context.Context, target actorhost.Target) (actorhost.ActivationStatus, error) {
	return p.control(ctx, target, "probe")
}

func (p *NATSTransport) EnsureActivation(ctx context.Context, target actorhost.Target) (actorhost.ActivationStatus, error) {
	return p.control(ctx, target, "ensure")
}

func (p *NATSTransport) RetryActivation(ctx context.Context, target actorhost.Target) (actorhost.ActivationStatus, error) {
	return p.control(ctx, target, "retry")
}

// CheckActivationCapacity 仅查询准入快照，不加载实体或预留容量。
// 复用实例控制入口；预检成功后仍须由激活路径原子预留。
func (p *NATSTransport) CheckActivationCapacity(ctx context.Context, target actorhost.Target) error {
	_, err := p.control(ctx, target, "capacity")
	return err
}

func (p *NATSTransport) control(ctx context.Context, target actorhost.Target, command string) (actorhost.ActivationStatus, error) {
	if err := ValidateRequest(ctx, target, true); err != nil {
		return actorhost.ActivationStatus{}, err
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
		return actorhost.ActivationStatus{}, err
	}
	result, err := f.Await(ctx)
	if err != nil {
		return actorhost.ActivationStatus{}, err
	}
	var status actorhost.ActivationStatus
	if result.MessageType != "actor.status" {
		return status, errors.New("actor rpc: invalid status response")
	}
	err = json.Unmarshal(result.Payload, &status)
	return status, err
}

func (p *NATSTransport) activationControl(id actorhost.Identity, command string) (actorhost.ActivationStatus, error) {
	switch command {
	case "capacity":
		return actorhost.ActivationStatus{}, p.runtime.CheckActivationCapacity(id)
	case "probe":
		return p.runtime.Probe(id), nil
	case "ensure":
		return p.runtime.EnsureActivation(id)
	case "retry":
		return p.runtime.RetryActivation(id)
	default:
		return actorhost.ActivationStatus{}, errors.New("actor rpc: invalid activation control")
	}
}

// Flush 在实体排空和必要写回之后确认输出到达 NATS，失败时保留连接供框架重试。
func (p *NATSTransport) Flush(ctx context.Context) error { return p.conn.FlushWithContext(ctx) }

// Close 关闭传输并完成未决调用；不排空宿主或执行写回，这些步骤由框架先行编排。
// 也用于启动失败回滚，不依赖 NATS 异步关闭回调才释放 pending。
func (p *NATSTransport) Close() {
	p.conn.Close()
	p.closeRequests()
}
