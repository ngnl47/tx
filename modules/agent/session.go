package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
	"github.com/ngnl47/tx/modules/msgq"
	txprotocol "github.com/ngnl47/tx/protocol"
)

// Server 运行一条玩家连接对应的逻辑会话。
type Server struct {
	identityResolver IdentityResolver
	dispatcher       Dispatcher
	admission        lifecycle.Admission
	connections      lifecycle.Admission
	sessions         SessionClientRouter
	connectionID     ConnectionIDGenerator
	activeSessions   *sessionRegistry
}

// NewServer 创建传输无关的 Agent Session 运行时。
func NewServer(options ServerOptions) (*Server, error) {
	if options.IdentityResolver == nil {
		return nil, ErrIdentityResolverNil
	}
	if options.Dispatcher == nil {
		return nil, ErrDispatcherNil
	}
	if options.Sessions == nil {
		return nil, ErrSessionRouterNil
	}
	if options.ConnectionID == nil {
		options.ConnectionID = randomConnectionID
	}
	return &Server{
		identityResolver: options.IdentityResolver,
		dispatcher:       options.Dispatcher,
		admission:        options.Admission,
		connections:      options.Connections,
		sessions:         options.Sessions,
		connectionID:     options.ConnectionID,
		activeSessions:   newSessionRegistry(),
	}, nil
}

// Serve 完成一条连接的身份解析、上行 Seq 校验、业务分发与下行发送。
func (s *Server) Serve(ctx context.Context, transport Transport) error {
	if s == nil {
		return ErrServerNil
	}
	if transport == nil {
		return ErrTransportNil
	}
	connectionID, err := s.nextConnectionID()
	if err != nil {
		return err
	}
	return s.serve(ctx, transport, connectionID, nil)
}

type sessionTrace struct {
	roleID    int64
	sessionID int64
}

// serve 编排一条物理连接的验票、接入、收发和收尾。
//
//	读取首帧 → 验票并建立连接身份
//	├─ 携票 Logout：终止逻辑会话 → LoggedOut，不新建邮箱
//	└─ 登录/重连：定位 MsgQ → 登记初始化中的连接 → 等待接入确认
//	             └─ Ready → 并行下行发送与串行上行读取
//	已登记接入的收尾：取消并等待下行工作 → 解绑本地连接
//	                 └─ 已取得邮箱：关闭传输 → 确认远端解除接入
//	所有返回路径：必要时尽力通知 SessionEnded → 确保传输关闭
//
// Ready 必须晚于 MsgQ 接纳；初始化期间的终止由 openSession 协调补偿。
// 普通断线只结束接入，逻辑退出由 Logout 显式触发；取消不能替代关闭确认。
// 传输可能已因断线或撤销关闭，SessionEnded 不承诺送达。
func (s *Server) serve(ctx context.Context, transport Transport, connectionID int64, trace *sessionTrace) (serveErr error) {
	defer func() { _ = transport.Close() }()
	defer func() {
		if errors.Is(serveErr, ErrSessionEnded) || errors.Is(serveErr, msgq.ErrSessionAdmission) || errors.Is(serveErr, msgq.ErrSessionOffline) {
			// 通知只是尽力发送；真正的失效由 MsgQ 许可保证，断流后下一次握手仍会拒绝。
			notice, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = transport.Write(notice, Frame{Kind: FrameSessionEnded})
		}
	}()

	handshake, auth, err := s.authenticate(ctx, transport)
	if err != nil {
		return err
	}
	identity := Identity{SessionPrincipal: auth.SessionPrincipal, ConnectionID: connectionID}
	if err := identity.Validate(); err != nil {
		return ErrInvalidIdentity
	}
	if trace != nil {
		trace.roleID = identity.RoleID
		trace.sessionID = identity.SessionID
	}
	if handshake.Kind == FrameLogout {
		if err := s.sessions.TerminateLogicalSession(ctx, identity.RoleID, identity.SessionID); err != nil {
			return err
		}
		return transport.Write(ctx, Frame{Kind: FrameLoggedOut})
	}
	active, err := s.openSession(ctx, transport, identity, handshake, auth)
	if active != nil {
		defer func() { serveErr = active.close(s.activeSessions, serveErr) }()
	}
	if err != nil {
		return err
	}
	return active.run(ctx, s, handshake)
}

func (s *Server) authenticate(ctx context.Context, transport Transport) (Frame, AuthResult, error) {
	handshake, err := transport.Read(ctx)
	if err != nil {
		return Frame{}, AuthResult{}, err
	}
	if !handshake.validHandshake() && !(handshake.validLogout() && len(handshake.Payload) > 0) {
		return Frame{}, AuthResult{}, ErrHandshakeRequired
	}
	auth, err := s.identityResolver.ResolveIdentity(ctx, AuthRequest{
		Token: cloneBytes(handshake.Payload), Restore: handshake.Restore,
		Ack: handshake.Ack, Logout: handshake.Kind == FrameLogout,
	})
	return handshake, auth, err
}

// openSession 先登记初始化中的连接，再等待 MsgQ 接入确认。
// 返回的连接即使接入失败也由调用方关闭，统一完成初始化补偿后的资源收尾。
func (s *Server) openSession(ctx context.Context, transport Transport, identity Identity, handshake Frame, auth AuthResult) (*activeSession, error) {
	client, err := s.sessions.Acquire(ctx, msgq.AcquireSessionRequest{RoleID: identity.RoleID, SessionID: identity.SessionID, Restore: handshake.Restore})
	if err != nil {
		return nil, err
	}
	header := msgq.SessionHeader{RoleID: identity.RoleID, SessionID: identity.SessionID, ConnectionID: identity.ConnectionID}
	sessionCtx, cancel := context.WithCancel(ctx)
	active := &activeSession{
		identity:  identity,
		header:    header,
		client:    client,
		ctx:       sessionCtx,
		cancel:    cancel,
		transport: transport,
		writer:    sessionFrameWriter{transport: transport},
	}
	s.activeSessions.beginInitialization(active)
	active.mailbox, err = client.OpenMailboxStream(sessionCtx, msgq.InitRequest{
		Header: header, Restore: handshake.Restore, Ack: handshake.Ack, Admission: auth.Admission,
	}, active.disconnect)
	previous, ready, terminateOnInitialization := s.activeSessions.completeInitialization(active, err == nil)
	if terminateOnInitialization {
		terminateErr := terminateActiveSession(active)
		active.completeTermination(terminateErr)
		logInitializationTerminationError(ctx, active, terminateErr)
		if err == nil {
			err = context.Canceled
		}
		return active, errors.Join(err, terminateErr)
	}
	if err != nil {
		return active, err
	}
	if !ready {
		return active, context.Canceled
	}
	txlog.InfoContext(ctx, "agent session established",
		"roleID", identity.RoleID,
		"sessionID", identity.SessionID,
		"connectionID", identity.ConnectionID,
		"restore", handshake.Restore,
		"downAck", handshake.Ack,
	)
	if previous != nil {
		previous.disconnect()
	}
	return active, nil
}

func logInitializationTerminationError(ctx context.Context, session *activeSession, err error) {
	if err == nil || errors.Is(err, msgq.ErrSessionOffline) {
		return
	}
	txlog.ErrorContext(ctx, "agent session initialization termination failed",
		"roleID", session.header.RoleID,
		"sessionID", session.header.SessionID,
		"connectionID", session.header.ConnectionID,
		"err", err,
	)
}

func (s *Server) nextConnectionID() (int64, error) {
	if s == nil {
		return 0, ErrServerNil
	}
	connectionID, err := s.connectionID()
	if err != nil {
		return 0, err
	}
	if connectionID <= 0 {
		return 0, ErrInvalidServer
	}
	return connectionID, nil
}

func (s *Server) dispatchUpMsg(ctx context.Context, mailboxStream msgq.MailboxStreamSession, identity Identity, frame Frame, acceptedUpSeq *int64, confirmedDownSeq *int64) error {
	var done func()
	if s.admission != nil {
		var err error
		done, err = s.admission.Enter()
		if err != nil {
			return err
		}
		defer done()
	}
	*confirmedDownSeq = frame.Ack
	if frame.Seq <= *acceptedUpSeq {
		return mailboxStream.Ack(ctx, frame.Ack)
	}
	started := time.Now()
	err := s.dispatcher.Dispatch(ctx, DispatchRequest{
		Identity:    identity,
		CommandHash: frame.CommandHash,
		RouteKey:    frame.RouteKey,
		Payload:     cloneBytes(frame.Payload),
	})
	txmetrics.RecordAgentUplink(err == nil, time.Since(started))
	if err != nil {
		return err
	}
	*acceptedUpSeq = frame.Seq
	return mailboxStream.Ack(ctx, frame.Ack)
}

const immediateDownBatchLimit = 5

func (s *Server) writeDownMsgs(ctx context.Context, mailboxStream msgq.MailboxStreamSession, writer *sessionFrameWriter, written *atomic.Int64) error {
	for {
		message, err := mailboxStream.Recv()
		if err != nil {
			return err
		}
		var frames [immediateDownBatchLimit]Frame
		frames[0], err = downFrame(message)
		if err != nil {
			return err
		}
		count := 1
		var receiveErr error
		if tcp, ok := writer.transport.(*tcpTransport); ok {
			if available, ok := mailboxStream.(interface {
				TryRecv() (msgq.FetchResult, bool, error)
			}); ok {
				for count < immediateDownBatchLimit {
					next, ready, err := available.TryRecv()
					if err != nil {
						receiveErr = err
						break
					}
					if !ready {
						break
					}
					frames[count], err = downFrame(next)
					if err != nil {
						return err
					}
					count++
				}
			}
			if count > 1 {
				started := time.Now()
				err = writer.writeTCPDownBatch(ctx, tcp, frames[:count], written)
				ended := time.Now()
				txmetrics.RecordAgentDownlink(count, ended.Sub(started), err)
			}
		}
		if count == 1 {
			started := time.Now()
			err = writer.writeDownMsg(ctx, frames[0], written)
			ended := time.Now()
			txmetrics.RecordAgentDownlink(1, ended.Sub(started), err)
		}
		if err != nil {
			return err
		}
		if receiveErr != nil {
			return receiveErr
		}
	}
}

// sessionFrameWriter 在同一发送锁内采样确认号并写帧，避免并发组帧导致确认号回退。
type sessionFrameWriter struct {
	transport Transport

	mu sync.Mutex
}

func (w *sessionFrameWriter) Write(ctx context.Context, frame Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	frame.Ack = 0
	return w.transport.Write(ctx, frame)
}

func (w *sessionFrameWriter) writeDownMsg(ctx context.Context, frame Frame, written *atomic.Int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	frame.Ack = 0
	if err := w.transport.Write(ctx, frame); err != nil {
		return err
	}
	written.Store(frame.Seq)
	return nil
}

func (w *sessionFrameWriter) writeTCPDownBatch(ctx context.Context, transport *tcpTransport, frames []Frame, written *atomic.Int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for index := range frames {
		frames[index].Ack = 0
	}
	if err := transport.writeDownBatch(ctx, frames); err != nil {
		return err
	}
	written.Store(frames[len(frames)-1].Seq)
	return nil
}

func (w *sessionFrameWriter) validateAcknowledgement(ack, confirmed int64, written *atomic.Int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return validateAcknowledgement(ack, confirmed, written.Load())
}

func validateAcknowledgement(ack, confirmed, written int64) error {
	if ack < confirmed || ack > written {
		return ErrInvalidAcknowledgement
	}
	return nil
}

func randomConnectionID() (int64, error) {
	for {
		var raw [8]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		value := int64(binary.BigEndian.Uint64(raw[:]) & math.MaxInt64)
		if value > 0 {
			return value, nil
		}
	}
}

func downFrame(message msgq.FetchResult) (Frame, error) {
	hash, body, err := txprotocol.UnpackCommand(message.Payload)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Kind: FrameDownMsg, Seq: message.Seq, CommandHash: hash, Payload: body}, nil
}

func (s *Server) beginConnection() (func(), error) {
	if s.connections == nil {
		return func() {}, nil
	}
	return s.connections.Enter()
}
