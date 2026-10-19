package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	txprotocol "github.com/ngnl47/tx/protocol"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
	"github.com/ngnl47/tx/modules/msgq"
)

// Server 运行一条玩家连接对应的逻辑会话。
type Server struct {
	identityResolver IdentityResolver
	dispatcher       Dispatcher
	admission        lifecycle.Admission
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

func (s *Server) serve(ctx context.Context, transport Transport, connectionID int64, trace *sessionTrace) (serveErr error) {
	defer func() { _ = transport.Close() }()

	handshake, err := transport.Read(ctx)
	if err != nil {
		return err
	}
	if !handshake.validHandshake() {
		return ErrHandshakeRequired
	}
	principal, err := s.identityResolver.ResolveIdentity(ctx, AuthRequest{Token: cloneBytes(handshake.Payload), Restore: handshake.Restore, Ack: handshake.Ack})
	if err != nil {
		return err
	}
	identity := Identity{SessionPrincipal: principal, ConnectionID: connectionID}
	if err := identity.Validate(); err != nil {
		return ErrInvalidIdentity
	}
	if trace != nil {
		trace.roleID = identity.RoleID
		trace.sessionID = identity.SessionID
	}
	client, err := s.sessions.Acquire(ctx, msgq.AcquireSessionRequest{RoleID: identity.RoleID, SessionID: identity.SessionID, Restore: handshake.Restore})
	if err != nil {
		return err
	}
	header := msgq.SessionHeader{RoleID: identity.RoleID, SessionID: identity.SessionID, ConnectionID: connectionID}
	sessionCtx, cancel := context.WithCancel(ctx)
	active := &activeSession{
		identity:  identity,
		header:    header,
		client:    client,
		cancel:    cancel,
		transport: transport,
	}
	s.activeSessions.beginInitialization(active)
	previous, ready, terminateOnInitialization, err := s.initializeSession(sessionCtx, active, msgq.InitRequest{
		Header:  header,
		Restore: handshake.Restore,
		Ack:     handshake.Ack,
	})
	if err != nil {
		if terminateOnInitialization {
			terminateErr := terminateActiveSession(active)
			active.completeTermination(terminateErr)
			logInitializationTerminationError(ctx, active, terminateErr)
			cancel()
			return errors.Join(err, terminateErr)
		}
		cancel()
		return err
	}

	if terminateOnInitialization {
		terminateErr := terminateActiveSession(active)
		active.completeTermination(terminateErr)
		logInitializationTerminationError(ctx, active, terminateErr)
		cancel()
		return errors.Join(context.Canceled, terminateErr)
	}
	if !ready {
		cancel()
		return context.Canceled
	}
	txlog.InfoContext(ctx, "agent session established",
		"roleID", identity.RoleID,
		"sessionID", identity.SessionID,
		"connectionID", connectionID,
		"restore", handshake.Restore,
		"downAck", handshake.Ack,
	)
	defer s.activeSessions.unbind(header.SessionID, header.ConnectionID)
	if previous != nil {
		previous.disconnect()
	}
	mailboxStream, err := client.OpenMailboxStream(sessionCtx, header)
	if err != nil {
		cancel()
		return err
	}
	// 邮箱失败必须能中断正在写 Socket 的协程，不能等下一次 Recv 才感知。
	stopMailbox := context.AfterFunc(mailboxStream.Context(), active.disconnect)
	defer stopMailbox()
	defer func() { serveErr = errors.Join(serveErr, mailboxStream.Close()) }()
	var writtenDownSeq atomic.Int64
	if handshake.Restore {
		writtenDownSeq.Store(handshake.Ack)
	}
	// 上行游标仅属于当前物理连接，重连只续接 MsgQ 的下行邮箱。
	var acceptedUpSeq int64
	writer := sessionFrameWriter{transport: transport}
	if err := writer.Write(sessionCtx, Frame{Kind: FrameReady}); err != nil {
		cancel()
		return err
	}
	writerResult := make(chan error, 1)
	go func() {
		writerResult <- s.writeDownMsgs(sessionCtx, mailboxStream, &writer, &writtenDownSeq)
		cancel()
	}()
	defer func() {
		cancel()
		writerErr := <-writerResult
		if cause := context.Cause(mailboxStream.Context()); cause != nil && !errors.Is(cause, context.Canceled) {
			serveErr = errors.Join(serveErr, cause)
		}
		if serveErr == nil && writerErr != nil && writerErr != context.Canceled {
			serveErr = writerErr
		}
	}()
	confirmedDownSeq := handshake.Ack
	for {
		frame, err := transport.Read(sessionCtx)
		if err != nil {
			return err
		}
		if frame.validAck() {
			if err := writer.validateAcknowledgement(frame.Ack, confirmedDownSeq, &writtenDownSeq); err != nil {
				return err
			}
			if err := mailboxStream.Ack(sessionCtx, frame.Ack); err != nil {
				return err
			}
			confirmedDownSeq = frame.Ack
			continue
		}
		if !frame.validUpMsg() {
			return ErrInvalidFrame
		}
		if err := writer.validateAcknowledgement(frame.Ack, confirmedDownSeq, &writtenDownSeq); err != nil {
			return err
		}
		if err := s.dispatchUpMsg(sessionCtx, mailboxStream, identity, frame, &acceptedUpSeq, &confirmedDownSeq); err != nil {
			return err
		}
	}
}

func (s *Server) initializeSession(ctx context.Context, active *activeSession, request msgq.InitRequest) (*activeSession, bool, bool, error) {
	err := active.client.Init(ctx, request)
	previous, ready, terminate := s.activeSessions.completeInitialization(active, err == nil)
	return previous, ready, terminate, err
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
	mu        sync.Mutex
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
