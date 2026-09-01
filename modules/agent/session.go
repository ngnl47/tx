package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

const defaultFetchBatch = 1

// Server 运行一条玩家连接对应的逻辑会话。
type Server struct {
	identityResolver IdentityResolver
	dispatcher       Dispatcher
	admission        lifecycle.Admission
	sessions         SessionClientRouter
	connectionID     ConnectionIDGenerator
	fetchBatch       int
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
	if options.FetchBatch < 0 {
		return nil, ErrInvalidServer
	}
	if options.FetchBatch == 0 {
		options.FetchBatch = defaultFetchBatch
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
		fetchBatch:       options.FetchBatch,
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
	initialized, err := client.Init(sessionCtx, msgq.InitRequest{
		Header:  header,
		Restore: handshake.Restore,
		Primary: identity.AccessMode == protocol.AccessModeNormal,
		Ack:     handshake.Ack,
	})
	if err != nil {
		_, _, terminateOnInitialization := s.activeSessions.completeInitialization(active, false)
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

	previous, ready, terminateOnInitialization := s.activeSessions.completeInitialization(active, true)
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
		"upMsgSeq", initialized.UpMsgSeq,
		"downAck", handshake.Ack,
	)
	if previous != nil {
		previous.disconnect()
	}
	defer s.activeSessions.unbind(header.SessionID, header.ConnectionID)
	writer := sessionFrameWriter{transport: transport}
	var writtenDownSeq atomic.Int64
	if handshake.Restore {
		writtenDownSeq.Store(handshake.Ack)
	}
	var acceptedUpSeq atomic.Int64
	acceptedUpSeq.Store(initialized.UpMsgSeq)
	var processedUpSeq atomic.Int64
	processedUpSeq.Store(initialized.UpMsgSeq)
	if err := writer.Write(sessionCtx, Frame{Kind: FrameReady, Ack: initialized.UpMsgSeq}); err != nil {
		cancel()
		return err
	}
	writerResult := make(chan error, 1)
	go func() {
		writerResult <- s.writeDownMsgs(sessionCtx, client, header, &writer, &writtenDownSeq, &processedUpSeq)
		cancel()
	}()
	defer func() {
		cancel()
		writerErr := <-writerResult
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
			if err := client.Confirm(sessionCtx, msgq.ConfirmRequest{Header: header, Ack: frame.Ack}); err != nil {
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
		err = s.dispatchUpMsg(sessionCtx, client, header, identity, frame, &acceptedUpSeq, &processedUpSeq, &confirmedDownSeq)
		if err != nil {
			return err
		}
		if err := writer.Write(sessionCtx, Frame{Kind: FrameAck, Ack: processedUpSeq.Load()}); err != nil {
			return err
		}
	}
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

func (s *Server) dispatchUpMsg(ctx context.Context, client msgq.SessionClient, header msgq.SessionHeader, identity Identity, frame Frame, acceptedUpSeq, processedUpSeq *atomic.Int64, confirmedDownSeq *int64) error {
	var done func()
	if s.admission != nil {
		var err error
		done, err = s.admission.Enter()
		if err != nil {
			return err
		}
		defer done()
	}
	result, err := client.Rec(ctx, msgq.RecRequest{Header: header, Seq: frame.Seq, Ack: frame.Ack})
	if err != nil {
		return err
	}
	*confirmedDownSeq = frame.Ack
	if result.UpMsgSeq <= acceptedUpSeq.Load() {
		return nil
	}
	acceptedUpSeq.Store(result.UpMsgSeq)
	dispatched, err := s.dispatcher.Dispatch(ctx, DispatchRequest{
		Identity:  identity,
		Seq:       frame.Seq,
		CommandID: frame.CommandID,
		RouteKey:  frame.RouteKey,
		Payload:   cloneBytes(frame.Payload),
	})
	if err != nil {
		return err
	}
	if dispatched.ProcessedUpSeq != frame.Seq {
		return ErrInvalidDispatchResult
	}
	processedUpSeq.Store(dispatched.ProcessedUpSeq)
	return nil
}

func (s *Server) writeDownMsgs(ctx context.Context, client msgq.SessionClient, header msgq.SessionHeader, writer *sessionFrameWriter, written, processedUpSeq *atomic.Int64) error {
	for {
		batch, err := client.FetchSome(ctx, msgq.FetchSomeRequest{Header: header, Max: s.fetchBatch})
		if err != nil {
			return err
		}
		for _, message := range batch.Messages {
			if err := writer.writeDownMsg(ctx, Frame{Kind: FrameDownMsg, Seq: message.Seq, Ack: processedUpSeq.Load(), Payload: message.Payload}, written); err != nil {
				return err
			}
		}
	}
}

// sessionFrameWriter 保证一条连接上的响应帧与控制帧串行写入。
type sessionFrameWriter struct {
	transport Transport
	mu        sync.Mutex
}

func (w *sessionFrameWriter) Write(ctx context.Context, frame Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.transport.Write(ctx, frame)
}

func (w *sessionFrameWriter) writeDownMsg(ctx context.Context, frame Frame, written *atomic.Int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.transport.Write(ctx, frame); err != nil {
		return err
	}
	written.Store(frame.Seq)
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
