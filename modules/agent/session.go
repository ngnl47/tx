package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"math"
	"sync/atomic"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/msgq"
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
	}, nil
}

// Serve 完成一条连接的身份解析、上行 Seq 校验、业务分发与下行发送。
func (s *Server) Serve(ctx context.Context, transport Transport) (serveErr error) {
	if s == nil {
		return ErrServerNil
	}
	if transport == nil {
		return ErrTransportNil
	}
	defer func() { _ = transport.Close() }()

	handshake, err := transport.Read(ctx)
	if err != nil {
		return err
	}
	if !handshake.validHandshake() {
		return ErrHandshakeRequired
	}
	identity, err := s.identityResolver.ResolveIdentity(ctx, AuthRequest{Token: cloneBytes(handshake.Payload), Restore: handshake.Restore, Ack: handshake.Ack})
	if err != nil {
		return err
	}
	if !identity.valid() {
		return ErrInvalidIdentity
	}
	client, err := s.sessions.Acquire(ctx, identity.RoleID)
	if err != nil {
		return err
	}
	connectionID, err := s.connectionID()
	if err != nil {
		return err
	}
	if connectionID <= 0 {
		return ErrInvalidServer
	}
	header := msgq.SessionHeader{RoleID: identity.RoleID, SessionID: identity.SessionID, ConnectionID: connectionID}
	initialized, err := client.Init(ctx, msgq.InitRequest{Header: header, Restore: handshake.Restore, Ack: handshake.Ack})
	if err != nil {
		return err
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	var writtenOutbound atomic.Int64
	if handshake.Restore {
		writtenOutbound.Store(handshake.Ack)
	}
	writerResult := make(chan error, 1)
	go func() {
		writerResult <- s.writeOutbound(sessionCtx, client, header, transport, &writtenOutbound)
		cancel()
	}()
	defer func() {
		cancel()
		writerErr := <-writerResult
		if serveErr == nil && writerErr != nil && writerErr != context.Canceled {
			serveErr = writerErr
		}
	}()

	receivedSeq := initialized.ReceivedSeq
	confirmedOutbound := handshake.Ack
	for {
		frame, err := transport.Read(sessionCtx)
		if err != nil {
			return err
		}
		if frame.validAck() {
			if err := validateAcknowledgement(frame.Ack, confirmedOutbound, writtenOutbound.Load()); err != nil {
				return err
			}
			if err := client.Confirm(sessionCtx, msgq.ConfirmRequest{Header: header, Ack: frame.Ack}); err != nil {
				return err
			}
			confirmedOutbound = frame.Ack
			continue
		}
		if !frame.validBusiness() {
			return ErrInvalidFrame
		}
		if err := validateAcknowledgement(frame.Ack, confirmedOutbound, writtenOutbound.Load()); err != nil {
			return err
		}
		if err := s.dispatchBusiness(sessionCtx, client, header, identity, frame, &receivedSeq, &confirmedOutbound); err != nil {
			return err
		}
	}
}

func (s *Server) dispatchBusiness(ctx context.Context, client msgq.SessionClient, header msgq.SessionHeader, identity Identity, frame Frame, receivedSeq, confirmedOutbound *int64) error {
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
	*confirmedOutbound = frame.Ack
	if result.ReceivedSeq <= *receivedSeq {
		return nil
	}
	*receivedSeq = result.ReceivedSeq
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
	if dispatched.Payload == nil {
		return nil
	}
	_, err = client.Pub(ctx, msgq.PubRequest{Header: header, Payload: dispatched.Payload})
	return err
}

func (s *Server) writeOutbound(ctx context.Context, client msgq.SessionClient, header msgq.SessionHeader, transport Transport, written *atomic.Int64) error {
	for {
		batch, err := client.FetchSome(ctx, msgq.FetchSomeRequest{Header: header, Max: s.fetchBatch})
		if err != nil {
			return err
		}
		for _, message := range batch.Messages {
			if err := transport.Write(ctx, Frame{Kind: FrameOutbound, Seq: message.Seq, Payload: message.Payload}); err != nil {
				return err
			}
			written.Store(message.Seq)
		}
	}
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
