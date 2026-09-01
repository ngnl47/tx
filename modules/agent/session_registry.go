package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
)

const sessionTerminationTimeout = 5 * time.Second

// SessionControlResult 返回一次本地 Session 控制操作命中的连接身份。
type SessionControlResult struct {
	SessionID    int64
	ConnectionID int64
	Handled      bool
}

type activeSession struct {
	identity           Identity
	header             msgq.SessionHeader
	client             msgq.SessionClient
	cancel             context.CancelFunc
	transport          Transport
	initializationDone chan struct{}
	initialized        bool
	suppressReady      bool
	terminateRequested bool
	terminationDone    chan struct{}
	terminationErr     error
}

func (session *activeSession) disconnect() {
	if session == nil {
		return
	}
	session.cancel()
	_ = session.transport.Close()
}

func (session *activeSession) completeTermination(err error) {
	session.terminationErr = err
	close(session.terminationDone)
}

type sessionTerminationOperation struct {
	sessionID int64
	pending   map[int64]*activeSession
	done      chan struct{}
	result    SessionControlResult
	err       error
}

type sessionRegistry struct {
	mu           sync.Mutex
	sessions     map[int64]*activeSession
	initializing map[int64]map[int64]*activeSession
	terminating  map[int64]*sessionTerminationOperation
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		sessions:     make(map[int64]*activeSession),
		initializing: make(map[int64]map[int64]*activeSession),
		terminating:  make(map[int64]*sessionTerminationOperation),
	}
}

func (registry *sessionRegistry) beginInitialization(session *activeSession) {
	registry.mu.Lock()
	if session.initializationDone == nil {
		session.initializationDone = make(chan struct{})
		session.terminationDone = make(chan struct{})
	}
	if operation := registry.terminating[session.header.SessionID]; operation != nil {
		session.suppressReady = true
		session.terminateRequested = true
		operation.pending[session.header.ConnectionID] = session
	}
	connections := registry.initializing[session.header.SessionID]
	if connections == nil {
		connections = make(map[int64]*activeSession)
		registry.initializing[session.header.SessionID] = connections
	}
	connections[session.header.ConnectionID] = session
	registry.mu.Unlock()
}

// completeInitialization 公布 MsgQ Init 结果，并在未被控制请求截断时激活连接。
func (registry *sessionRegistry) completeInitialization(session *activeSession, initialized bool) (previous *activeSession, ready, terminateOnInitialization bool) {
	registry.mu.Lock()
	connections := registry.initializing[session.header.SessionID]
	if connections[session.header.ConnectionID] == session {
		delete(connections, session.header.ConnectionID)
		if len(connections) == 0 {
			delete(registry.initializing, session.header.SessionID)
		}
	}
	session.initialized = initialized
	ready = initialized && !session.suppressReady
	if ready {
		previous = registry.sessions[session.header.SessionID]
		registry.sessions[session.header.SessionID] = session
	}
	terminateOnInitialization = session.terminateRequested
	close(session.initializationDone)
	registry.mu.Unlock()
	return previous, ready, terminateOnInitialization
}

func (registry *sessionRegistry) unbind(sessionID, connectionID int64) {
	registry.mu.Lock()
	current := registry.sessions[sessionID]
	if current != nil && current.header.ConnectionID == connectionID {
		delete(registry.sessions, sessionID)
	}
	registry.mu.Unlock()
}

func (registry *sessionRegistry) disconnectTargets(sessionID int64) (*activeSession, []*activeSession) {
	registry.mu.Lock()
	current := registry.sessions[sessionID]
	connections := registry.initializing[sessionID]
	initializing := make([]*activeSession, 0, len(connections))
	for _, session := range connections {
		session.suppressReady = true
		initializing = append(initializing, session)
	}
	registry.mu.Unlock()
	return current, initializing
}

func (registry *sessionRegistry) beginTermination(sessionID int64) (*sessionTerminationOperation, *activeSession, bool) {
	registry.mu.Lock()
	if operation := registry.terminating[sessionID]; operation != nil {
		registry.mu.Unlock()
		return operation, nil, false
	}
	operation := &sessionTerminationOperation{
		sessionID: sessionID,
		pending:   make(map[int64]*activeSession),
		done:      make(chan struct{}),
		result:    SessionControlResult{SessionID: sessionID},
	}
	registry.terminating[sessionID] = operation
	current := registry.sessions[sessionID]
	for connectionID, session := range registry.initializing[sessionID] {
		session.suppressReady = true
		session.terminateRequested = true
		operation.pending[connectionID] = session
	}
	registry.mu.Unlock()
	return operation, current, true
}

// nextTerminationTarget 返回终止期间加入的初始化连接；没有剩余连接时原子完成操作。
func (registry *sessionRegistry) nextTerminationTarget(operation *sessionTerminationOperation, result SessionControlResult, err error) (*activeSession, bool) {
	registry.mu.Lock()
	for connectionID, session := range operation.pending {
		delete(operation.pending, connectionID)
		registry.mu.Unlock()
		return session, false
	}
	operation.result = result
	operation.err = err
	if registry.terminating[operation.sessionID] == operation {
		delete(registry.terminating, operation.sessionID)
	}
	close(operation.done)
	registry.mu.Unlock()
	return nil, true
}

// DisconnectSession 关闭当前 Agent 本地持有的物理连接，保留逻辑 Session 供恢复。
func (s *Server) DisconnectSession(sessionID int64) SessionControlResult {
	if s == nil || s.activeSessions == nil || sessionID <= 0 {
		return SessionControlResult{SessionID: sessionID}
	}
	session, initializing := s.activeSessions.disconnectTargets(sessionID)
	if session == nil && len(initializing) == 0 {
		return SessionControlResult{SessionID: sessionID}
	}
	connectionID := int64(0)
	if session != nil {
		connectionID = session.header.ConnectionID
		session.disconnect()
	}
	for _, pending := range initializing {
		if connectionID == 0 {
			connectionID = pending.header.ConnectionID
		}
		pending.disconnect()
	}
	return SessionControlResult{SessionID: sessionID, ConnectionID: connectionID, Handled: true}
}

// TerminateSession 终止当前 Agent 本地持有的逻辑 Session，并关闭对应物理连接。
func (s *Server) TerminateSession(ctx context.Context, sessionID int64) (SessionControlResult, error) {
	if s == nil || s.activeSessions == nil || sessionID <= 0 {
		return SessionControlResult{SessionID: sessionID}, nil
	}
	operation, current, owner := s.activeSessions.beginTermination(sessionID)
	if owner {
		go s.runSessionTermination(operation, current)
	}
	select {
	case <-operation.done:
		return operation.result, operation.err
	case <-ctx.Done():
		return SessionControlResult{SessionID: sessionID}, ctx.Err()
	}
}

func (s *Server) runSessionTermination(operation *sessionTerminationOperation, current *activeSession) {
	result := SessionControlResult{SessionID: operation.sessionID}
	successfulConnectionID := int64(0)
	sessionChanged := false
	var errs []error
	if current != nil {
		result.Handled = true
		result.ConnectionID = current.header.ConnectionID
		err := terminateActiveSession(current)
		current.disconnect()
		switch {
		case err == nil || errors.Is(err, msgq.ErrSessionOffline):
			successfulConnectionID = current.header.ConnectionID
		case errors.Is(err, msgq.ErrSessionChanged):
			sessionChanged = true
		default:
			errs = append(errs, err)
		}
	}
	for {
		pending, finished := s.activeSessions.nextTerminationTarget(operation, result, terminationResultError(successfulConnectionID, sessionChanged, errs))
		if finished {
			return
		}
		result.Handled = true
		result.ConnectionID = pending.header.ConnectionID
		pending.disconnect()
		<-pending.initializationDone
		<-pending.terminationDone
		switch {
		case pending.terminationErr == nil || errors.Is(pending.terminationErr, msgq.ErrSessionOffline):
			successfulConnectionID = pending.header.ConnectionID
			result.ConnectionID = successfulConnectionID
		case errors.Is(pending.terminationErr, msgq.ErrSessionChanged):
			sessionChanged = true
		default:
			errs = append(errs, pending.terminationErr)
		}
	}
}

func terminationResultError(successfulConnectionID int64, sessionChanged bool, errs []error) error {
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if successfulConnectionID == 0 && sessionChanged {
		return msgq.ErrSessionChanged
	}
	return nil
}

func terminateActiveSession(session *activeSession) error {
	ctx, cancel := context.WithTimeout(context.Background(), sessionTerminationTimeout)
	defer cancel()
	return session.client.TerminateSession(ctx, session.header)
}
