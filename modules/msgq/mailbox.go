package msgq

import (
	"context"
	"sync"
)

// Server 在本进程维护客户端会话与未确认下行消息。
// Redis 只保存角色到 msgq 实例的短期归属，不保存下行 payload。
type Server struct {
	instanceID string
	store      SessionStore
	maxBacklog int

	mu       sync.RWMutex
	sessions map[int64]*mailboxSession
}

// ServerOptions 配置会话邮箱服务。
type ServerOptions struct {
	InstanceID string
	Store      SessionStore
	MaxBacklog int
}

// SessionHeader 唯一标识一次客户端连接。
type SessionHeader struct {
	RoleID       int64
	SessionID    int64
	ConnectionID int64
}

// InitRequest 建立或恢复会话。Restore 会重置发送游标并确认已有 Ack。
type InitRequest struct {
	Header  SessionHeader
	Restore bool
	Ack     int64
}

// InitResult 返回当前已接收的上行序号。
type InitResult struct {
	ReceivedSeq int64
}

// RecRequest 确认一条客户端上行消息，并可携带下行确认号。
type RecRequest struct {
	Header SessionHeader
	Seq    int64
	Ack    int64
}

// RecResult 返回服务端实际接受的上行序号和确认号。
type RecResult struct {
	ReceivedSeq int64
	Ack         int64
}

// PubRequest 向一个角色的当前会话发布下行 BON 数据。
// Header 仅填写 RoleID 时表示投递给该角色当前会话；同时填写 SessionID 时会校验会话一致性。
type PubRequest struct {
	Header  SessionHeader
	Message []byte
}

// PubResult 返回下行消息序号，供 Agent 写入协议包。
type PubResult struct {
	Seq int64
}

// FetchRequest 从当前会话读取下一条尚未发送的下行消息。
type FetchRequest struct {
	Header SessionHeader
}

// FetchResult 是一条待发送消息及其确认序号。
type FetchResult struct {
	Seq     int64
	Message []byte
}

// ConfirmRequest 确认客户端已收到指定序号及之前的所有下行消息。
type ConfirmRequest struct {
	Header SessionHeader
	Ack    int64
}

// TouchRequest 续期当前会话的 Redis 归属。
type TouchRequest struct {
	Header SessionHeader
}

// NewServer 创建一个有界的进程内会话邮箱。
func NewServer(options ServerOptions) (*Server, error) {
	if !validValue(options.InstanceID) || options.Store == nil || options.MaxBacklog <= 0 {
		return nil, ErrInvalidServer
	}
	return &Server{
		instanceID: options.InstanceID,
		store:      options.Store,
		maxBacklog: options.MaxBacklog,
		sessions:   make(map[int64]*mailboxSession),
	}, nil
}

// Init 建立新会话，或在相同 SessionID 重连时恢复未确认下行消息。
func (s *Server) Init(ctx context.Context, request InitRequest) (InitResult, error) {
	if s == nil {
		return InitResult{}, ErrServerNil
	}
	if err := request.Header.validateConnection(); err != nil || request.Ack < 0 {
		return InitResult{}, ErrInvalidHeader
	}
	binding := SessionBinding{
		RoleID:       request.Header.RoleID,
		SessionID:    request.Header.SessionID,
		ConnectionID: request.Header.ConnectionID,
		InstanceID:   s.instanceID,
	}
	if _, _, err := s.store.Bind(ctx, binding); err != nil {
		return InitResult{}, err
	}

	session := s.install(request.Header)
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.active || session.sessionID != request.Header.SessionID {
		return InitResult{}, ErrSessionChanged
	}
	session.connectionID = request.Header.ConnectionID
	if request.Restore {
		session.outbound.resetSent()
		session.outbound.confirm(request.Ack)
	}
	session.signal()
	return InitResult{ReceivedSeq: session.receivedSeq}, nil
}

// Rec 串行确认一条客户端上行消息。重复序号幂等返回当前已接收序号。
func (s *Server) Rec(ctx context.Context, request RecRequest) (RecResult, error) {
	if request.Seq <= 0 || request.Ack < 0 {
		return RecResult{}, ErrInboundSequence
	}
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return RecResult{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireConnection(request.Header); err != nil {
		return RecResult{}, err
	}
	if request.Seq > session.receivedSeq+1 {
		return RecResult{}, ErrInboundSequence
	}
	if request.Seq == session.receivedSeq+1 {
		session.receivedSeq++
	}
	session.outbound.confirm(request.Ack)
	return RecResult{ReceivedSeq: session.receivedSeq, Ack: request.Ack}, nil
}

// Pub 将数据追加到当前会话的未确认下行队列。payload 会复制，调用方之后可安全复用原切片。
func (s *Server) Pub(_ context.Context, request PubRequest) (PubResult, error) {
	if s == nil {
		return PubResult{}, ErrServerNil
	}
	if request.Header.RoleID <= 0 || request.Header.SessionID < 0 || request.Header.ConnectionID < 0 {
		return PubResult{}, ErrInvalidHeader
	}
	session, found := s.lookup(request.Header.RoleID)
	if !found {
		return PubResult{}, ErrSessionOffline
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.active {
		return PubResult{}, ErrSessionOffline
	}
	if request.Header.SessionID != 0 && request.Header.SessionID != session.sessionID {
		return PubResult{}, ErrSessionChanged
	}
	if session.outbound.len() >= s.maxBacklog {
		return PubResult{}, ErrOutboundOverflow
	}
	seq := session.outbound.enqueue(request.Message)
	session.signal()
	return PubResult{Seq: seq}, nil
}

// Fetch 返回下一条尚未发送的下行消息；队列为空时等待 Pub 或 ctx 结束。
func (s *Server) Fetch(ctx context.Context, request FetchRequest) (FetchResult, error) {
	for {
		session, err := s.touch(ctx, request.Header)
		if err != nil {
			return FetchResult{}, err
		}
		session.mu.Lock()
		if err := session.requireConnection(request.Header); err != nil {
			session.mu.Unlock()
			return FetchResult{}, err
		}
		if entry, found := session.outbound.dequeue(); found {
			session.mu.Unlock()
			return FetchResult{Seq: entry.seq, Message: cloneBytes(entry.message)}, nil
		}
		notify := session.notify
		session.mu.Unlock()

		select {
		case <-ctx.Done():
			return FetchResult{}, ctx.Err()
		case <-notify:
		}
	}
}

// Confirm 释放指定 Ack 之前的下行消息。
func (s *Server) Confirm(ctx context.Context, request ConfirmRequest) error {
	if request.Ack < 0 {
		return ErrInvalidHeader
	}
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireConnection(request.Header); err != nil {
		return err
	}
	session.outbound.confirm(request.Ack)
	return nil
}

// Touch 仅续期会话 owner，供连接空闲但仍保持在线时调用。
func (s *Server) Touch(ctx context.Context, request TouchRequest) error {
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.requireConnection(request.Header)
}

// Close 删除当前会话。只有 Redis 仍保存完全相同 binding 时才会删除 owner。
func (s *Server) Close(ctx context.Context, header SessionHeader) error {
	if s == nil {
		return ErrServerNil
	}
	if err := header.validateConnection(); err != nil {
		return err
	}
	session, found := s.lookup(header.RoleID)
	if !found {
		return ErrSessionOffline
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireConnection(header); err != nil {
		return err
	}
	binding := SessionBinding{
		RoleID:       header.RoleID,
		SessionID:    header.SessionID,
		ConnectionID: header.ConnectionID,
		InstanceID:   s.instanceID,
	}
	removed, err := s.store.Unbind(ctx, binding)
	if err != nil {
		return err
	}
	if !removed {
		return ErrSessionChanged
	}
	s.removeLocked(session)
	return nil
}

// SessionCount 返回当前进程内活动会话数。
func (s *Server) SessionCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func (s *Server) install(header SessionHeader) *mailboxSession {
	s.mu.Lock()
	current := s.sessions[header.RoleID]
	if current != nil && current.sessionID == header.SessionID {
		s.mu.Unlock()
		return current
	}
	session := newMailboxSession(header)
	s.sessions[header.RoleID] = session
	s.mu.Unlock()
	if current != nil {
		current.mu.Lock()
		current.active = false
		current.signal()
		current.mu.Unlock()
	}
	return session
}

func (s *Server) lookup(roleID int64) (*mailboxSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, found := s.sessions[roleID]
	return session, found
}

func (s *Server) remove(session *mailboxSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	s.removeLocked(session)
}

func (s *Server) removeLocked(session *mailboxSession) {
	s.mu.Lock()
	if s.sessions[session.roleID] == session {
		delete(s.sessions, session.roleID)
	}
	s.mu.Unlock()
	session.active = false
	session.signal()
}

func (s *Server) touch(ctx context.Context, header SessionHeader) (*mailboxSession, error) {
	if s == nil {
		return nil, ErrServerNil
	}
	if err := header.validateConnection(); err != nil {
		return nil, err
	}
	session, found := s.lookup(header.RoleID)
	if !found {
		return nil, ErrSessionOffline
	}
	session.mu.Lock()
	err := session.requireConnection(header)
	session.mu.Unlock()
	if err != nil {
		return nil, err
	}
	refreshed, err := s.store.Refresh(ctx, SessionBinding{
		RoleID:       header.RoleID,
		SessionID:    header.SessionID,
		ConnectionID: header.ConnectionID,
		InstanceID:   s.instanceID,
	})
	if err != nil {
		return nil, err
	}
	if !refreshed {
		s.remove(session)
		return nil, ErrSessionChanged
	}
	return session, nil
}

func (h SessionHeader) validateConnection() error {
	if h.RoleID <= 0 || h.SessionID <= 0 || h.ConnectionID <= 0 {
		return ErrInvalidHeader
	}
	return nil
}

type mailboxSession struct {
	roleID       int64
	sessionID    int64
	connectionID int64
	receivedSeq  int64
	active       bool
	notify       chan struct{}
	outbound     outboundQueue
	mu           sync.Mutex
}

func newMailboxSession(header SessionHeader) *mailboxSession {
	return &mailboxSession{
		roleID:       header.RoleID,
		sessionID:    header.SessionID,
		connectionID: header.ConnectionID,
		active:       true,
		notify:       make(chan struct{}, 1),
	}
}

func (s *mailboxSession) requireConnection(header SessionHeader) error {
	if !s.active {
		return ErrSessionOffline
	}
	if s.sessionID != header.SessionID || s.connectionID != header.ConnectionID {
		return ErrSessionChanged
	}
	return nil
}

func (s *mailboxSession) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

type outboundEntry struct {
	seq     int64
	message []byte
}

type outboundQueue struct {
	entries []outboundEntry
	sent    int
	nextSeq int64
}

func (q *outboundQueue) enqueue(message []byte) int64 {
	q.nextSeq++
	q.entries = append(q.entries, outboundEntry{seq: q.nextSeq, message: cloneBytes(message)})
	return q.nextSeq
}

func (q *outboundQueue) dequeue() (outboundEntry, bool) {
	if q.sent >= len(q.entries) {
		return outboundEntry{}, false
	}
	entry := q.entries[q.sent]
	q.sent++
	return entry, true
}

func (q *outboundQueue) resetSent() {
	q.sent = 0
}

func (q *outboundQueue) confirm(ack int64) {
	cut := 0
	for cut < len(q.entries) && q.entries[cut].seq <= ack {
		cut++
	}
	if cut == 0 {
		return
	}
	copy(q.entries, q.entries[cut:])
	for index := len(q.entries) - cut; index < len(q.entries); index++ {
		q.entries[index] = outboundEntry{}
	}
	q.entries = q.entries[:len(q.entries)-cut]
	q.sent -= cut
	if q.sent < 0 {
		q.sent = 0
	}
}

func (q *outboundQueue) len() int {
	return len(q.entries)
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
