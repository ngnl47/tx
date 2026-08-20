package msgq

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	txlog "github.com/ngnl47/tx/log"
)

const (
	defaultOfflineTimeout  = 60 * time.Second
	defaultSweepInterval   = 2 * time.Second
	defaultMaxBacklogBytes = 64 * 1024 * 1024
)

// Server 在本进程维护客户端会话与未确认下行消息。
// Redis 只保存角色到 msgq 实例的短期归属，不保存下行 payload。
type Server struct {
	instanceID      string
	store           SessionStore
	maxBacklog      int
	maxBacklogBytes int
	offlineTTL      time.Duration
	sweepEvery      time.Duration
	reaping         atomic.Bool

	mu       sync.RWMutex
	sessions map[int64]*mailboxSession

	topicMu sync.RWMutex
	topics  map[string]map[*mailboxSession]struct{}
}

// ServerOptions 配置会话邮箱服务。
type ServerOptions struct {
	InstanceID string
	Store      SessionStore
	MaxBacklog int
	// MaxBacklogBytes 限制每个会话中尚未确认 payload 的总字节数。零值使用安全默认值。
	MaxBacklogBytes int
	OfflineTimeout  time.Duration
	SweepInterval   time.Duration
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
	Header SessionHeader
	// Message 在 Pub 成功后转交 msgq 只读持有，调用方不得修改、append、复用或归还该切片。
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
	Seq int64
	// Message 引用 msgq 当前持有的只读 payload，调用方不得修改、append、复用或归还该切片。
	Message []byte
}

// FetchSomeRequest 从当前会话批量读取尚未发送的下行消息。Max 为零时读取当前全部可发送消息。
type FetchSomeRequest struct {
	Header SessionHeader
	Max    int
}

// FetchSomeResult 是一批保持发送顺序的下行消息。
type FetchSomeResult struct {
	Messages []FetchResult
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

// TopicRequest 对当前连接管理本机 topic 订阅。
type TopicRequest struct {
	Header SessionHeader
	Topics []string
}

// TopicsResult 返回当前会话的 topic 快照，按字典序排列。
type TopicsResult struct {
	Topics []string
}

// BroadcastTopicRequest 向当前 msgq 实例内订阅 topic 的会话发送 BON 数据。
type BroadcastTopicRequest struct {
	Topic string
	// Message 在 BroadcastTopic 成功后转交 msgq 只读持有，多个会话可能共享同一引用。
	Message []byte
}

// BroadcastTopicResult 说明一次本地广播的实际投递结果。
type BroadcastTopicResult struct {
	Attempted  int
	Delivered  int
	Skipped    int
	Overflowed int
}

// NewServer 创建一个有界的进程内会话邮箱。
func NewServer(options ServerOptions) (*Server, error) {
	if !validValue(options.InstanceID) || options.Store == nil || options.MaxBacklog <= 0 || options.OfflineTimeout < 0 || options.SweepInterval < 0 {
		return nil, ErrInvalidServer
	}
	if options.OfflineTimeout == 0 {
		options.OfflineTimeout = defaultOfflineTimeout
	}
	if options.MaxBacklogBytes == 0 {
		options.MaxBacklogBytes = defaultMaxBacklogBytes
	}
	if options.MaxBacklogBytes < 0 {
		return nil, ErrInvalidServer
	}
	if options.SweepInterval == 0 {
		options.SweepInterval = defaultSweepInterval
	}
	return &Server{
		instanceID:      options.InstanceID,
		store:           options.Store,
		maxBacklog:      options.MaxBacklog,
		maxBacklogBytes: options.MaxBacklogBytes,
		offlineTTL:      options.OfflineTimeout,
		sweepEvery:      options.SweepInterval,
		sessions:        make(map[int64]*mailboxSession),
		topics:          make(map[string]map[*mailboxSession]struct{}),
	}, nil
}

// Init 建立新会话，或在相同 SessionID 重连时恢复未确认下行消息。
func (s *Server) Init(ctx context.Context, request InitRequest) (InitResult, error) {
	if s == nil {
		return InitResult{}, ErrServerNil
	}
	if err := request.Header.validateConnection(); err != nil || request.Ack < 0 || (!request.Restore && request.Ack != 0) {
		return InitResult{}, ErrInvalidHeader
	}
	if request.Restore {
		if current, found := s.lookup(request.Header.RoleID); found && current.sessionID == request.Header.SessionID {
			current.mu.Lock()
			valid := current.outbound.validateAck(request.Ack) == nil
			current.mu.Unlock()
			if !valid {
				return InitResult{}, ErrOutboundAcknowledgement
			}
		}
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
	session.lastActivity = time.Now()
	if request.Restore {
		session.outbound.resetSent()
		if err := session.outbound.confirm(request.Ack); err != nil {
			return InitResult{}, err
		}
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
	if err := session.outbound.validateAck(request.Ack); err != nil {
		return RecResult{}, err
	}
	if request.Seq > session.receivedSeq+1 {
		return RecResult{}, ErrInboundSequence
	}
	if request.Seq == session.receivedSeq+1 {
		session.receivedSeq++
	}
	if err := session.outbound.confirm(request.Ack); err != nil {
		return RecResult{}, err
	}
	return RecResult{ReceivedSeq: session.receivedSeq, Ack: request.Ack}, nil
}

// Pub 将数据追加到当前会话的未确认下行队列。
//
// Message 的所有权在调用成功后移交给 msgq：调用方不得再修改、append、复用或归还该切片，直到对应会话关闭或消息被确认。
// 该约束使本地广播可以让多个会话共享同一份已序列化正文；跨进程 gRPC 调用在传输边界自然拥有独立字节副本。
func (s *Server) Pub(ctx context.Context, request PubRequest) (PubResult, error) {
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
	if !session.active {
		session.mu.Unlock()
		return PubResult{}, ErrSessionOffline
	}
	if request.Header.SessionID != 0 && request.Header.SessionID != session.sessionID {
		session.mu.Unlock()
		return PubResult{}, ErrSessionChanged
	}
	if session.outbound.exceeds(s.maxBacklog, s.maxBacklogBytes, request.Message) {
		header := session.header()
		session.mu.Unlock()
		s.evict(ctx, session, header)
		return PubResult{}, ErrOutboundOverflow
	}
	seq := session.outbound.enqueue(request.Message)
	session.signal()
	session.mu.Unlock()
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
			return FetchResult{Seq: entry.seq, Message: entry.message}, nil
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

// FetchSome 返回当前可发送的一批消息；队列为空时等待 Pub 或 ctx 结束。
func (s *Server) FetchSome(ctx context.Context, request FetchSomeRequest) (FetchSomeResult, error) {
	if request.Max < 0 {
		return FetchSomeResult{}, ErrInvalidHeader
	}
	for {
		session, err := s.touch(ctx, request.Header)
		if err != nil {
			return FetchSomeResult{}, err
		}
		session.mu.Lock()
		if err := session.requireConnection(request.Header); err != nil {
			session.mu.Unlock()
			return FetchSomeResult{}, err
		}
		entries := session.outbound.dequeueSome(request.Max)
		if len(entries) > 0 {
			messages := make([]FetchResult, len(entries))
			for index, entry := range entries {
				messages[index] = FetchResult{Seq: entry.seq, Message: entry.message}
			}
			session.mu.Unlock()
			return FetchSomeResult{Messages: messages}, nil
		}
		notify := session.notify
		session.mu.Unlock()

		select {
		case <-ctx.Done():
			return FetchSomeResult{}, ctx.Err()
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
	return session.outbound.confirm(request.Ack)
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

// JoinTopics 将当前连接加入本机 topic 索引。
func (s *Server) JoinTopics(ctx context.Context, request TopicRequest) error {
	if err := validateTopics(request.Topics); err != nil {
		return err
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
	for _, topic := range request.Topics {
		if _, exists := session.topics[topic]; exists {
			continue
		}
		session.topics[topic] = struct{}{}
		s.addTopicLocked(session, topic)
	}
	return nil
}

// LeaveTopics 将当前连接移出本机 topic 索引。
func (s *Server) LeaveTopics(ctx context.Context, request TopicRequest) error {
	if err := validateTopics(request.Topics); err != nil {
		return err
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
	for _, topic := range request.Topics {
		if _, exists := session.topics[topic]; !exists {
			continue
		}
		delete(session.topics, topic)
		s.removeTopicLocked(session, topic)
	}
	return nil
}

// Topics 返回当前连接已加入的本机 topic 快照。
func (s *Server) Topics(ctx context.Context, request TopicRequest) (TopicsResult, error) {
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return TopicsResult{}, err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireConnection(request.Header); err != nil {
		return TopicsResult{}, err
	}
	topics := make([]string, 0, len(session.topics))
	for topic := range session.topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return TopicsResult{Topics: topics}, nil
}

// BroadcastTopic 向当前实例内订阅 topic 的会话广播。
// 部分会话队列满时，已成功投递的会话不会回滚，返回值会保留完整统计并返回 ErrOutboundOverflow。
func (s *Server) BroadcastTopic(ctx context.Context, request BroadcastTopicRequest) (BroadcastTopicResult, error) {
	if s == nil {
		return BroadcastTopicResult{}, ErrServerNil
	}
	if !validValue(request.Topic) {
		return BroadcastTopicResult{}, ErrInvalidTopic
	}
	sessions := s.topicSnapshot(request.Topic)
	result := BroadcastTopicResult{Attempted: len(sessions)}
	overflowed := make([]*mailboxSession, 0)
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		session.mu.Lock()
		if !session.active {
			result.Skipped++
			session.mu.Unlock()
			continue
		}
		if session.outbound.exceeds(s.maxBacklog, s.maxBacklogBytes, request.Message) {
			result.Overflowed++
			overflowed = append(overflowed, session)
			session.mu.Unlock()
			continue
		}
		session.outbound.enqueue(request.Message)
		session.signal()
		result.Delivered++
		session.mu.Unlock()
	}
	for _, session := range overflowed {
		session.mu.Lock()
		active := session.active
		header := session.header()
		session.mu.Unlock()
		if active {
			s.evict(ctx, session, header)
		}
	}
	if result.Overflowed > 0 {
		return result, ErrOutboundOverflow
	}
	return result, nil
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

// Reap 移除超过 OfflineTimeout 未产生客户端会话活动的本地 Session。
// 即使 Redis 条件解绑失败，失活会话也会从本机移除，Redis TTL 会兜底清理旧 owner。
func (s *Server) Reap(ctx context.Context, now time.Time) (int, error) {
	if s == nil {
		return 0, ErrServerNil
	}
	if ctx == nil {
		return 0, ErrLifecycleNil
	}
	if now.IsZero() {
		now = time.Now()
	}
	sessions := s.sessionSnapshot()
	removed := 0
	var errs []error
	for _, session := range sessions {
		session.mu.Lock()
		expired := now.Sub(session.lastActivity) >= s.offlineTTL || session.outbound.expired(now, s.offlineTTL)
		if !session.active || !expired {
			session.mu.Unlock()
			continue
		}
		binding := SessionBinding{
			RoleID:       session.roleID,
			SessionID:    session.sessionID,
			ConnectionID: session.connectionID,
			InstanceID:   s.instanceID,
		}
		_, err := s.store.Unbind(ctx, binding)
		s.removeLocked(session)
		session.mu.Unlock()
		removed++
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, errors.Join(errs...)
}

// Start 将空闲会话清理循环绑定到服务的 WaitGroupContext。
func (s *Server) Start(ctx waitGroupContext) error {
	if s == nil {
		return ErrServerNil
	}
	if ctx == nil {
		return ErrLifecycleNil
	}
	if !s.reaping.CompareAndSwap(false, true) {
		return ErrReaperRunning
	}
	return ctx.Go(func(context.Context) {
		defer s.reaping.Store(false)
		ticker := time.NewTicker(s.sweepEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if _, err := s.Reap(ctx, now); err != nil && ctx.Err() == nil {
					txlog.ErrorContext(ctx, "msgq reap idle sessions failed", "err", err)
				}
			}
		}
	})
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
		s.removeSessionTopicsLocked(current)
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

func (s *Server) sessionSnapshot() []*mailboxSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]*mailboxSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
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
	s.removeSessionTopicsLocked(session)
	session.signal()
}

func (s *Server) evict(ctx context.Context, session *mailboxSession, header SessionHeader) {
	session.mu.Lock()
	if !session.active || session.sessionID != header.SessionID || session.connectionID != header.ConnectionID {
		session.mu.Unlock()
		return
	}
	s.removeLocked(session)
	session.mu.Unlock()
	removed, err := s.store.Unbind(ctx, SessionBinding{
		RoleID: header.RoleID, SessionID: header.SessionID, ConnectionID: header.ConnectionID, InstanceID: s.instanceID,
	})
	if err != nil {
		txlog.ErrorContext(ctx, "msgq evict session unbind failed", "roleID", header.RoleID, "err", err)
		return
	}
	if !removed {
		txlog.ErrorContext(ctx, "msgq evict session binding changed", "roleID", header.RoleID)
	}
}

func (s *Server) addTopicLocked(session *mailboxSession, topic string) {
	s.topicMu.Lock()
	defer s.topicMu.Unlock()
	sessions := s.topics[topic]
	if sessions == nil {
		sessions = make(map[*mailboxSession]struct{})
		s.topics[topic] = sessions
	}
	sessions[session] = struct{}{}
}

func (s *Server) removeTopicLocked(session *mailboxSession, topic string) {
	s.topicMu.Lock()
	defer s.topicMu.Unlock()
	sessions := s.topics[topic]
	if sessions == nil {
		return
	}
	delete(sessions, session)
	if len(sessions) == 0 {
		delete(s.topics, topic)
	}
}

func (s *Server) removeSessionTopicsLocked(session *mailboxSession) {
	for topic := range session.topics {
		s.removeTopicLocked(session, topic)
	}
	clear(session.topics)
}

func (s *Server) topicSnapshot(topic string) []*mailboxSession {
	s.topicMu.RLock()
	defer s.topicMu.RUnlock()
	sessions := s.topics[topic]
	result := make([]*mailboxSession, 0, len(sessions))
	for session := range sessions {
		result = append(result, session)
	}
	return result
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
	session.mu.Lock()
	if err := session.requireConnection(header); err != nil {
		session.mu.Unlock()
		return nil, err
	}
	session.lastActivity = time.Now()
	session.mu.Unlock()
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
	lastActivity time.Time
	notify       chan struct{}
	outbound     outboundQueue
	topics       map[string]struct{}
	mu           sync.Mutex
}

func newMailboxSession(header SessionHeader) *mailboxSession {
	return &mailboxSession{
		roleID:       header.RoleID,
		sessionID:    header.SessionID,
		connectionID: header.ConnectionID,
		active:       true,
		lastActivity: time.Now(),
		notify:       make(chan struct{}, 1),
		topics:       make(map[string]struct{}),
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

func (s *mailboxSession) header() SessionHeader {
	return SessionHeader{RoleID: s.roleID, SessionID: s.sessionID, ConnectionID: s.connectionID}
}

type outboundEntry struct {
	seq       int64
	message   []byte
	createdAt time.Time
}

type outboundQueue struct {
	entries      []outboundEntry
	head         int
	sent         int
	nextSeq      int64
	sentSeq      int64
	confirmedSeq int64
	bytes        int
}

func (q *outboundQueue) enqueue(message []byte) int64 {
	q.nextSeq++
	q.entries = append(q.entries, outboundEntry{seq: q.nextSeq, message: message, createdAt: time.Now()})
	q.bytes += len(message)
	return q.nextSeq
}

func (q *outboundQueue) dequeue() (outboundEntry, bool) {
	if q.sent >= len(q.entries) {
		return outboundEntry{}, false
	}
	entry := q.entries[q.sent]
	q.sent++
	q.sentSeq = entry.seq
	return entry, true
}

func (q *outboundQueue) dequeueSome(max int) []outboundEntry {
	available := len(q.entries) - q.sent
	if available <= 0 {
		return nil
	}
	if max > 0 && available > max {
		available = max
	}
	entries := q.entries[q.sent : q.sent+available]
	q.sent += available
	q.sentSeq = entries[len(entries)-1].seq
	return entries
}

func (q *outboundQueue) resetSent() {
	q.sent = q.head
}

func (q *outboundQueue) validateAck(ack int64) error {
	if ack < q.confirmedSeq || ack > q.sentSeq {
		return ErrOutboundAcknowledgement
	}
	return nil
}

func (q *outboundQueue) confirm(ack int64) error {
	if err := q.validateAck(ack); err != nil {
		return err
	}
	q.confirmedSeq = ack
	cut := q.head
	for cut < len(q.entries) && q.entries[cut].seq <= ack {
		q.bytes -= len(q.entries[cut].message)
		q.entries[cut] = outboundEntry{}
		cut++
	}
	if cut == q.head {
		return nil
	}
	q.head = cut
	if q.sent < q.head {
		q.sent = q.head
	}
	q.compactStorage()
	return nil
}

func (q *outboundQueue) len() int {
	return len(q.entries) - q.head
}

func (q *outboundQueue) exceeds(maxEntries, maxBytes int, message []byte) bool {
	return q.len() >= maxEntries || len(message) > maxBytes-q.bytes
}

func (q *outboundQueue) expired(now time.Time, ttl time.Duration) bool {
	return q.len() > 0 && now.Sub(q.entries[q.head].createdAt) >= ttl
}

func (q *outboundQueue) compactStorage() {
	pending := q.len()
	if pending == 0 {
		q.entries = nil
		q.head = 0
		q.sent = 0
		return
	}
	if q.head < 64 || q.head*2 < len(q.entries) {
		return
	}
	entries := make([]outboundEntry, pending, pending*2)
	copy(entries, q.entries[q.head:])
	q.entries = entries
	q.sent -= q.head
	q.head = 0
}

func validateTopics(topics []string) error {
	if len(topics) == 0 {
		return ErrInvalidTopic
	}
	for _, topic := range topics {
		if !validValue(topic) {
			return ErrInvalidTopic
		}
	}
	return nil
}

type waitGroupContext interface {
	context.Context
	Go(func(context.Context)) error
}
