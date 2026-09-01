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
	defaultOfflineTimeout               = 180 * time.Second
	defaultTerminateDeadSessionInterval = 2 * time.Second
	defaultMaxBacklogBytes              = 64 * 1024 * 1024
	storeCompensationTimeout            = 5 * time.Second
	initLockShards                      = 64
)

// Server 在本进程维护客户端会话与未确认下行消息。
// Redis 只保存精确会话归属和角色主会话投影，不保存下行编码载荷。
type Server struct {
	instanceID                   string
	store                        SessionStore
	maxBacklogMessages           int
	maxBacklogBytes              int
	offlineTimeout               time.Duration
	terminateDeadSessionInterval time.Duration
	leaseRefreshInterval         time.Duration
	terminatingDeadSessions      atomic.Bool
	initLocks                    [initLockShards]sync.Mutex
	// beforeRestoreSessionLock 仅供包内测试观察 Restore 即将等待会话锁的阶段。
	beforeRestoreSessionLock func()

	mu       sync.RWMutex
	sessions map[int64]*mailboxSession

	topicMu sync.RWMutex
	topics  map[string]map[*mailboxSession]struct{}
}

// ServerOptions 配置会话邮箱服务。
type ServerOptions struct {
	InstanceID         string
	Store              SessionStore
	MaxBacklogMessages int
	// MaxBacklogBytes 限制每个会话中尚未确认编码载荷的总字节数。零值使用安全默认值。
	MaxBacklogBytes              int
	OfflineTimeout               time.Duration
	TerminateDeadSessionInterval time.Duration
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
	Primary bool
	Ack     int64
}

// InitResult 返回当前已接收的上行序号。
type InitResult struct {
	UpMsgSeq int64
}

// RecRequest 确认一条客户端上行消息，并可携带下行确认号。
type RecRequest struct {
	Header SessionHeader
	Seq    int64
	Ack    int64
}

// RecResult 返回服务端实际接受的上行序号和确认号。
type RecResult struct {
	UpMsgSeq int64
	Ack      int64
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
	// Payload 引用 msgq 当前持有的只读数据，调用方不得修改、append、复用或归还该切片。
	Payload []byte
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

// TouchRequest 记录当前会话活动，并在达到限频阈值时续期 Redis 归属。
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
	// Payload 在 BroadcastTopic 成功后转交 msgq 只读持有，多个会话可能共享同一引用。
	Payload []byte
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
	if !validValue(options.InstanceID) || options.Store == nil || options.MaxBacklogMessages <= 0 || options.OfflineTimeout < 0 || options.TerminateDeadSessionInterval < 0 {
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
	if options.TerminateDeadSessionInterval == 0 {
		options.TerminateDeadSessionInterval = defaultTerminateDeadSessionInterval
	}
	var leaseRefreshInterval time.Duration
	if provider, ok := options.Store.(interface{ leaseRefreshInterval() time.Duration }); ok {
		leaseRefreshInterval = provider.leaseRefreshInterval()
	}
	return &Server{
		instanceID:                   options.InstanceID,
		store:                        options.Store,
		maxBacklogMessages:           options.MaxBacklogMessages,
		maxBacklogBytes:              options.MaxBacklogBytes,
		offlineTimeout:               options.OfflineTimeout,
		terminateDeadSessionInterval: options.TerminateDeadSessionInterval,
		leaseRefreshInterval:         leaseRefreshInterval,
		sessions:                     make(map[int64]*mailboxSession),
		topics:                       make(map[string]map[*mailboxSession]struct{}),
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
	initLock := &s.initLocks[uint64(request.Header.SessionID)%initLockShards]
	initLock.Lock()
	defer initLock.Unlock()

	var session *mailboxSession
	var previousConnectionID int64
	if request.Restore {
		current, found := s.lookup(request.Header.SessionID)
		if !found {
			return InitResult{}, ErrSessionOffline
		}
		if s.beforeRestoreSessionLock != nil {
			s.beforeRestoreSessionLock()
		}
		current.mu.Lock()
		if !current.active {
			current.mu.Unlock()
			return InitResult{}, ErrSessionOffline
		}
		if current.roleID != request.Header.RoleID || current.primary != request.Primary {
			current.mu.Unlock()
			return InitResult{}, ErrSessionChanged
		}
		if err := current.downMsgs.validateAck(request.Ack); err != nil {
			current.mu.Unlock()
			return InitResult{}, err
		}
		session = current
		previousConnectionID = current.connectionID
	} else {
		var reserved bool
		session, reserved = s.reserveFresh(request.Header, request.Primary)
		if !reserved {
			return InitResult{}, ErrSessionChanged
		}
	}
	binding := SessionBinding{
		RoleID:       request.Header.RoleID,
		SessionID:    request.Header.SessionID,
		ConnectionID: request.Header.ConnectionID,
		InstanceID:   s.instanceID,
	}
	if request.Restore {
		expected := SessionBinding{
			RoleID:       session.roleID,
			SessionID:    session.sessionID,
			ConnectionID: session.connectionID,
			InstanceID:   s.instanceID,
		}
		rebound, err := s.store.Rebind(ctx, expected, binding)
		if err != nil {
			session.mu.Unlock()
			return InitResult{}, err
		}
		if !rebound {
			var primaryErr error
			if session.primary {
				primaryErr = s.unbindPrimaryForCompensation(ctx, session.roleID, session.sessionID)
			}
			s.removeLocked(session)
			session.mu.Unlock()
			return InitResult{}, errors.Join(ErrSessionChanged, primaryErr)
		}
		if session.primary {
			refreshed, refreshErr := s.store.RefreshPrimary(ctx, session.roleID, session.sessionID)
			if refreshErr != nil || !refreshed {
				exactRemoved, exactErr := s.unbindExactForCompensation(ctx, binding)
				primaryErr := s.unbindPrimaryForCompensation(ctx, session.roleID, session.sessionID)
				s.removeLocked(session)
				session.mu.Unlock()
				if refreshErr == nil {
					refreshErr = ErrSessionChanged
				} else if !exactRemoved {
					refreshErr = errors.Join(refreshErr, ErrSessionChanged)
				}
				return InitResult{}, errors.Join(refreshErr, exactErr, primaryErr)
			}
		}
	} else {
		created, err := s.store.Create(ctx, binding)
		if err != nil {
			s.abortFreshLocked(session)
			return InitResult{}, err
		}
		if !created {
			s.abortFreshLocked(session)
			return InitResult{}, ErrSessionChanged
		}
		if request.Primary {
			if err := s.store.BindPrimary(ctx, request.Header.RoleID, request.Header.SessionID); err != nil {
				exactRemoved, exactErr := s.unbindExactForCompensation(ctx, binding)
				primaryErr := s.unbindPrimaryForCompensation(ctx, request.Header.RoleID, request.Header.SessionID)
				s.abortFreshLocked(session)
				if !exactRemoved {
					return InitResult{}, errors.Join(err, exactErr, primaryErr, ErrSessionChanged)
				}
				return InitResult{}, errors.Join(err, exactErr, primaryErr)
			}
		}
		session.active = true
	}
	if !session.active || session.sessionID != request.Header.SessionID {
		session.mu.Unlock()
		return InitResult{}, ErrSessionChanged
	}
	now := time.Now()
	session.connectionID = request.Header.ConnectionID
	session.lastActivity = now
	if s.leaseRefreshInterval > 0 {
		session.nextLeaseRefresh = now.Add(s.leaseRefreshInterval)
	}
	if request.Restore {
		session.downMsgs.resetSent()
		if err := session.downMsgs.confirm(request.Ack); err != nil {
			session.mu.Unlock()
			return InitResult{}, err
		}
	}
	upMsgSeq := session.upMsgSeq
	pending := session.downMsgs.len()
	session.signal()
	session.mu.Unlock()
	logArgs := []any{
		"roleID", request.Header.RoleID,
		"sessionID", request.Header.SessionID,
		"connectionID", request.Header.ConnectionID,
		"restore", request.Restore,
		"upMsgSeq", upMsgSeq,
		"downAck", request.Ack,
		"pending", pending,
	}
	if request.Restore {
		logArgs = append(logArgs, "previousConnectionID", previousConnectionID)
	}
	txlog.DebugContext(ctx, "msgq session initialized", logArgs...)
	return InitResult{UpMsgSeq: upMsgSeq}, nil
}

// Rec 串行确认一条客户端上行消息。重复序号幂等返回当前已接收序号。
func (s *Server) Rec(ctx context.Context, request RecRequest) (RecResult, error) {
	if request.Seq <= 0 || request.Ack < 0 {
		return RecResult{}, ErrUpMsgSequence
	}
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return RecResult{}, err
	}
	session.mu.Lock()
	current, err := session.checkConnection(request.Header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, request.Header, current, err)
		return RecResult{}, err
	}
	if err := session.downMsgs.validateAck(request.Ack); err != nil {
		session.mu.Unlock()
		return RecResult{}, err
	}
	if request.Seq > session.upMsgSeq+1 {
		session.mu.Unlock()
		return RecResult{}, ErrUpMsgSequence
	}
	if request.Seq == session.upMsgSeq+1 {
		session.upMsgSeq++
	}
	if err := session.downMsgs.confirm(request.Ack); err != nil {
		session.mu.Unlock()
		return RecResult{}, err
	}
	upMsgSeq := session.upMsgSeq
	pending := session.downMsgs.len()
	session.mu.Unlock()
	txlog.DebugContext(ctx, "msgq upMsg accepted",
		"roleID", request.Header.RoleID,
		"sessionID", request.Header.SessionID,
		"connectionID", request.Header.ConnectionID,
		"seq", request.Seq,
		"upMsgSeq", upMsgSeq,
		"downAck", request.Ack,
		"pending", pending,
	)
	return RecResult{UpMsgSeq: upMsgSeq, Ack: request.Ack}, nil
}

// PubSession 将数据追加到精确会话的未确认下行队列。
//
// Message 的所有权在调用成功后移交给 msgq：调用方不得再修改、append、复用或归还该切片，直到对应会话关闭或消息被确认。
// 该约束使本地广播可以让多个会话共享同一份已序列化正文；跨进程 gRPC 调用在传输边界自然拥有独立字节副本。
func (s *Server) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error) {
	if s == nil {
		return PubResult{}, ErrServerNil
	}
	if sessionID <= 0 {
		return PubResult{}, ErrInvalidHeader
	}
	session, found := s.lookup(sessionID)
	if !found {
		return PubResult{}, ErrSessionOffline
	}
	session.mu.Lock()
	if !session.active {
		session.mu.Unlock()
		return PubResult{}, ErrSessionOffline
	}
	if session.downMsgs.exceeds(s.maxBacklogMessages, s.maxBacklogBytes, payload) {
		header := session.header()
		session.mu.Unlock()
		s.evict(ctx, session, header)
		return PubResult{}, ErrDownMsgOverflow
	}
	seq := session.downMsgs.enqueue(payload)
	pending := session.downMsgs.len()
	pendingBytes := session.downMsgs.bytes
	header := session.header()
	session.signal()
	session.mu.Unlock()
	txlog.DebugContext(ctx, "msgq downMsg published",
		"roleID", header.RoleID,
		"sessionID", header.SessionID,
		"connectionID", header.ConnectionID,
		"seq", seq,
		"bytes", len(payload),
		"pending", pending,
		"pendingBytes", pendingBytes,
	)
	return PubResult{Seq: seq}, nil
}

// Fetch 返回下一条尚未发送的下行消息；队列为空时等待下行发布或 ctx 结束。
func (s *Server) Fetch(ctx context.Context, request FetchRequest) (FetchResult, error) {
	for {
		session, err := s.touch(ctx, request.Header)
		if err != nil {
			return FetchResult{}, err
		}
		session.mu.Lock()
		current, err := session.checkConnection(request.Header)
		if err != nil {
			session.mu.Unlock()
			s.logStaleConnection(ctx, request.Header, current, err)
			return FetchResult{}, err
		}
		if entry, found := session.downMsgs.dequeue(); found {
			session.mu.Unlock()
			return FetchResult{Seq: entry.seq, Payload: entry.payload}, nil
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

// FetchSome 返回当前可发送的一批消息；队列为空时等待下行发布或 ctx 结束。
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
		current, err := session.checkConnection(request.Header)
		if err != nil {
			session.mu.Unlock()
			s.logStaleConnection(ctx, request.Header, current, err)
			return FetchSomeResult{}, err
		}
		entries := session.downMsgs.dequeueSome(request.Max)
		if len(entries) > 0 {
			messages := make([]FetchResult, len(entries))
			for index, entry := range entries {
				messages[index] = FetchResult{Seq: entry.seq, Payload: entry.payload}
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
	current, err := session.checkConnection(request.Header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, request.Header, current, err)
		return err
	}
	if err := session.downMsgs.confirm(request.Ack); err != nil {
		session.mu.Unlock()
		return err
	}
	pending := session.downMsgs.len()
	pendingBytes := session.downMsgs.bytes
	session.mu.Unlock()
	txlog.DebugContext(ctx, "msgq downMsg confirmed",
		"roleID", request.Header.RoleID,
		"sessionID", request.Header.SessionID,
		"connectionID", request.Header.ConnectionID,
		"ack", request.Ack,
		"pending", pending,
		"pendingBytes", pendingBytes,
	)
	return nil
}

// Touch 仅续期会话 owner，供连接空闲但仍保持在线时调用。
func (s *Server) Touch(ctx context.Context, request TouchRequest) error {
	_, err := s.touch(ctx, request.Header)
	return err
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
	current, err := session.checkConnection(request.Header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, request.Header, current, err)
		return err
	}
	for _, topic := range request.Topics {
		if _, exists := session.topics[topic]; exists {
			continue
		}
		session.topics[topic] = struct{}{}
		s.addTopicLocked(session, topic)
	}
	session.mu.Unlock()
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
	current, err := session.checkConnection(request.Header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, request.Header, current, err)
		return err
	}
	for _, topic := range request.Topics {
		if _, exists := session.topics[topic]; !exists {
			continue
		}
		delete(session.topics, topic)
		s.removeTopicLocked(session, topic)
	}
	session.mu.Unlock()
	return nil
}

// Topics 返回当前连接已加入的本机 topic 快照。
func (s *Server) Topics(ctx context.Context, request TopicRequest) (TopicsResult, error) {
	session, err := s.touch(ctx, request.Header)
	if err != nil {
		return TopicsResult{}, err
	}
	session.mu.Lock()
	current, err := session.checkConnection(request.Header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, request.Header, current, err)
		return TopicsResult{}, err
	}
	topics := make([]string, 0, len(session.topics))
	for topic := range session.topics {
		topics = append(topics, topic)
	}
	session.mu.Unlock()
	sort.Strings(topics)
	return TopicsResult{Topics: topics}, nil
}

// BroadcastTopic 向当前实例内订阅 topic 的会话广播。
// 部分会话队列满时，已成功投递的会话不会回滚，返回值会保留完整统计并返回 ErrDownMsgOverflow。
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
		if session.downMsgs.exceeds(s.maxBacklogMessages, s.maxBacklogBytes, request.Payload) {
			result.Overflowed++
			overflowed = append(overflowed, session)
			session.mu.Unlock()
			continue
		}
		session.downMsgs.enqueue(request.Payload)
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
		return result, ErrDownMsgOverflow
	}
	return result, nil
}

// TerminateSession 终止当前逻辑会话。只有 Redis 仍保存完全相同 binding 时才会删除 owner。
func (s *Server) TerminateSession(ctx context.Context, header SessionHeader) error {
	if s == nil {
		return ErrServerNil
	}
	if err := header.validateConnection(); err != nil {
		return err
	}
	session, found := s.lookup(header.SessionID)
	if !found {
		return ErrSessionOffline
	}
	session.mu.Lock()
	current, err := session.checkConnection(header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, header, current, err)
		return err
	}
	binding := SessionBinding{
		RoleID:       header.RoleID,
		SessionID:    header.SessionID,
		ConnectionID: header.ConnectionID,
		InstanceID:   s.instanceID,
	}
	removed, err := s.terminateSessionLocked(ctx, session, binding)
	session.mu.Unlock()
	if err != nil {
		return err
	}
	if !removed {
		return ErrSessionChanged
	}
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

// TerminateDeadSessions 终止超过 OfflineTimeout 未产生客户端会话活动的本地 Session。
// 即使 Redis 条件解绑失败，失活会话也会从本机移除，Redis TTL 会兜底清理旧 owner。
func (s *Server) TerminateDeadSessions(ctx context.Context, now time.Time) (int, error) {
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
		activityExpired := now.Sub(session.lastActivity) >= s.offlineTimeout
		backlogExpired := session.downMsgs.expired(now, s.offlineTimeout)
		expired := activityExpired || backlogExpired
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
		_, err := s.terminateSessionLocked(ctx, session, binding)
		session.mu.Unlock()
		removed++
		txlog.DebugContext(ctx, "msgq dead session terminated",
			"roleID", binding.RoleID,
			"sessionID", binding.SessionID,
			"connectionID", binding.ConnectionID,
			"instanceID", binding.InstanceID,
			"activityExpired", activityExpired,
			"backlogExpired", backlogExpired,
		)
		if err != nil {
			txlog.ErrorContext(ctx, "msgq terminate dead session unbind failed",
				"roleID", binding.RoleID,
				"sessionID", binding.SessionID,
				"connectionID", binding.ConnectionID,
				"instanceID", binding.InstanceID,
				"err", err,
			)
			errs = append(errs, err)
		}
	}
	return removed, errors.Join(errs...)
}

// Start 将失活会话终止循环绑定到服务的 WaitGroupContext。
func (s *Server) Start(ctx waitGroupContext) error {
	if s == nil {
		return ErrServerNil
	}
	if ctx == nil {
		return ErrLifecycleNil
	}
	if !s.terminatingDeadSessions.CompareAndSwap(false, true) {
		return ErrDeadSessionTerminatorRunning
	}
	return ctx.Go(func(context.Context) {
		defer s.terminatingDeadSessions.Store(false)
		ticker := time.NewTicker(s.terminateDeadSessionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if _, err := s.TerminateDeadSessions(ctx, now); err != nil && ctx.Err() == nil {
					txlog.ErrorContext(ctx, "msgq terminate dead sessions failed", "err", err)
				}
			}
		}
	})
}

// reserveFresh 在返回成功时保持新 session.mu 锁定，直到 Init 完成或回滚。
func (s *Server) reserveFresh(header SessionHeader, primary bool) (*mailboxSession, bool) {
	session := newMailboxSession(header, primary)
	session.active = false
	session.mu.Lock()
	s.mu.Lock()
	if _, found := s.sessions[header.SessionID]; found {
		s.mu.Unlock()
		session.mu.Unlock()
		return nil, false
	}
	s.sessions[header.SessionID] = session
	s.mu.Unlock()
	return session, true
}

func (s *Server) abortFreshLocked(session *mailboxSession) {
	s.removeLocked(session)
	session.mu.Unlock()
}

func (s *Server) unbindExactForCompensation(ctx context.Context, binding SessionBinding) (bool, error) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeCompensationTimeout)
	defer cancel()
	return s.store.Unbind(cleanupCtx, binding)
}

func (s *Server) unbindPrimaryForCompensation(ctx context.Context, roleID, sessionID int64) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeCompensationTimeout)
	defer cancel()
	_, err := s.store.UnbindPrimary(cleanupCtx, roleID, sessionID)
	return err
}

func (s *Server) lookup(sessionID int64) (*mailboxSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, found := s.sessions[sessionID]
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

func (s *Server) removeLocked(session *mailboxSession) {
	s.mu.Lock()
	if s.sessions[session.sessionID] == session {
		delete(s.sessions, session.sessionID)
	}
	s.mu.Unlock()
	session.active = false
	s.removeSessionTopicsLocked(session)
	session.signal()
}

// terminateSessionLocked 必须在持有 session.mu 时调用。
func (s *Server) terminateSessionLocked(ctx context.Context, session *mailboxSession, binding SessionBinding) (bool, error) {
	removed, err := s.store.Unbind(ctx, binding)
	s.removeLocked(session)
	return removed, err
}

func (s *Server) evict(ctx context.Context, session *mailboxSession, header SessionHeader) {
	session.mu.Lock()
	if !session.active || session.sessionID != header.SessionID || session.connectionID != header.ConnectionID {
		session.mu.Unlock()
		return
	}
	removed, err := s.terminateSessionLocked(ctx, session, SessionBinding{
		RoleID: header.RoleID, SessionID: header.SessionID, ConnectionID: header.ConnectionID, InstanceID: s.instanceID,
	})
	session.mu.Unlock()
	txlog.WarnContext(ctx, "msgq session evicted",
		"roleID", header.RoleID,
		"sessionID", header.SessionID,
		"connectionID", header.ConnectionID,
		"instanceID", s.instanceID,
		"reason", "backlog_overflow",
	)
	if err != nil {
		txlog.ErrorContext(ctx, "msgq evict session unbind failed",
			"roleID", header.RoleID,
			"sessionID", header.SessionID,
			"connectionID", header.ConnectionID,
			"instanceID", s.instanceID,
			"err", err,
		)
	} else if !removed {
		txlog.ErrorContext(ctx, "msgq evict session binding changed",
			"roleID", header.RoleID,
			"sessionID", header.SessionID,
			"connectionID", header.ConnectionID,
			"instanceID", s.instanceID,
		)
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
	session, found := s.lookup(header.SessionID)
	if !found {
		return nil, ErrSessionOffline
	}
	session.mu.Lock()
	current, err := session.checkConnection(header)
	if err != nil {
		session.mu.Unlock()
		s.logStaleConnection(ctx, header, current, err)
		return nil, err
	}
	now := time.Now()
	if s.leaseRefreshInterval > 0 && now.Before(session.nextLeaseRefresh) {
		session.lastActivity = now
		session.mu.Unlock()
		return session, nil
	}
	refreshed, err := s.store.Refresh(ctx, SessionBinding{
		RoleID:       header.RoleID,
		SessionID:    header.SessionID,
		ConnectionID: header.ConnectionID,
		InstanceID:   s.instanceID,
	})
	if err != nil {
		session.mu.Unlock()
		return nil, err
	}
	if !refreshed {
		s.removeLocked(session)
		session.mu.Unlock()
		return nil, ErrSessionChanged
	}
	if session.primary {
		primaryRefreshed, err := s.store.RefreshPrimary(ctx, session.roleID, session.sessionID)
		if err != nil {
			session.mu.Unlock()
			return nil, err
		}
		if !primaryRefreshed {
			_, unbindErr := s.store.Unbind(ctx, SessionBinding{
				RoleID:       session.roleID,
				SessionID:    session.sessionID,
				ConnectionID: session.connectionID,
				InstanceID:   s.instanceID,
			})
			s.removeLocked(session)
			session.mu.Unlock()
			return nil, errors.Join(ErrSessionChanged, unbindErr)
		}
	}
	session.lastActivity = now
	if s.leaseRefreshInterval > 0 {
		session.nextLeaseRefresh = now.Add(s.leaseRefreshInterval)
	}
	session.mu.Unlock()
	return session, nil
}

func (h SessionHeader) validateConnection() error {
	if h.RoleID <= 0 || h.SessionID <= 0 || h.ConnectionID <= 0 {
		return ErrInvalidHeader
	}
	return nil
}

// logStaleConnection 在释放 session.mu 后记录被栅栏拒绝的新旧连接身份。
func (s *Server) logStaleConnection(ctx context.Context, requested, current SessionHeader, err error) {
	if !errors.Is(err, ErrSessionChanged) {
		return
	}
	txlog.DebugContext(ctx, "msgq stale connection rejected",
		"roleID", requested.RoleID,
		"sessionID", requested.SessionID,
		"connectionID", requested.ConnectionID,
		"currentSessionID", current.SessionID,
		"currentConnectionID", current.ConnectionID,
		"instanceID", s.instanceID,
	)
}

type mailboxSession struct {
	roleID           int64
	sessionID        int64
	connectionID     int64
	primary          bool
	upMsgSeq         int64
	active           bool
	lastActivity     time.Time
	nextLeaseRefresh time.Time
	notify           chan struct{}
	downMsgs         downMsgQueue
	topics           map[string]struct{}
	mu               sync.Mutex
}

func newMailboxSession(header SessionHeader, primary bool) *mailboxSession {
	return &mailboxSession{
		roleID:       header.RoleID,
		sessionID:    header.SessionID,
		connectionID: header.ConnectionID,
		primary:      primary,
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
	if s.roleID != header.RoleID || s.sessionID != header.SessionID || s.connectionID != header.ConnectionID {
		return ErrSessionChanged
	}
	return nil
}

// checkConnection 必须在持有 s.mu 时调用；发生错误时返回当前身份供解锁后记录。
func (s *mailboxSession) checkConnection(header SessionHeader) (SessionHeader, error) {
	err := s.requireConnection(header)
	if err == nil {
		return SessionHeader{}, nil
	}
	return s.header(), err
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

type downMsgEntry struct {
	seq       int64
	payload   []byte
	createdAt time.Time
}

type downMsgQueue struct {
	entries      []downMsgEntry
	head         int
	sent         int
	nextSeq      int64
	sentSeq      int64
	confirmedSeq int64
	bytes        int
}

func (q *downMsgQueue) enqueue(payload []byte) int64 {
	q.nextSeq++
	q.entries = append(q.entries, downMsgEntry{seq: q.nextSeq, payload: payload, createdAt: time.Now()})
	q.bytes += len(payload)
	return q.nextSeq
}

func (q *downMsgQueue) dequeue() (downMsgEntry, bool) {
	if q.sent >= len(q.entries) {
		return downMsgEntry{}, false
	}
	entry := q.entries[q.sent]
	q.sent++
	q.sentSeq = entry.seq
	return entry, true
}

func (q *downMsgQueue) dequeueSome(max int) []downMsgEntry {
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

func (q *downMsgQueue) resetSent() {
	q.sent = q.head
}

func (q *downMsgQueue) validateAck(ack int64) error {
	if ack < q.confirmedSeq || ack > q.sentSeq {
		return ErrDownMsgAcknowledgement
	}
	return nil
}

func (q *downMsgQueue) confirm(ack int64) error {
	if err := q.validateAck(ack); err != nil {
		return err
	}
	q.confirmedSeq = ack
	cut := q.head
	for cut < len(q.entries) && q.entries[cut].seq <= ack {
		q.bytes -= len(q.entries[cut].payload)
		q.entries[cut] = downMsgEntry{}
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

func (q *downMsgQueue) len() int {
	return len(q.entries) - q.head
}

func (q *downMsgQueue) exceeds(maxMessages, maxBytes int, payload []byte) bool {
	return q.len() >= maxMessages || len(payload) > maxBytes-q.bytes
}

func (q *downMsgQueue) expired(now time.Time, ttl time.Duration) bool {
	return q.len() > 0 && now.Sub(q.entries[q.head].createdAt) >= ttl
}

func (q *downMsgQueue) compactStorage() {
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
	entries := make([]downMsgEntry, pending, pending*2)
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
