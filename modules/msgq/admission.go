package msgq

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrSessionOccupied   = errors.New("msgq: role already has a session")
	ErrSessionAdmission  = errors.New("msgq: session admission is no longer valid")
	ErrAdmissionStoreNil = errors.New("msgq: admission store is unavailable")
)

// SessionPolicy 由应用完成授权后选择；框架不识别玩家或 GM 身份。
type SessionPolicy uint8

const (
	RejectExisting SessionPolicy = iota
	ReplaceExisting
)

// SessionAdmission 是验票后交给 MsgQ 的接入资格。TicketOrder 独立于不透明的 SessionID。
// ExpiresAt 只约束首次接入；同一 Session 恢复仍受当前许可及邮箱有效性约束。
type SessionAdmission struct {
	Policy      SessionPolicy
	TicketOrder int64
	ExpiresAt   time.Time
}

// AdmissionRecord 保存当前许可、已接纳票据水位及精确流接入。
// SessionID 为零表示逻辑退出，TicketOrder 仍保留，防止旧票据重新建立邮箱。
// Attached 只有 Agent 明确确认关闭准入后才清除，流断开本身不构成确认。
type AdmissionRecord struct {
	SessionID   int64
	TicketOrder int64
	ExpiresAt   time.Time
	Ready       bool
	Header      SessionHeader
	Attached    bool
}

// AdmissionStore 以单角色 CAS 保存接入许可，独立于角色归属租约。
type AdmissionStore interface {
	LoadAdmission(context.Context, int64) (AdmissionRecord, error)
	CompareAdmission(context.Context, int64, AdmissionRecord, AdmissionRecord) (bool, error)
}

// SessionControlClient 提供精确逻辑退出和只读观察，不授予连接关闭权。
type SessionControlClient interface {
	TerminateLogicalSession(context.Context, int64, int64) error
	InspectSession(context.Context, int64) (AdmissionRecord, error)
}

// roleAdmission 仅串行化同角色低频接入，不阻塞共享流上的 ACK 和其他角色。
type roleAdmission struct {
	turn        chan struct{}
	users       int
	attachments map[SessionHeader]*mailboxAttachment
}

type mailboxAttachment struct {
	restore  bool
	header   SessionHeader
	revoke   func(context.Context) error
	detached chan struct{}
	once     sync.Once
}

func (s *Server) beginAdmission(ctx context.Context, roleID int64) (func(), error) {
	s.admissionMu.Lock()
	if s.stopped.Load() {
		s.admissionMu.Unlock()
		return nil, ErrSessionOffline
	}
	if s.admissions == nil {
		s.admissions = make(map[int64]*roleAdmission)
	}
	role := s.admissions[roleID]
	if role == nil {
		role = &roleAdmission{turn: make(chan struct{}, 1), attachments: make(map[SessionHeader]*mailboxAttachment)}
		s.admissions[roleID] = role
	}
	role.users++
	s.admissionUsers++
	if s.admissionUsers == 1 {
		s.admissionIdle = make(chan struct{})
	}
	s.admissionMu.Unlock()
	release := func() {
		s.admissionMu.Lock()
		role.users--
		s.admissionUsers--
		if role.users == 0 && len(role.attachments) == 0 {
			delete(s.admissions, roleID)
		}
		if s.admissionUsers == 0 {
			close(s.admissionIdle)
		}
		s.admissionMu.Unlock()
	}
	select {
	case role.turn <- struct{}{}:
		return func() { <-role.turn; release() }, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

func (s *Server) admissionBusy(roleID int64) bool {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	role := s.admissions[roleID]
	return role != nil && (role.users > 0 || len(role.attachments) > 0)
}

func (s *Server) admissionStore() (AdmissionStore, error) {
	store, ok := s.store.(AdmissionStore)
	if !ok {
		return nil, ErrAdmissionStoreNil
	}
	return store, nil
}
func (s *Server) loadAdmission(ctx context.Context, roleID int64) (AdmissionRecord, error) {
	store, err := s.admissionStore()
	if err != nil {
		return AdmissionRecord{}, err
	}
	return store.LoadAdmission(ctx, roleID)
}
func (s *Server) saveAdmission(ctx context.Context, roleID int64, old, next AdmissionRecord) error {
	store, err := s.admissionStore()
	if err != nil {
		return err
	}
	changed, err := store.CompareAdmission(ctx, roleID, old, next)
	if err != nil {
		return err
	}
	if !changed {
		return ErrSessionAdmission
	}
	return nil
}

func (s *Server) openAttachment(ctx context.Context, request InitRequest, attachment *mailboxAttachment) error {
	if request.Header.validateConnection() != nil {
		return ErrInvalidHeader
	}
	if err := s.checkStartup(); err != nil {
		return err
	}
	finish, err := s.beginAdmission(ctx, request.Header.RoleID)
	if err != nil {
		return err
	}
	defer finish()
	if err := ctx.Err(); err != nil {
		return err
	}
	lock := &s.initLocks[uint64(request.Header.RoleID)%initLockShards]
	lock.Lock()
	err = s.ensureRole(ctx, request.Header.RoleID)
	lock.Unlock()
	if err != nil {
		return err
	}
	if request.Admission != nil {
		if err = s.admitAttachment(ctx, request); err != nil {
			return err
		}
	} else if store, ok := s.store.(AdmissionStore); ok {
		record, loadErr := store.LoadAdmission(ctx, request.Header.RoleID)
		if loadErr != nil {
			return loadErr
		}
		if record.SessionID != 0 || record.Attached {
			return ErrSessionAdmission
		}
	}
	if err = s.initMailbox(ctx, request); err != nil {
		return err
	}
	s.admissionMu.Lock()
	s.admissions[request.Header.RoleID].attachments[request.Header] = attachment
	s.admissionMu.Unlock()
	if request.Admission != nil {
		// 旧端点此时已关闭，不再与 detach 争用这次持久更新。
		old, loadErr := s.loadAdmission(ctx, request.Header.RoleID)
		if loadErr != nil {
			return loadErr
		}
		next := old
		next.Header = request.Header
		next.Ready = true
		next.Attached = true
		if err = s.saveAdmission(ctx, request.Header.RoleID, old, next); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) admitAttachment(ctx context.Context, request InitRequest) error {
	admission := request.Admission
	if admission.TicketOrder <= 0 || (admission.Policy != RejectExisting && admission.Policy != ReplaceExisting) {
		return ErrInvalidSession
	}
	old, err := s.loadAdmission(ctx, request.Header.RoleID)
	if err != nil {
		return err
	}
	if request.Restore {
		if old.SessionID != request.Header.SessionID || !old.Ready || old.TicketOrder != admission.TicketOrder {
			return ErrSessionAdmission
		}
	} else {
		if !time.Now().Before(admission.ExpiresAt) || admission.TicketOrder <= old.TicketOrder || old.SessionID == request.Header.SessionID {
			return ErrSessionAdmission
		}
		occupied := old.Attached
		if old.SessionID != 0 {
			binding, found, findErr := s.store.Find(ctx, old.SessionID)
			if findErr != nil {
				return findErr
			}
			occupied = occupied || (found && binding.InstanceID == s.instanceID)
		}
		if admission.Policy == RejectExisting && occupied {
			return ErrSessionOccupied
		}
		// 只有接纳的票据推进水位；被拒绝的非强制登录不影响原会话。
		next := old
		next.SessionID = request.Header.SessionID
		next.TicketOrder = admission.TicketOrder
		next.ExpiresAt = admission.ExpiresAt
		next.Ready = false
		if err = s.saveAdmission(ctx, request.Header.RoleID, old, next); err != nil {
			return err
		}
	}
	if old.Attached {
		if err = s.revokeAttachment(ctx, old.Header); err != nil {
			return err
		}
	}
	if !request.Restore && old.Header.SessionID != 0 {
		if err = s.removeAdmissionMailbox(ctx, old.Header); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) revokeAttachment(ctx context.Context, header SessionHeader) error {
	s.admissionMu.Lock()
	var attachment *mailboxAttachment
	if role := s.admissions[header.RoleID]; role != nil {
		attachment = role.attachments[header]
	}
	s.admissionMu.Unlock()
	if attachment == nil {
		return ErrRouteUnavailable
	}
	select {
	case <-attachment.detached:
		return nil
	default:
	}
	return attachment.revoke(ctx)
}

// detachAttachment 只接收 Agent 已关闭本地准入的精确确认，不能由流 EOF 代替。
func (s *Server) detachAttachment(ctx context.Context, header SessionHeader) error {
	if store, ok := s.store.(AdmissionStore); ok {
		for {
			old, err := store.LoadAdmission(ctx, header.RoleID)
			if err != nil {
				return err
			}
			if old.Header != header || !old.Attached {
				break
			}
			next := old
			next.Attached = false
			changed, err := store.CompareAdmission(ctx, header.RoleID, old, next)
			if err != nil {
				return err
			}
			if changed {
				break
			}
			if err = ctx.Err(); err != nil {
				return err
			}
		}
	}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if role := s.admissions[header.RoleID]; role != nil {
		if attachment := role.attachments[header]; attachment != nil {
			attachment.once.Do(func() { close(attachment.detached) })
			delete(role.attachments, header)
		}
		if role.users == 0 && len(role.attachments) == 0 {
			delete(s.admissions, header.RoleID)
		}
	}
	return nil
}

func (s *Server) removeAdmissionMailbox(ctx context.Context, header SessionHeader) error {
	session, found := s.lookup(header.SessionID)
	if !found {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.header() != header {
		return ErrSessionChanged
	}
	_, err := s.terminateSessionLocked(ctx, session, SessionBinding{RoleID: header.RoleID, SessionID: header.SessionID, ConnectionID: session.connectionID, InstanceID: s.instanceID})
	return err
}

// TerminateLogicalSession 只结束指定 SID；旧退出请求不能关闭较新的会话。
func (s *Server) TerminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	if roleID <= 0 || sessionID <= 0 {
		return ErrInvalidSession
	}
	finish, err := s.beginAdmission(ctx, roleID)
	if err != nil {
		return err
	}
	defer finish()
	return s.terminateLogicalSession(ctx, roleID, sessionID)
}

// terminateLogicalSession 调用方须持有角色准入，确保接入校验与终止之间不会切换连接。
func (s *Server) terminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	old, err := s.loadAdmission(ctx, roleID)
	if err != nil {
		return err
	}
	if old.SessionID != sessionID {
		return nil
	}
	if old.Attached {
		if err = s.revokeAttachment(ctx, old.Header); err != nil {
			return err
		}
	}
	if err = s.removeAdmissionMailbox(ctx, old.Header); err != nil {
		return err
	}
	old, err = s.loadAdmission(ctx, roleID)
	if err != nil {
		return err
	}
	if old.SessionID != sessionID {
		return nil
	}
	next := old
	next.SessionID = 0
	next.Header = SessionHeader{}
	next.Ready = false
	next.Attached = false
	return s.saveAdmission(ctx, roleID, old, next)
}

// InspectSession 只读当前许可；不创建角色归属或会话。
func (s *Server) InspectSession(ctx context.Context, roleID int64) (AdmissionRecord, error) {
	if roleID <= 0 {
		return AdmissionRecord{}, ErrInvalidSession
	}
	return s.loadAdmission(ctx, roleID)
}

// stopAttachments 关闭准入后等待在途接入，再有界并发确认 Agent 已关门；失败保留资源以便重试。
func (s *Server) stopAttachments(ctx context.Context) error {
	s.admissionMu.Lock()
	s.stopped.Store(true)
	idle := s.admissionIdle
	users := s.admissionUsers
	s.admissionMu.Unlock()
	if users > 0 {
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.admissionMu.Lock()
	var attachments []*mailboxAttachment
	for _, role := range s.admissions {
		for _, attachment := range role.attachments {
			attachments = append(attachments, attachment)
		}
	}
	s.admissionMu.Unlock()
	// 关闭确认不能按会话串行累计网络往返；限制并发以保护共享控制队列。
	group, controlCtx := errgroup.WithContext(ctx)
	group.SetLimit(64)
	for _, attachment := range attachments {
		group.Go(func() error { return attachment.revoke(controlCtx) })
	}
	return group.Wait()
}

// detachFailedAttachment 只收尾本次未返回成功的候选，不能撤销已有的同名接入。
func (s *Server) detachFailedAttachment(ctx context.Context, attachment *mailboxAttachment) error {
	s.admissionMu.Lock()
	role := s.admissions[attachment.header.RoleID]
	registered := role != nil && role.attachments[attachment.header] == attachment
	s.admissionMu.Unlock()
	if !registered {
		return nil
	}
	if err := s.detachAttachment(ctx, attachment.header); err != nil {
		return err
	}
	if attachment.restore {
		return nil
	}
	return s.removeAdmissionMailbox(ctx, attachment.header)
}
