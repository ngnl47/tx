// Package msgq 提供客户端会话邮箱的路由与状态基础能力。
package msgq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/maypok86/otter/v2"
	"github.com/ngnl47/tx/modules/discovery"
	"golang.org/x/sync/singleflight"
)

var (
	ErrDirectoryNil    = errors.New("msgq: service directory is nil")
	ErrSessionStoreNil = errors.New("msgq: session store is nil")
	ErrInvalidSession  = errors.New("msgq: invalid session binding")
	ErrSessionOffline  = errors.New("msgq: session is offline")
	// ErrRoleBindingChanged 表示角色归属校验失败，尚未向任何邮箱投递，可重新寻址。
	ErrRoleBindingChanged           = fmt.Errorf("msgq: role binding changed: %w", ErrSessionChanged)
	ErrSessionChanged               = errors.New("msgq: session has changed")
	ErrRouteUnavailable             = errors.New("msgq: session instance is unavailable")
	ErrServerNil                    = errors.New("msgq: server is nil")
	ErrInvalidServer                = errors.New("msgq: invalid server options")
	ErrInvalidHeader                = errors.New("msgq: invalid session header")
	ErrUpMsgSequence                = errors.New("msgq: UpMsg sequence is invalid")
	ErrDownMsgAcknowledgement       = errors.New("msgq: DownMsg acknowledgement is invalid")
	ErrDownMsgOverflow              = errors.New("msgq: DownMsg queue is full")
	ErrInvalidTopic                 = errors.New("msgq: invalid topic")
	ErrLifecycleNil                 = errors.New("msgq: lifecycle context is nil")
	ErrDeadSessionTerminatorRunning = errors.New("msgq: dead session terminator is already running")
)

// SessionBinding 记录一个角色会话当前归属的消息服务实例。
type SessionBinding struct {
	RoleID       int64
	SessionID    int64
	ConnectionID int64
	InstanceID   string
}

// Validate 校验绑定能否安全标识一个活动会话。
func (b SessionBinding) Validate() error {
	if b.RoleID <= 0 || b.SessionID <= 0 || b.ConnectionID <= 0 || !validValue(b.InstanceID) {
		return ErrInvalidSession
	}
	return nil
}

// SessionStore 持久化角色归属租约和精确会话绑定。
type SessionStore interface {
	RoleStore
	Create(ctx context.Context, binding SessionBinding) (created bool, err error)
	Rebind(ctx context.Context, expected, replacement SessionBinding) (rebound bool, err error)
	Find(ctx context.Context, sessionID int64) (binding SessionBinding, found bool, err error)
	Refresh(ctx context.Context, binding SessionBinding) (refreshed bool, err error)
	Unbind(ctx context.Context, binding SessionBinding) (removed bool, err error)
}

// SessionTarget 是一次会话投递所需的绑定和目标实例。
type SessionTarget struct {
	Binding  SessionBinding
	Instance discovery.Instance
}

// SessionRouter 将精确会话解析为唯一的消息服务归属实例。
type SessionRouter struct {
	directory discovery.Directory
	store     SessionStore
	mailboxes *otter.Cache[int64, string]
	loads     singleflight.Group
}

// NewSessionRouter 创建会话路由器，maximumSize 限制精确下行的邮箱归属缓存容量。
func NewSessionRouter(directory discovery.Directory, store SessionStore, maximumSize int) (*SessionRouter, error) {
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if store == nil {
		return nil, ErrSessionStoreNil
	}
	if maximumSize <= 0 {
		return nil, ErrInvalidSessionOptions
	}
	cache, err := otter.New(&otter.Options[int64, string]{MaximumSize: maximumSize, InitialCapacity: min(maximumSize, 16)})
	if err != nil {
		return nil, err
	}
	return &SessionRouter{directory: directory, store: store, mailboxes: cache}, nil
}

// ResolveMailbox 缓存逻辑 Session 的 MsgQ 实例，不缓存物理 ConnectionID。
// 同一 Session 不跨 MsgQ 迁移；MsgQ 丢失邮箱后须建立新 Session。
func (r *SessionRouter) ResolveMailbox(ctx context.Context, sessionID int64) (discovery.Instance, error) {
	if sessionID <= 0 {
		return discovery.Instance{}, ErrInvalidSession
	}
	if err := ctx.Err(); err != nil {
		return discovery.Instance{}, err
	}
	id, found := r.mailboxes.GetIfPresent(sessionID)
	if !found {
		loaded := r.loads.DoChan(strconv.FormatInt(sessionID, 10), func() (any, error) {
			if current, found := r.mailboxes.GetIfPresent(sessionID); found {
				return current, nil
			}
			target, err := r.ResolveSession(ctx, sessionID)
			if err != nil {
				return nil, err
			}
			r.mailboxes.Set(sessionID, target.Instance.ID)
			return target.Instance.ID, nil
		})
		select {
		case <-ctx.Done():
			return discovery.Instance{}, ctx.Err()
		case result := <-loaded:
			if result.Err != nil {
				return discovery.Instance{}, result.Err
			}
			id = result.Val.(string)
		}
	}
	instance, found := r.directory.LookupInstance(id)
	if !found || instance.Status != discovery.StatusRunning || !providesService(instance, "msgq") {
		r.invalidateMailbox(sessionID, id)
		return discovery.Instance{}, ErrRouteUnavailable
	}
	return instance, nil
}

func (r *SessionRouter) invalidateMailbox(sessionID int64, instanceID string) {
	r.mailboxes.Compute(sessionID, func(current string, found bool) (string, otter.ComputeOp) {
		if found && current == instanceID {
			return current, otter.InvalidateOp
		}
		return current, otter.CancelOp
	})
}

// ResolveSession 按 SessionID 只读解析精确会话归属。
func (r *SessionRouter) ResolveSession(ctx context.Context, sessionID int64) (SessionTarget, error) {
	if r == nil || r.directory == nil {
		return SessionTarget{}, ErrDirectoryNil
	}
	if r.store == nil {
		return SessionTarget{}, ErrSessionStoreNil
	}
	if sessionID <= 0 {
		return SessionTarget{}, ErrInvalidSession
	}
	binding, found, err := r.store.Find(ctx, sessionID)
	if err != nil {
		return SessionTarget{}, err
	}
	if !found || binding.Validate() != nil || binding.SessionID != sessionID {
		return SessionTarget{}, ErrSessionOffline
	}
	return r.resolveBinding(binding)
}

func (r *SessionRouter) resolveBinding(binding SessionBinding) (SessionTarget, error) {
	instance, found := r.directory.LookupInstance(binding.InstanceID)
	if !found || !providesService(instance, "msgq") {
		return SessionTarget{}, ErrRouteUnavailable
	}
	return SessionTarget{Binding: binding, Instance: instance}, nil
}

func validValue(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func providesService(instance discovery.Instance, service string) bool {
	for _, value := range instance.Services {
		if value == service {
			return true
		}
	}
	return false
}
