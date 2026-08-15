// Package msgq 提供客户端会话邮箱的路由与状态基础能力。
package msgq

import (
	"context"
	"errors"
	"strings"

	"github.com/ngnl47/tx/modules/discovery"
)

var (
	ErrDirectoryNil            = errors.New("msgq: service directory is nil")
	ErrSessionStoreNil         = errors.New("msgq: session store is nil")
	ErrInvalidSession          = errors.New("msgq: invalid session binding")
	ErrSessionOffline          = errors.New("msgq: session is offline")
	ErrSessionChanged          = errors.New("msgq: session has changed")
	ErrRouteUnavailable        = errors.New("msgq: session instance is unavailable")
	ErrServerNil               = errors.New("msgq: server is nil")
	ErrInvalidServer           = errors.New("msgq: invalid server options")
	ErrInvalidHeader           = errors.New("msgq: invalid session header")
	ErrInboundSequence         = errors.New("msgq: inbound sequence is invalid")
	ErrOutboundAcknowledgement = errors.New("msgq: outbound acknowledgement is invalid")
	ErrOutboundOverflow        = errors.New("msgq: outbound queue is full")
	ErrInvalidTopic            = errors.New("msgq: invalid topic")
	ErrLifecycleNil            = errors.New("msgq: lifecycle context is nil")
	ErrReaperRunning           = errors.New("msgq: reaper is already running")
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

// SessionStore 持久化角色到实例的短期会话绑定。
type SessionStore interface {
	Bind(ctx context.Context, binding SessionBinding) (previous SessionBinding, replaced bool, err error)
	Find(ctx context.Context, roleID int64) (binding SessionBinding, found bool, err error)
	Refresh(ctx context.Context, binding SessionBinding) (refreshed bool, err error)
	Unbind(ctx context.Context, binding SessionBinding) (removed bool, err error)
}

// SessionTarget 是一次会话投递所需的绑定和目标实例。
type SessionTarget struct {
	Binding  SessionBinding
	Instance discovery.Instance
}

// SessionRouter 将角色会话解析为唯一的消息服务归属实例。
type SessionRouter struct {
	directory *discovery.ServiceDirectory
	store     SessionStore
}

// NewSessionRouter 创建使用实例目录和绑定存储的会话路由器。
func NewSessionRouter(directory *discovery.ServiceDirectory, store SessionStore) (*SessionRouter, error) {
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if store == nil {
		return nil, ErrSessionStoreNil
	}
	return &SessionRouter{directory: directory, store: store}, nil
}

// Resolve 解析当前会话归属。expectedSessionID 为零时接受当前任意会话。
func (r *SessionRouter) Resolve(ctx context.Context, roleID, expectedSessionID int64) (SessionTarget, error) {
	if r == nil || r.directory == nil {
		return SessionTarget{}, ErrDirectoryNil
	}
	if r.store == nil {
		return SessionTarget{}, ErrSessionStoreNil
	}
	if roleID <= 0 || expectedSessionID < 0 {
		return SessionTarget{}, ErrInvalidSession
	}

	binding, found, err := r.store.Find(ctx, roleID)
	if err != nil {
		return SessionTarget{}, err
	}
	if !found || binding.Validate() != nil || binding.RoleID != roleID {
		return SessionTarget{}, ErrSessionOffline
	}
	if expectedSessionID != 0 && binding.SessionID != expectedSessionID {
		return SessionTarget{}, ErrSessionChanged
	}
	instance, found := r.directory.ResolveID(binding.InstanceID)
	if !found || !providesService(instance, "msgq") {
		return SessionTarget{}, ErrRouteUnavailable
	}
	refreshed, err := r.store.Refresh(ctx, binding)
	if err != nil {
		return SessionTarget{}, err
	}
	if !refreshed {
		return SessionTarget{}, ErrSessionChanged
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
