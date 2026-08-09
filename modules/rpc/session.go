package rpc

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/discovery"
)

var (
	ErrSessionStoreNil = errors.New("rpc: session store is nil")
	ErrSessionOffline  = errors.New("rpc: session is offline")
	ErrSessionChanged  = errors.New("rpc: session has changed")
)

// SessionBinding 记录一个角色会话当前归属的消息服务实例。
type SessionBinding struct {
	RoleID     int64
	SessionID  int64
	InstanceID string
}

// Validate 校验绑定能否安全标识一个活动会话。
func (b SessionBinding) Validate() error {
	if b.RoleID <= 0 || b.SessionID <= 0 || !validRouteValue(b.InstanceID) {
		return ErrInvalidRoute
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
func (r *SessionRouter) Resolve(ctx context.Context, roleID, expectedSessionID int64) (Route, error) {
	if r == nil || r.directory == nil {
		return Route{}, ErrDirectoryNil
	}
	if r.store == nil {
		return Route{}, ErrSessionStoreNil
	}
	if roleID <= 0 || expectedSessionID < 0 {
		return Route{}, ErrInvalidRoute
	}

	binding, found, err := r.store.Find(ctx, roleID)
	if err != nil {
		return Route{}, err
	}
	if !found {
		return Route{}, ErrSessionOffline
	}
	if err := binding.Validate(); err != nil || binding.RoleID != roleID {
		return Route{}, ErrSessionOffline
	}
	if expectedSessionID != 0 && binding.SessionID != expectedSessionID {
		return Route{}, ErrSessionChanged
	}
	if _, err := Resolve(r.directory, InstanceRoute(binding.InstanceID)); err != nil {
		return Route{}, err
	}
	refreshed, err := r.store.Refresh(ctx, binding)
	if err != nil {
		return Route{}, err
	}
	if !refreshed {
		return Route{}, ErrSessionChanged
	}

	return Route{
		Kind:       RouteSession,
		InstanceID: binding.InstanceID,
		RoleID:     binding.RoleID,
		SessionID:  binding.SessionID,
	}, nil
}
