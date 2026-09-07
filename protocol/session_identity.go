package protocol

import "errors"

// AccessMode 描述会话身份的访问方式。
type AccessMode uint8

const (
	// AccessModeNormal 表示账号正常使用自身角色的会话。
	AccessModeNormal AccessMode = iota
	// AccessModeBorrow 表示借用目标账号的独立会话。
	AccessModeBorrow
)

// SessionPrincipal 表示经过认证的账号和角色主体。
type SessionPrincipal struct {
	AccountID       int64
	BorrowAccountID int64
	ServerID        int64
	RoleID          int64
	SessionID       int64
	AccessMode      AccessMode
	CanCreateRole   bool
	// Primary 由项目鉴权决定是否参与角色主会话投影，不表示独占操作权。
	Primary bool
}

// SessionIdentity 表示可信会话携带的主体和访问方式。
type SessionIdentity struct {
	SessionPrincipal
	ConnectionID int64
}

// Validate 校验会话身份是否满足访问方式的约束。
func (identity SessionIdentity) Validate() error {
	if identity.AccountID <= 0 {
		return errors.New("session identity: account ID must be positive")
	}
	if identity.ServerID <= 0 {
		return errors.New("session identity: server ID must be positive")
	}
	if identity.RoleID <= 0 {
		return errors.New("session identity: role ID must be positive")
	}
	if identity.SessionID <= 0 {
		return errors.New("session identity: session ID must be positive")
	}
	if identity.ConnectionID <= 0 {
		return errors.New("session identity: connection ID must be positive")
	}

	switch identity.AccessMode {
	case AccessModeNormal:
		if identity.BorrowAccountID != 0 {
			return errors.New("session identity: normal mode must not have borrow account ID")
		}
	case AccessModeBorrow:
		if identity.BorrowAccountID <= 0 {
			return errors.New("session identity: borrow mode requires a borrow account ID")
		}
	default:
		return errors.New("session identity: unknown access mode")
	}

	return nil
}
