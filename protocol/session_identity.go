package protocol

import "errors"

// SessionPrincipal 是认证适配器交给框架的会话主体。
// Claims 保存项目编码的可信身份，使用不可变字符串承载字节，连接内复用。
// 框架不解释 Claims，也不从中决定账号、权限或业务路由。
type SessionPrincipal struct {
	RoleID    int64
	SessionID int64
	Claims    string
}

// SessionIdentity 为可信主体附加本次物理连接编号。
type SessionIdentity struct {
	SessionPrincipal
	ConnectionID int64
}

// Validate 仅检查邮箱与连接运行所需的编号；业务身份由项目校验。
func (identity SessionIdentity) Validate() error {
	if identity.RoleID <= 0 {
		return errors.New("session identity: role ID must be positive")
	}
	if identity.SessionID <= 0 {
		return errors.New("session identity: session ID must be positive")
	}
	if identity.ConnectionID <= 0 {
		return errors.New("session identity: connection ID must be positive")
	}
	return nil
}
