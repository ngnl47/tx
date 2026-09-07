package protocol

import "testing"

func TestSessionIdentityValidate(t *testing.T) {
	validPrincipal := SessionPrincipal{
		AccountID:  1001,
		ServerID:   2001,
		RoleID:     3001,
		SessionID:  4001,
		AccessMode: AccessModeNormal,
	}

	tests := []struct {
		name     string
		identity SessionIdentity
		wantErr  bool
	}{
		{
			name: "接受普通身份",
			identity: SessionIdentity{
				SessionPrincipal: validPrincipal,
				ConnectionID:     5001,
			},
		},
		{
			name: "接受 Borrow 身份",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{
					AccountID:       1001,
					BorrowAccountID: 1002,
					ServerID:        2001,
					RoleID:          3001,
					SessionID:       4001,
					AccessMode:      AccessModeBorrow,
				},
				ConnectionID: 5001,
			},
		},
		{
			name: "拒绝缺失正账号 ID",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{ServerID: 2001, RoleID: 3001, SessionID: 4001, AccessMode: AccessModeNormal},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝缺失正服务器 ID",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, RoleID: 3001, SessionID: 4001, AccessMode: AccessModeNormal},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝缺失正角色 ID",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, ServerID: 2001, SessionID: 4001, AccessMode: AccessModeNormal},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝缺失正会话 ID",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, ServerID: 2001, RoleID: 3001, AccessMode: AccessModeNormal},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝缺失正连接 ID",
			identity: SessionIdentity{
				SessionPrincipal: validPrincipal,
			},
			wantErr: true,
		},
		{
			name: "拒绝未知模式",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, ServerID: 2001, RoleID: 3001, SessionID: 4001, AccessMode: AccessMode(99)},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝普通模式携带 BorrowAccountID",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, BorrowAccountID: 1002, ServerID: 2001, RoleID: 3001, SessionID: 4001, AccessMode: AccessModeNormal},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "拒绝 Borrow 模式缺少目标账号",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, ServerID: 2001, RoleID: 3001, SessionID: 4001, AccessMode: AccessModeBorrow},
				ConnectionID:     5001,
			},
			wantErr: true,
		},
		{
			name: "接受业务授予 Borrow 模式创建角色许可",
			identity: SessionIdentity{
				SessionPrincipal: SessionPrincipal{AccountID: 1001, BorrowAccountID: 1002, ServerID: 2001, RoleID: 3001, SessionID: 4001, AccessMode: AccessModeBorrow, CanCreateRole: true},
				ConnectionID:     5001,
			},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.identity.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, want error: %t", err, test.wantErr)
			}
		})
	}
}
