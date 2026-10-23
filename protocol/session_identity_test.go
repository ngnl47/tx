package protocol

import "testing"

func TestSessionIdentityValidate(t *testing.T) {
	valid := SessionIdentity{SessionPrincipal: SessionPrincipal{RoleID: 7, SessionID: 9, Claims: "业务自定义身份"}, ConnectionID: 11}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"role", "session", "connection"} {
		broken := valid
		switch field {
		case "role":
			broken.RoleID = 0
		case "session":
			broken.SessionID = 0
		case "connection":
			broken.ConnectionID = 0
		}
		if broken.Validate() == nil {
			t.Fatalf("缺失 %s 未拒绝", field)
		}
	}
	valid.Claims = ""
	if err := valid.Validate(); err != nil {
		t.Fatalf("框架不要求业务载荷: %v", err)
	}
}
