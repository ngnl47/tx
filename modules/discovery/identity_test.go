package discovery

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRuntimeIdentityUsesUUID(t *testing.T) {
	first, err := newRuntimeIdentity("txgame")
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRuntimeIdentity("txgame")
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []RuntimeIdentity{first, second} {
		if !strings.HasPrefix(identity.InstanceID, "txgame-") {
			t.Fatalf("InstanceID = %q", identity.InstanceID)
		}
		id, err := uuid.Parse(strings.TrimPrefix(identity.InstanceID, "txgame-"))
		if err != nil || id.Version() != 4 {
			t.Fatalf("InstanceID %q 缺少随机 UUID: %v", identity.InstanceID, err)
		}
	}
	if first == second {
		t.Fatalf("重复身份: %v", first)
	}
}

func TestRuntimeIdentityRejectsInvalidAppID(t *testing.T) {
	for _, appID := range []string{"", " txgame", "txgame "} {
		if _, err := newRuntimeIdentity(appID); !errors.Is(err, ErrInvalidRuntimeIdentity) {
			t.Fatalf("newRuntimeIdentity(%q) error = %v, want ErrInvalidRuntimeIdentity", appID, err)
		}
	}
}
