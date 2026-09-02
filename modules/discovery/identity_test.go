package discovery

import (
	"errors"
	"testing"
)

func TestRuntimeIdentityFormatsInstanceID(t *testing.T) {
	identity, err := newRuntimeIdentity("txgame", 17, 291)
	if err != nil {
		t.Fatalf("newRuntimeIdentity: %v", err)
	}
	want := RuntimeIdentity{
		InstanceID:  "txgame-17-291",
		WorkSlot:    17,
		ModRevision: 291,
	}
	if identity != want {
		t.Fatalf("identity = %#v, want %#v", identity, want)
	}
}

func TestRuntimeIdentityRejectsUnassignedWorkSlot(t *testing.T) {
	_, err := newRuntimeIdentity("txgame", 0, 291)
	if !errors.Is(err, ErrInvalidRuntimeIdentity) {
		t.Fatalf("newRuntimeIdentity error = %v, want ErrInvalidRuntimeIdentity", err)
	}
}

func TestRuntimeIdentityRejectsWorkSlotOutsideBitRange(t *testing.T) {
	_, err := newRuntimeIdentity("txgame", 4096, 291)
	if !errors.Is(err, ErrInvalidRuntimeIdentity) {
		t.Fatalf("newRuntimeIdentity error = %v, want ErrInvalidRuntimeIdentity", err)
	}
}

func TestRuntimeIdentityRejectsMissingAppID(t *testing.T) {
	_, err := newRuntimeIdentity("", 17, 291)
	if !errors.Is(err, ErrInvalidRuntimeIdentity) {
		t.Fatalf("newRuntimeIdentity error = %v, want ErrInvalidRuntimeIdentity", err)
	}
}

func TestRuntimeIdentityRejectsMissingRevision(t *testing.T) {
	_, err := newRuntimeIdentity("txgame", 17, 0)
	if !errors.Is(err, ErrInvalidRuntimeIdentity) {
		t.Fatalf("newRuntimeIdentity error = %v, want ErrInvalidRuntimeIdentity", err)
	}
}

func TestWorkSlotAttemptsWrapWithinBoundedRange(t *testing.T) {
	got := workSlotAttempts(4094)
	if len(got) != WorkSlotCount {
		t.Fatalf("attempt count = %d, want %d", len(got), WorkSlotCount)
	}
	if got[0] != 4094 || got[1] != 4095 || got[2] != 1 {
		t.Fatalf("first attempts = %v, want [4094 4095 1]", got[:3])
	}
	if got[len(got)-1] != 4093 {
		t.Fatalf("last attempt = %d, want 4093", got[len(got)-1])
	}
}
