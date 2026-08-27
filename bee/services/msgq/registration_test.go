package msgq

import (
	"slices"
	"testing"

	"github.com/ngnl47/tx/bee"
)

func TestRegistrationDeclaresMsgQDependencies(t *testing.T) {
	service, err := NewConfigured(func() (Options, error) {
		return Options{}, nil
	})
	if err != nil {
		t.Fatalf("NewConfigured returned error: %v", err)
	}
	registration := service.Registration()
	want := []bee.CModID{bee.CModRPC, bee.CModRedisStorage}
	if registration.Spec.Name != ServiceName || !slices.Equal(registration.Spec.Requires, want) {
		t.Fatalf("registration spec = %#v", registration.Spec)
	}
}
