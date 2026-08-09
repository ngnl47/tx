package tests

import (
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestEtcdConfigNormalizesInstancePrefix(t *testing.T) {
	cfg, err := (discovery.EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		Prefix:    " tx/discovery/ ",
		LeaseTTL:  10 * time.Second,
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if cfg.Prefix != "/tx/discovery/instances" {
		t.Fatalf("Normalize() prefix = %q, want /tx/discovery/instances", cfg.Prefix)
	}
}

func TestEtcdConfigRejectsInvalidLeaseTTL(t *testing.T) {
	_, err := (discovery.EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		LeaseTTL:  time.Second / 2,
	}).Normalize()
	if err == nil {
		t.Fatal("Normalize() error = nil, want invalid lease TTL error")
	}
}

func TestEtcdConfigRejectsInstanceIDWithPathSeparator(t *testing.T) {
	cfg, err := (discovery.EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		LeaseTTL:  10 * time.Second,
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if err := cfg.ValidateInstance(discovery.Instance{
		ID:       "battle/1",
		Services: []string{"battle"},
		Endpoint: "10.0.0.1:9000",
		Status:   discovery.StatusRunning,
	}); err == nil {
		t.Fatal("ValidateInstance() error = nil, want invalid instance ID error")
	}
}

func TestNewEtcdRegistryWithClientRejectsNilClient(t *testing.T) {
	cfg, err := (discovery.EtcdConfig{
		Endpoints: []string{"127.0.0.1:2379"},
		LeaseTTL:  10 * time.Second,
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if _, err := discovery.NewEtcdRegistryWithClient(nil, cfg); err == nil {
		t.Fatal("NewEtcdRegistryWithClient() error = nil, want nil client error")
	}
}
