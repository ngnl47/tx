package framework

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestDiscoveryCModDoesNotRequireManualInstanceID(t *testing.T) {
	server := &Server{config: Config{AppID: "txgame", Discovery: validDiscoveryConfig()}}
	mod := server.discoveryCMod(RunOptions{})
	validator, ok := mod.(CModValidator)
	if !ok {
		t.Fatal("discovery CMod does not implement CModValidator")
	}
	if err := validator.Validate(newCModContext(server)); err != nil {
		t.Fatalf("validate = %v", err)
	}
}

func validDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{Etcd: discovery.EtcdConfig{
		Endpoints:   []string{"127.0.0.1:2379"},
		LeaseTTL:    time.Second,
		DialTimeout: time.Second,
	}}
}

func TestDiscoveryAutomaticIdentityWithEtcd(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	prefix := fmt.Sprintf("/tx/tests/framework-identity-%d", time.Now().UnixNano())
	first := newDiscoveryIdentityTestServer(t, endpoint, prefix, "127.0.0.1:9101", testService{spec: Spec{Name: "game", Requires: []CModID{CModDiscovery}}})
	second := newDiscoveryIdentityTestServer(t, endpoint, prefix, "127.0.0.1:9102", testService{spec: Spec{Name: "game", Requires: []CModID{CModDiscovery}}})
	if err := first.Start(t.Context(), RunOptions{Services: "game", InstanceEndpoint: "127.0.0.1:9101"}); err != nil {
		t.Fatalf("start first server: %v", err)
	}
	t.Cleanup(func() { _ = first.Stop(context.Background()) })
	if err := second.Start(t.Context(), RunOptions{Services: "game", InstanceEndpoint: "127.0.0.1:9102"}); err != nil {
		t.Fatalf("start second server: %v", err)
	}
	t.Cleanup(func() { _ = second.Stop(context.Background()) })
	firstID := first.Context().Instance().ID
	secondID := second.Context().Instance().ID
	assertGeneratedInstanceIDShape(t, firstID, "txgame")
	assertGeneratedInstanceIDShape(t, secondID, "txgame")
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatalf("automatic InstanceID values = %q, %q", firstID, secondID)
	}
}

func TestDiscoveryAutomaticIdentityIsSharedByServicesWithEtcd(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	var firstID, secondID string
	server := newDiscoveryIdentityTestServer(t, endpoint, fmt.Sprintf("/tx/tests/framework-shared-identity-%d", time.Now().UnixNano()), "127.0.0.1:9103",
		testService{
			spec: Spec{Name: "game", Requires: []CModID{CModDiscovery}},
			start: func(ctx Context) error {
				firstID = ctx.Instance().ID
				return nil
			},
		},
		testService{
			spec: Spec{Name: "login", Requires: []CModID{CModDiscovery}},
			start: func(ctx Context) error {
				secondID = ctx.Instance().ID
				return nil
			},
		},
	)
	if err := server.Start(t.Context(), RunOptions{Services: "*", InstanceEndpoint: "127.0.0.1:9103"}); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	assertGeneratedInstanceIDShape(t, firstID, "txgame")
	assertGeneratedInstanceIDShape(t, secondID, "txgame")
	if firstID == "" || secondID == "" || firstID != secondID {
		t.Fatalf("service InstanceID values = %q, %q", firstID, secondID)
	}
}

func TestDiscoveryAutomaticIdentityBindsLogicalSlotWithEtcd(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	server := newDiscoveryIdentityTestServer(
		t,
		endpoint,
		fmt.Sprintf("/tx/tests/framework-logical-slot-%d", time.Now().UnixNano()),
		"127.0.0.1:9104",
		testService{spec: Spec{Name: "game", Requires: []CModID{CModDiscovery}}},
	)
	if err := server.Start(t.Context(), RunOptions{
		Services:         "game",
		InstanceEndpoint: "127.0.0.1:9104",
		LogicalSlot:      "game-1",
	}); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	assertGeneratedInstanceIDShape(t, server.Context().Instance().ID, "txgame")
	slot, ok := server.Context().LogicalSlot()
	if !ok || slot.RuntimeInstanceID == "" || slot.RuntimeInstanceID != server.Context().Instance().ID {
		t.Fatalf("logical slot = %#v, InstanceID = %q", slot, server.Context().Instance().ID)
	}
}

func assertGeneratedInstanceIDShape(t *testing.T, value, appID string) {
	t.Helper()
	var workSlot uint16
	var modRevision int64
	matched, err := fmt.Sscanf(value, appID+"-%d-%d", &workSlot, &modRevision)
	if err != nil || matched != 2 || workSlot == 0 || workSlot > discovery.WorkSlotCount || modRevision <= 0 ||
		value != fmt.Sprintf("%s-%d-%d", appID, workSlot, modRevision) {
		t.Fatalf("InstanceID = %q, want %s-<workSlot>-<ModRevision>", value, appID)
	}
}

func newDiscoveryIdentityTestServer(t *testing.T, endpoint, prefix, instanceEndpoint string, services ...Service) *Server {
	t.Helper()
	server, err := New(
		Config{AppID: "txgame", Discovery: DiscoveryConfig{Etcd: discovery.EtcdConfig{
			Endpoints:   []string{endpoint},
			Prefix:      prefix,
			LeaseTTL:    2 * time.Second,
			DialTimeout: time.Second,
		}}},
		WithService(services...),
	)
	if err != nil {
		t.Fatalf("new server for %s: %v", instanceEndpoint, err)
	}
	return server
}

func TestNewLogicalSlotBindsStableSlotToRuntimeInstance(t *testing.T) {
	slot, err := newLogicalSlot("game-1", "pod-uid-a")
	if err != nil {
		t.Fatalf("NewLogicalSlot returned error: %v", err)
	}
	if slot.ID != "game-1" || slot.RuntimeInstanceID != "pod-uid-a" {
		t.Fatalf("slot = %#v", slot)
	}
	if _, err := newLogicalSlot("game/1", "pod-uid-a"); err == nil {
		t.Fatal("NewLogicalSlot accepted invalid slot ID")
	}
	if _, err := newLogicalSlot("game-1", ""); err == nil {
		t.Fatal("NewLogicalSlot accepted empty runtime instance ID")
	}
}
