package discovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdRuntimeReportsIdentityLoss(t *testing.T) {
	registry, raw, _ := newIdentityIntegrationRegistry(t, "identity-loss", 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	registration, err := registry.Register(ctx, "txgame", Instance{
		Services: []string{"login"},
		Endpoint: "127.0.0.1:9000",
		Status:   StatusStarting,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	runtime, err := newRuntime(registry, registration, registry.Slots())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if _, err := raw.Revoke(ctx, clientv3.LeaseID(registration.lease.ID())); err != nil {
		t.Fatalf("revoke runtime lease: %v", err)
	}
	select {
	case <-runtime.Done():
	case <-ctx.Done():
		t.Fatalf("wait Runtime.Done: %v", ctx.Err())
	}
	if err := runtime.Err(); !errors.Is(err, ErrRuntimeIdentityLost) || !errors.Is(err, ErrEtcdLeaseLost) {
		t.Fatalf("Runtime.Err = %v, want identity loss with lease cause", err)
	}
}

func newIdentityIntegrationRegistry(t *testing.T, suffix string, ttl time.Duration) (*EtcdRegistry, *clientv3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	root := fmt.Sprintf("/tx/tests/identity-%s-%d", suffix, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	registry, err := NewEtcdRegistry(ctx, EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      root,
		LeaseTTL:    ttl,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewEtcdRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	raw, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create raw etcd client: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = raw.Delete(cleanupCtx, root+"/", clientv3.WithPrefix())
		_ = raw.Close()
	})
	return registry, raw, root
}

func TestEtcdRegistryBorrowedClientRemainsUsableAfterClose(t *testing.T) {
	owner, _, _ := newIdentityIntegrationRegistry(t, "borrowed-client", 2*time.Second)
	borrowed, err := NewEtcdRegistryWithClient(owner.client, owner.config)
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := owner.client.Probe(ctx); err != nil {
		t.Fatalf("借用的客户端被关闭: %v", err)
	}
}

func TestEtcdRegistrationDoesNotOverwriteExistingInstance(t *testing.T) {
	registry, _, _ := newIdentityIntegrationRegistry(t, "collision", 2*time.Second)
	uuid.SetRand(bytes.NewReader(make([]byte, 32)))
	defer uuid.SetRand(nil)
	descriptor := Instance{Services: []string{"login"}, Endpoint: "127.0.0.1:9000", Status: StatusStarting}
	first, err := registry.Register(t.Context(), "txgame", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	key := registry.instanceKey(first.Identity().InstanceID)
	before, found, err := registry.client.Get(t.Context(), key)
	if err != nil || !found {
		t.Fatalf("read first: %v %v", found, err)
	}
	second, err := registry.Register(t.Context(), "txgame", descriptor)
	if second != nil {
		defer second.Close(context.Background())
	}
	if err == nil {
		t.Fatal("相同 InstanceID 注册未拒绝")
	}
	after, found, err := registry.client.Get(t.Context(), key)
	if err != nil || !found || !bytes.Equal(after.Value, before.Value) || after.LeaseID != before.LeaseID || after.ModRevision != before.ModRevision {
		t.Fatalf("已有成员被修改: before=%+v after=%+v found=%v err=%v", before, after, found, err)
	}
}

func TestEtcdRuntimeRestartChangesIdentityWithoutWorkSlots(t *testing.T) {
	owner, raw, root := newIdentityIntegrationRegistry(t, "restart", 2*time.Second)
	var previous string
	for range 2 {
		runtime, err := NewRuntimeWithClient(t.Context(), owner.client, owner.config, "txgame", Instance{
			Services: []string{"login"}, Endpoint: "127.0.0.1:9000", Status: StatusStarting,
		})
		if err != nil {
			t.Fatal(err)
		}
		id := runtime.Identity().InstanceID
		if id == previous {
			t.Fatalf("重启复用了 InstanceID: %q", id)
		}
		previous = id
		snapshot, err := raw.Get(t.Context(), root+"/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Kvs) != 1 || string(snapshot.Kvs[0].Key) != owner.instanceKey(id) {
			t.Fatalf("启动应只登记成员，不应创建 work slot: %v", snapshot.Kvs)
		}
		if err := runtime.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := owner.client.Probe(t.Context()); err != nil {
			t.Fatalf("Runtime.Close 关闭了借用客户端: %v", err)
		}
	}
}

func TestEtcdRuntimeInitializationFailureKeepsBorrowedClient(t *testing.T) {
	owner, raw, root := newIdentityIntegrationRegistry(t, "init-failure", 2*time.Second)
	runtime, err := NewRuntimeWithClient(t.Context(), owner.client, owner.config, "txgame", Instance{Status: StatusStarting})
	if !errors.Is(err, ErrInvalidInstance) || runtime != nil {
		t.Fatalf("NewRuntimeWithClient = %v, %v", runtime, err)
	}
	if err := owner.client.Probe(t.Context()); err != nil {
		t.Fatalf("启动失败关闭了借用客户端: %v", err)
	}
	snapshot, err := raw.Get(t.Context(), root+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Kvs) != 0 {
		t.Fatalf("启动失败残留注册记录: %v", snapshot.Kvs)
	}
}

func TestEtcdRuntimeCloseClosesOwnedClient(t *testing.T) {
	owner, _, _ := newIdentityIntegrationRegistry(t, "owned-client", 2*time.Second)
	runtime, err := NewRuntime(t.Context(), owner.config, "txgame", Instance{
		Services: []string{"login"}, Endpoint: "127.0.0.1:9000", Status: StatusStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := runtime.registry.(*EtcdRegistry).client
	if err := client.Probe(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := client.Probe(ctx); err == nil {
		t.Fatal("Runtime.Close 未关闭自建客户端")
	}
}
