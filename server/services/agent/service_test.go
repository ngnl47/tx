package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	txserver "github.com/ngnl47/tx/server"
	msgqservice "github.com/ngnl47/tx/server/services/msgq"
)

func TestServiceStartsWithLocalMsgQAndFrameworkLifecycle(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	rpcHost, err := txrpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHost returned error: %v", err)
	}
	mailbox, err := msgqservice.New(msgqservice.Options{
		InstanceID:         "msgq-test",
		Store:              newMemorySessionStore(),
		MaxBacklogMessages: 8,
		OfflineTimeout:     time.Minute,
		SweepInterval:      time.Second,
	})
	if err != nil {
		t.Fatalf("new msgq service: %v", err)
	}
	service, err := New(Options{
		TCPListen:     "127.0.0.1:0",
		MaxFrameBytes: 1024,
		FetchBatch:    1,
		Identity: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 1001, SessionID: 1}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) error { return nil }),
		LocalMsgQ:  mailbox,
	})
	if err != nil {
		t.Fatalf("new agent service: %v", err)
	}
	if err := mailbox.Start(&txserver.Runtime{Lifecycle: root, RPC: rpcHost}); err != nil {
		t.Fatalf("start msgq service: %v", err)
	}
	if err := service.Start(&txserver.Runtime{Lifecycle: root, Ingress: lifecycle.NewIngress(root)}); err != nil {
		t.Fatalf("start agent service: %v", err)
	}
	if service.TCPAddress() == "" {
		t.Fatal("TCP address is empty")
	}
	if err := rpcHost.Start(root); err != nil {
		t.Fatalf("start RPC host: %v", err)
	}
	t.Cleanup(func() {
		_ = service.Stop(context.Background())
		_ = mailbox.Stop(context.Background())
		_ = rpcHost.Stop(context.Background())
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
}

func TestRemoteAgentDeclaresDiscoveryCapability(t *testing.T) {
	service := &Service{options: Options{}}
	want := txserver.CapabilityStorage | txserver.CapabilityDiscovery
	if got := service.Spec().Requires; got != want {
		t.Fatalf("capabilities = %d, want %d", got, want)
	}
}

type memorySessionStore struct {
	mu       sync.Mutex
	bindings map[int64]txmsgq.SessionBinding
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{bindings: make(map[int64]txmsgq.SessionBinding)}
}

func (store *memorySessionStore) Bind(_ context.Context, binding txmsgq.SessionBinding) (txmsgq.SessionBinding, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	previous, exists := store.bindings[binding.RoleID]
	store.bindings[binding.RoleID] = binding
	return previous, exists, nil
}

func (store *memorySessionStore) Find(_ context.Context, roleID int64) (txmsgq.SessionBinding, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	binding, exists := store.bindings[roleID]
	return binding, exists, nil
}

func (store *memorySessionStore) Refresh(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.bindings[binding.RoleID]
	return exists && current == binding, nil
}

func (store *memorySessionStore) Unbind(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.bindings[binding.RoleID]
	if !exists || current != binding {
		return false, nil
	}
	delete(store.bindings, binding.RoleID)
	return true, nil
}
