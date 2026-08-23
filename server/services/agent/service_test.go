package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txserver "github.com/ngnl47/tx/server"
	"github.com/ngnl47/tx/server/internal/servertest"
)

func TestServiceStartsWithLocalMsgQAndFrameworkLifecycle(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{
		InstanceID:         "msgq-test",
		Store:              newMemorySessionStore(),
		MaxBacklogMessages: 8,
		OfflineTimeout:     time.Minute,
		SweepInterval:      time.Second,
	})
	if err != nil {
		t.Fatalf("new msgq runtime: %v", err)
	}
	client, err := txmsgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("new local session client: %v", err)
	}
	service, err := New(Options{
		TCPListen:     "127.0.0.1:0",
		MaxFrameBytes: 1024,
		FetchBatch:    1,
		Identity: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 1001, SessionID: 1}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) error { return nil }),
		LocalMsgQ:  fixedLocalMsgQ{client: client},
	})
	if err != nil {
		t.Fatalf("new agent service: %v", err)
	}
	if err := mailbox.Start(root); err != nil {
		t.Fatalf("start msgq runtime: %v", err)
	}
	if err := service.Start(servertest.Context{TasksValue: root, AdmissionValue: lifecycle.NewIngress(root)}); err != nil {
		t.Fatalf("start agent service: %v", err)
	}
	if service.TCPAddress() == "" {
		t.Fatal("TCP address is empty")
	}
	t.Cleanup(func() {
		_ = service.Stop(context.Background())
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

func TestServiceDoesNotFallbackWhenConfiguredLocalMsgQFails(t *testing.T) {
	want := errors.New("local msgq is not started")
	service, err := New(Options{
		TCPListen:     "127.0.0.1:0",
		MaxFrameBytes: 1024,
		FetchBatch:    1,
		Identity: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 1001, SessionID: 1}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) error { return nil }),
		LocalMsgQ:  failingLocalMsgQ{err: want},
	})
	if err != nil {
		t.Fatalf("new agent service: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	err = service.Start(servertest.Context{
		TasksValue:     root,
		AdmissionValue: lifecycle.NewIngress(root),
	})
	if !errors.Is(err, want) {
		t.Fatalf("Start error = %v, want local msgq error", err)
	}
}

type failingLocalMsgQ struct{ err error }

func (msgq failingLocalMsgQ) SessionClient() (txmsgq.SessionClient, error) {
	return nil, msgq.err
}

type fixedLocalMsgQ struct{ client txmsgq.SessionClient }

func (msgq fixedLocalMsgQ) SessionClient() (txmsgq.SessionClient, error) {
	return msgq.client, nil
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
