package agentservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
)

func TestServiceStartsWithExplicitDependenciesAndLocalMsgQ(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{
		InstanceID:         "msgq-test",
		Store:              newMemorySessionStore(),
		MaxBacklogMessages: 8,
		OfflineTimeout:     time.Minute,
		SweepInterval:      time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer returned error: %v", err)
	}
	client, err := txmsgq.NewLocalSessionClient(mailbox)
	if err != nil {
		t.Fatalf("NewLocalSessionClient returned error: %v", err)
	}
	if err := mailbox.Start(root); err != nil {
		t.Fatalf("start mailbox: %v", err)
	}

	service, err := New(Options{
		TCPListen:     "127.0.0.1:0",
		MaxFrameBytes: 1024,
		FetchBatch:    1,
		Identity: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (txagent.Identity, error) {
			return txagent.Identity{RoleID: 1001, SessionID: 1}, nil
		}),
		Dispatcher: txagent.DispatchFunc(func(context.Context, txagent.DispatchRequest) (txagent.DispatchResult, error) {
			return txagent.DispatchResult{}, nil
		}),
		LocalMsgQ: fixedLocalMsgQ{client: client},
	}, Dependencies{
		Tasks:     root,
		Admission: lifecycle.NewIngress(root),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if service.TCPAddress() == "" {
		t.Fatal("TCPAddress is empty")
	}
	if err := service.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
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
