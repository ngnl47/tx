package msgqservice

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txrpc "github.com/ngnl47/tx/modules/rpc"
)

func TestServiceStartsWithExplicitDependencies(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	}()
	host, err := txrpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewHost returned error: %v", err)
	}
	service, err := New(Options{
		Store:              newMemoryStore(),
		MaxBacklogMessages: 4,
	}, Dependencies{
		Tasks:    root,
		RPC:      host,
		Instance: discovery.Instance{ID: "msgq-1"},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := service.SessionClient(); err != nil {
		t.Fatalf("SessionClient returned error: %v", err)
	}
	if err := service.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

type memoryStore struct {
	roleTestStore
	bindings map[int64]txmsgq.SessionBinding
}

func newMemoryStore() *memoryStore {
	return &memoryStore{bindings: make(map[int64]txmsgq.SessionBinding)}
}

func (store *memoryStore) Create(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	if _, exists := store.bindings[binding.SessionID]; exists {
		return false, nil
	}
	store.bindings[binding.SessionID] = binding
	return true, nil
}

func (store *memoryStore) Rebind(_ context.Context, expected, replacement txmsgq.SessionBinding) (bool, error) {
	current, exists := store.bindings[expected.SessionID]
	if !exists || current != expected {
		return false, nil
	}
	store.bindings[replacement.SessionID] = replacement
	return true, nil
}

func (store *memoryStore) Find(_ context.Context, sessionID int64) (txmsgq.SessionBinding, bool, error) {
	binding, exists := store.bindings[sessionID]
	return binding, exists, nil
}

func (store *memoryStore) Refresh(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	current, exists := store.bindings[binding.SessionID]
	return exists && current == binding, nil
}

func (store *memoryStore) Unbind(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	current, exists := store.bindings[binding.SessionID]
	if !exists || current != binding {
		return false, nil
	}
	delete(store.bindings, binding.SessionID)
	return true, nil
}
