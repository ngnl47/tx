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
	bindings map[int64]txmsgq.SessionBinding
}

func newMemoryStore() *memoryStore {
	return &memoryStore{bindings: make(map[int64]txmsgq.SessionBinding)}
}

func (store *memoryStore) Bind(_ context.Context, binding txmsgq.SessionBinding) (txmsgq.SessionBinding, bool, error) {
	previous, exists := store.bindings[binding.RoleID]
	store.bindings[binding.RoleID] = binding
	return previous, exists, nil
}

func (store *memoryStore) Find(_ context.Context, roleID int64) (txmsgq.SessionBinding, bool, error) {
	binding, exists := store.bindings[roleID]
	return binding, exists, nil
}

func (store *memoryStore) Refresh(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	current, exists := store.bindings[binding.RoleID]
	return exists && current == binding, nil
}

func (store *memoryStore) Unbind(_ context.Context, binding txmsgq.SessionBinding) (bool, error) {
	current, exists := store.bindings[binding.RoleID]
	if !exists || current != binding {
		return false, nil
	}
	delete(store.bindings, binding.RoleID)
	return true, nil
}
