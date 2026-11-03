package msgqservice

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/component"
	"github.com/ngnl47/tx/lifecycle"
	txmetrics "github.com/ngnl47/tx/metrics"
	"github.com/ngnl47/tx/modules/discovery"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txrpc "github.com/ngnl47/tx/modules/rpc"
)

func TestServiceStartsWithExplicitDependencies(t *testing.T) {
	var ready atomic.Bool
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
		StartupAdmission: lifecycle.AdmissionFunc(func() (func(), error) {
			if !ready.Load() {
				return nil, lifecycle.ErrNotReady
			}
			return func() {}, nil
		}),
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
	header := txmsgq.SessionHeader{RoleID: 1, SessionID: 2, ConnectionID: 3}
	local, err := service.SessionClient()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.OpenMailboxStream(t.Context(), txmsgq.InitRequest{Header: header}, nil); !errors.Is(err, lifecycle.ErrNotReady) {
		t.Fatalf("装配期间创建了邮箱: %v", err)
	}
	ready.Store(true)
	if err := service.mailbox.Init(context.Background(), txmsgq.InitRequest{Header: header}); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	if _, err := service.mailbox.PubSession(context.Background(), header.SessionID, []byte("down")); err != nil {
		t.Fatalf("PubSession returned error: %v", err)
	}
	response := httptest.NewRecorder()
	txmetrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), "tx_msgq_backlog_messages 1") {
		t.Fatal("started mailbox backlog is absent from metrics")
	}
	if err := service.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	response = httptest.NewRecorder()
	txmetrics.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(response.Body.String(), "tx_msgq_backlog_messages 0") {
		t.Fatal("stopped mailbox remains registered in metrics")
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

func TestServiceStopRetainsMailboxAndMetricsUntilClosureConfirmed(t *testing.T) {
	store := newMemoryStore()
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	client, _ := txmsgq.NewLocalSessionClient(mailbox)
	removed := 0
	service := &Service{mailbox: mailbox, client: client, removeMetrics: func() { removed++ }}
	allow, entered := make(chan struct{}), make(chan struct{})
	_, err = client.OpenMailboxStream(t.Context(), txmsgq.InitRequest{Header: txmsgq.SessionHeader{RoleID: 1, SessionID: 2, ConnectionID: 3}}, func() { close(entered); <-allow })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = service.Stop(ctx)
	if !errors.Is(err, component.ErrStopDeferred) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop must retain dependencies: %v", err)
	}
	<-entered
	if service.mailbox != mailbox || removed != 0 {
		t.Fatal("incomplete stop discarded mailbox or metrics")
	}
	if _, found, err := store.FindRole(t.Context(), 1); err != nil || !found {
		t.Fatalf("owner prematurely released: %v %v", found, err)
	}
	close(allow)
	if err = service.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if service.mailbox != nil || removed != 1 {
		t.Fatal("successful stop retained mailbox or repeated metrics cleanup")
	}
	if err = service.Stop(t.Context()); err != nil || removed != 1 {
		t.Fatalf("repeated stop: %d %v", removed, err)
	}
}
