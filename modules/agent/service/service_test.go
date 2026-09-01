package agentservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
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
		Identity: txagent.IdentityResolverFunc(func(context.Context, txagent.AuthRequest) (protocol.SessionPrincipal, error) {
			return protocol.SessionPrincipal{
				AccountID:  100,
				ServerID:   1,
				RoleID:     1001,
				SessionID:  1,
				AccessMode: protocol.AccessModeNormal,
			}, nil
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

func TestServiceComposeHTTPHandlerRoutesRootToWebSocket(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	projectCalled := false
	service := &Service{
		options: Options{
			HTTPHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				projectCalled = true
			}),
		},
		dependencies: Dependencies{Admission: lifecycle.NewIngress(root)},
	}

	websocketCalled := false
	handler := service.composeHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocketCalled = true
		w.WriteHeader(http.StatusSwitchingProtocols)
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/", nil))

	if !websocketCalled {
		t.Fatal("root path did not route to WebSocket handler")
	}
	if projectCalled {
		t.Fatal("root path unexpectedly routed to project handler")
	}
	if recorder.Code != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusSwitchingProtocols)
	}
}

func TestServiceComposeHTTPHandlerRoutesProjectPathToHTTPHandler(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	projectCalled := false
	service := &Service{
		options: Options{
			HTTPHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				projectCalled = true
				if r.URL.Path != "/api/v1/login/server-list" {
					t.Fatalf("path = %q, want /api/v1/login/server-list", r.URL.Path)
				}
				w.WriteHeader(http.StatusNoContent)
			}),
		},
		dependencies: Dependencies{Admission: lifecycle.NewIngress(root)},
	}

	websocketCalled := false
	handler := service.composeHTTPHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		websocketCalled = true
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/login/server-list", nil))

	if !projectCalled {
		t.Fatal("project path did not route to HTTP handler")
	}
	if websocketCalled {
		t.Fatal("project path unexpectedly routed to WebSocket handler")
	}
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestServiceComposeHTTPHandlerReturnsNotFoundWithoutProjectHandler(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	service := &Service{
		dependencies: Dependencies{Admission: lifecycle.NewIngress(root)},
	}

	handler := service.composeHTTPHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("non-root path unexpectedly routed to WebSocket handler")
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/login/server-list", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestServiceComposeHTTPHandlerReturnsServiceUnavailableAfterDrain(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	ingress := lifecycle.NewIngress(root)
	ingress.BeginDrain()

	projectCalled := false
	service := &Service{
		options: Options{
			HTTPHandler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				projectCalled = true
			}),
		},
		dependencies: Dependencies{Admission: ingress},
	}

	handler := service.composeHTTPHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("draining project path unexpectedly routed to WebSocket handler")
	}))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/login/server-list", nil))

	if projectCalled {
		t.Fatal("draining request unexpectedly entered project handler")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestServiceComposeHTTPHandlerKeepsPendingUntilProjectHandlerReturns(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	ingress := lifecycle.NewIngress(root)
	entered := make(chan struct{})
	release := make(chan struct{})
	service := &Service{
		options: Options{
			HTTPHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				w.WriteHeader(http.StatusNoContent)
			}),
		},
		dependencies: Dependencies{Admission: ingress},
	}

	handler := service.composeHTTPHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("project path unexpectedly routed to WebSocket handler")
	}))

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() {
		releaseOnce.Do(func() { close(release) })
	}
	go func() {
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/login/server-list", nil))
		close(done)
	}()
	defer releaseHandler()

	<-entered
	if pending := ingress.Pending(); pending != 1 {
		t.Fatalf("pending = %d, want 1", pending)
	}

	ingress.BeginDrain()
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := ingress.WaitContext(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitContext error = %v, want %v", err, context.DeadlineExceeded)
	}

	releaseHandler()
	<-done

	if err := ingress.WaitContext(context.Background()); err != nil {
		t.Fatalf("WaitContext returned error after handler completion: %v", err)
	}
	if pending := ingress.Pending(); pending != 0 {
		t.Fatalf("pending = %d, want 0", pending)
	}
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
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
