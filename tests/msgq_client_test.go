package tests

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
)

func TestSessionClientDoesNotExposeConnectionClose(t *testing.T) {
	if _, exposed := reflect.TypeFor[msgq.SessionClient]().MethodByName("Close"); exposed {
		t.Fatal("借用的 SessionClient 不应暴露连接关闭权")
	}
}

func TestSessionClientRouterRestoreUsesExactSessionOwner(t *testing.T) {
	store := &sessionStoreFake{binding: msgq.SessionBinding{
		RoleID:       470001,
		SessionID:    99,
		ConnectionID: 7,
		InstanceID:   "msgq-b",
	}, found: true}
	directory := sessionClientDirectory(t)
	dialer := &sessionClientDialerFake{}
	router, err := msgq.NewSessionClientRouter("project-mailbox", directory, store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}

	_, err = router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 99, Restore: true})
	if err != nil {
		t.Fatalf("acquire existing owner: %v", err)
	}
	if len(dialer.instances) != 1 || dialer.instances[0].ID != "msgq-b" {
		t.Fatalf("dialed instances = %#v, want only msgq-b", dialer.instances)
	}
	if got := store.findSessionIDs; len(got) != 1 || got[0] != 99 {
		t.Fatalf("store find session IDs = %#v, want [99]", got)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v, want [Find]", store.operations)
	}
}

func TestSessionClientRouterReusesEndpointClientAndClosesIt(t *testing.T) {
	store := &sessionStoreFake{binding: msgq.SessionBinding{
		RoleID:       470001,
		SessionID:    99,
		ConnectionID: 7,
		InstanceID:   "msgq-b",
	}, found: true}
	dialer := &sessionClientDialerFake{}
	router, err := msgq.NewSessionClientRouter("project-mailbox", sessionClientDirectory(t), store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}
	for range 2 {
		if _, err := router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 99, Restore: true}); err != nil {
			t.Fatalf("acquire existing owner: %v", err)
		}
	}
	if len(dialer.instances) != 1 {
		t.Fatalf("dial calls = %d, want 1", len(dialer.instances))
	}
	if !slices.Equal(store.operations, []string{"Find", "Find"}) {
		t.Fatalf("store operations = %#v, want two exact reads", store.operations)
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close client router: %v", err)
	}
	if calls := dialer.clients[0].closeCalls.Load(); calls != 1 {
		t.Fatalf("client close calls = %d, want 1", calls)
	}
}

func TestSessionClientRouterFreshReusesRoleOwnerWithoutSessionLookup(t *testing.T) {
	directory := sessionClientDirectory(t)
	dialer := &sessionClientDialerFake{}
	store := &sessionStoreFake{findErr: errors.New("fresh acquire must not query store")}
	router, err := msgq.NewSessionClientRouter("project-mailbox", directory, store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}

	for index, want := range []string{"msgq-a", "msgq-a"} {
		_, err := router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: int64(99 + index)})
		if err != nil {
			t.Fatalf("acquire running service: %v", err)
		}
		if got := dialer.instances[len(dialer.instances)-1].ID; got != want {
			t.Fatalf("dialed instance = %q, want %q", got, want)
		}
	}
	if len(store.operations) != 0 {
		t.Fatalf("fresh acquire used session store: %#v", store.operations)
	}
}

func TestSessionClientRouterConcurrentFreshSessionsShareRoleOwner(t *testing.T) {
	const sessions = 32
	store := &sessionStoreFake{}
	var dialing sync.WaitGroup
	dialing.Add(sessions)
	dialer := &sessionClientDialerFake{dialBarrier: &dialing}
	router, err := msgq.NewSessionClientRouter("project-mailbox", sessionClientDirectory(t), store, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	clients := make([]msgq.SessionClient, sessions)
	errs := make([]error, sessions)
	var group sync.WaitGroup
	for index := range sessions {
		group.Go(func() {
			clients[index], errs[index] = router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 47, SessionID: int64(index + 1)})
		})
	}
	group.Wait()
	owner, found, err := store.FindRole(context.Background(), 47)
	if err != nil || !found {
		t.Fatalf("role owner = %+v, found=%v, err=%v", owner, found, err)
	}
	for index, client := range clients {
		if errs[index] != nil || client == nil || client != clients[0] {
			t.Fatalf("session %d client = %p, err=%v, want shared %p", index+1, client, errs[index], clients[0])
		}
	}
	for index, instance := range dialer.instances {
		if instance.ID != owner.InstanceID {
			t.Fatalf("dial %d instance = %q, want role owner %q", index, instance.ID, owner.InstanceID)
		}
		wantClosed := int32(1)
		if dialer.clients[index] == clients[0] {
			wantClosed = 0
		}
		if calls := dialer.clients[index].closeCalls.Load(); calls != wantClosed {
			t.Fatalf("dial %d close calls = %d, want %d", index, calls, wantClosed)
		}
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close client router: %v", err)
	}
	for index, client := range dialer.clients {
		if calls := client.closeCalls.Load(); calls != 1 {
			t.Fatalf("dial %d final close calls = %d, want 1", index, calls)
		}
	}
}

func TestSessionClientRouterRestoreDoesNotFallbackToAnotherInstance(t *testing.T) {
	dialer := &sessionClientDialerFake{}
	store := &sessionStoreFake{}
	router, err := msgq.NewSessionClientRouter("project-mailbox", sessionClientDirectory(t), store, dialer)
	if err != nil {
		t.Fatalf("new client router: %v", err)
	}

	_, err = router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 99, Restore: true})
	if !errors.Is(err, msgq.ErrSessionOffline) {
		t.Fatalf("restore error = %v, want session offline", err)
	}
	if len(dialer.instances) != 0 {
		t.Fatalf("restore fallback dialed %#v", dialer.instances)
	}
	if _, err := router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 100}); err != nil {
		t.Fatalf("fresh acquire after failed restore: %v", err)
	}
	if len(dialer.instances) != 1 || dialer.instances[0].ID != "msgq-a" {
		t.Fatalf("fresh dialed instances = %#v, want msgq-a", dialer.instances)
	}
	if !slices.Equal(store.operations, []string{"Find"}) {
		t.Fatalf("store operations = %#v, fresh must not add operations", store.operations)
	}
}

func TestSessionClientRouterRestoreRejectsInvalidBindingOrOwner(t *testing.T) {
	tests := map[string]struct {
		binding   msgq.SessionBinding
		instances []discovery.Instance
		wantErr   error
	}{
		"mismatched role": {
			binding: msgq.SessionBinding{RoleID: 470002, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-b"},
			wantErr: msgq.ErrSessionChanged,
		},
		"mismatched session record": {
			binding: msgq.SessionBinding{RoleID: 470001, SessionID: 100, ConnectionID: 7, InstanceID: "msgq-b"},
			wantErr: msgq.ErrSessionOffline,
		},
		"unavailable owner": {
			binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-missing"},
			wantErr: msgq.ErrRouteUnavailable,
		},
		"draining owner": {
			binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "msgq-b"},
			instances: []discovery.Instance{
				{ID: "msgq-a", Services: []string{"project-mailbox"}, Endpoint: "127.0.0.1:9101", Status: discovery.StatusRunning},
				{ID: "msgq-b", Services: []string{"project-mailbox"}, Endpoint: "127.0.0.1:9102", Status: discovery.StatusDraining},
			},
			wantErr: msgq.ErrRouteUnavailable,
		},
		"owner without msgq service": {
			binding: msgq.SessionBinding{RoleID: 470001, SessionID: 99, ConnectionID: 7, InstanceID: "game-b"},
			instances: []discovery.Instance{
				{ID: "msgq-a", Services: []string{"project-mailbox"}, Endpoint: "127.0.0.1:9101", Status: discovery.StatusRunning},
				{ID: "game-b", Services: []string{"game"}, Endpoint: "127.0.0.1:9102", Status: discovery.StatusRunning},
			},
			wantErr: msgq.ErrRouteUnavailable,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store := &sessionStoreFake{binding: test.binding, found: true}
			dialer := &sessionClientDialerFake{}
			directory := sessionClientDirectory(t)
			if test.instances != nil {
				directory = newSessionClientDirectory(t, test.instances)
			}
			router, err := msgq.NewSessionClientRouter("project-mailbox", directory, store, dialer)
			if err != nil {
				t.Fatalf("new client router: %v", err)
			}
			_, err = router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 99, Restore: true})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("restore error = %v, want %v", err, test.wantErr)
			}
			if len(dialer.instances) != 0 {
				t.Fatalf("restore dialed %#v", dialer.instances)
			}
			if !slices.Equal(store.operations, []string{"Find"}) {
				t.Fatalf("store operations = %#v, want [Find]", store.operations)
			}
		})
	}
}

func TestFixedSessionClientRouterAlwaysReturnsSharedLocalClient(t *testing.T) {
	client := &sessionClientFake{}
	router, err := msgq.NewFixedSessionClientRouter(client)
	if err != nil {
		t.Fatalf("new fixed session client router: %v", err)
	}
	for range 2 {
		got, err := router.Acquire(context.Background(), msgq.AcquireSessionRequest{RoleID: 470001, SessionID: 99})
		if err != nil {
			t.Fatalf("acquire fixed session client: %v", err)
		}
		if got != client {
			t.Fatalf("client = %T %p, want shared %p", got, got, client)
		}
	}
	if err := router.Close(); err != nil {
		t.Fatalf("close fixed session client router: %v", err)
	}
	if calls := client.closeCalls.Load(); calls != 0 {
		t.Fatalf("fixed router closed shared client %d times, want 0", calls)
	}
	for _, request := range []msgq.AcquireSessionRequest{{SessionID: 99}, {RoleID: 470001}} {
		if _, err := router.Acquire(context.Background(), request); !errors.Is(err, msgq.ErrInvalidSession) {
			t.Fatalf("Acquire(%#v) error = %v, want invalid session", request, err)
		}
	}
}

func sessionClientDirectory(t *testing.T) *discovery.ServiceDirectory {
	t.Helper()
	return newSessionClientDirectory(t, []discovery.Instance{
		{ID: "msgq-a", Services: []string{"project-mailbox"}, Endpoint: "127.0.0.1:9101", Status: discovery.StatusRunning},
		{ID: "msgq-b", Services: []string{"project-mailbox"}, Endpoint: "127.0.0.1:9102", Status: discovery.StatusRunning},
	})
}

func newSessionClientDirectory(t *testing.T, instances []discovery.Instance) *discovery.ServiceDirectory {
	t.Helper()
	directory := discovery.NewServiceDirectory()
	err := directory.Replace(instances)
	if err != nil {
		t.Fatalf("replace directory: %v", err)
	}
	return directory
}

type sessionClientDialerFake struct {
	mu        sync.Mutex
	instances []discovery.Instance
	clients   []*sessionClientFake

	dialBarrier *sync.WaitGroup
}

func (d *sessionClientDialerFake) DialSession(_ context.Context, instance discovery.Instance) (msgq.SessionConnection, error) {
	client := &sessionClientFake{}
	d.mu.Lock()
	d.instances = append(d.instances, instance)
	d.clients = append(d.clients, client)
	d.mu.Unlock()
	if d.dialBarrier != nil {
		// 等全部冷拨号进入后再返回，固定连接缓存竞争而不依赖调度时序。
		d.dialBarrier.Done()
		d.dialBarrier.Wait()
	}
	return client, nil
}

type sessionClientFake struct {
	closeCalls atomic.Int32
}

func (*sessionClientFake) OpenMailboxStream(context.Context, msgq.InitRequest, func()) (msgq.MailboxStreamSession, error) {
	return nil, errors.New("unexpected MailboxStream")
}

func (*sessionClientFake) TerminateSession(context.Context, msgq.SessionHeader) error {
	return nil
}

func (c *sessionClientFake) Close() error {
	c.closeCalls.Add(1)
	return nil
}
