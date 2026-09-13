package actortrial

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	neturl "net/url"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/protobuf/proto"
)

func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 显式运行真实 NATS 测试")
	}
	return url
}
func trialContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func peer(t *testing.T, url, namespace, name string, r *Runtime) *Transport {
	t.Helper()
	p, err := Connect(url, namespace, name, r, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.conn.Close)
	return p
}
func echoRuntime() *Runtime {
	return New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, v any) (any, error) {
			d := v.(*actorpb.Delivery)
			if d.MessageType == "ack" {
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
			}
			return &actorpb.Result{MessageType: d.MessageType, CommandId: d.CommandId, Payload: d.Payload}, nil
		}}, nil
	})
}
func TestNATSRoundTripAndAdmission(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	r := echoRuntime()
	a := peer(t, url, ns, "a", echoRuntime())
	b := peer(t, url, ns, "b", r)
	defer a.runtime.Drain(ctx)
	defer r.Drain(ctx)
	d := &actorpb.Delivery{MessageType: "buff", CommandId: 7, Payload: []byte{0, 1, 255}, UpSeq: 3, Identity: &actorpb.SessionIdentity{AccountId: 9}}
	target := Target{Instance: "b", Identity: Identity{"room", "42"}}
	result, err := a.Call(ctx, target, d)
	if err != nil || result.CommandId != 7 || string(result.Payload) != string(d.Payload) {
		t.Fatalf("result=%v err=%v", result, err)
	}
	if !proto.Equal(d, &actorpb.Delivery{MessageType: "buff", CommandId: 7, Payload: []byte{0, 1, 255}, UpSeq: 3, Identity: &actorpb.SessionIdentity{AccountId: 9}}) {
		t.Fatal("调用修改了输入")
	}
	ack, err := a.Call(ctx, target, &actorpb.Delivery{MessageType: "ack", UpSeq: 3, Identity: d.Identity})
	if err != nil || ack.ProcessedUpSeq != 3 {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
	target.Instance = "absent"
	if _, err = a.Call(ctx, target, d); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("absent: %v", err)
	}
	if err = b.Drain(ctx, func(context.Context) error { return errors.New("flush failed") }); err == nil {
		t.Fatal("丢失最终持久化错误")
	}
	target.Instance = "b"
	if _, err = a.Call(ctx, target, d); !errors.Is(err, ErrDraining) {
		t.Fatalf("draining: %v", err)
	}
	if b.conn.IsClosed() {
		t.Fatal("持久化失败不应关闭连接")
	}
	if err = b.Drain(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestNATSErrorsMatchLocalCalls(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	for _, test := range []struct {
		name    string
		factory func(Context) (*Behavior, error)
		want    error
		code    int32
	}{
		{"denied", func(Context) (*Behavior, error) { return nil, fmt.Errorf("battle expired: %w", ErrActivationDenied) }, ErrActivationDenied, -3},
		{"business", func(Context) (*Behavior, error) {
			return &Behavior{Receive: func(Context, any) (any, error) {
				return &actorpb.Result{ErrorCode: 42, ErrorMessage: "insufficient gold"}, nil
			}}, nil
		}, nil, 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := New(4, time.Second, test.factory)
			p := peer(t, url, ns, test.name, r)
			remote := peer(t, url, ns, "caller-"+test.name, echoRuntime())
			defer r.Drain(ctx)
			defer remote.runtime.Drain(ctx)
			for _, caller := range []*Transport{p, remote} {
				_, err := caller.Call(ctx, Target{Instance: test.name, Identity: Identity{"room", "1"}}, &actorpb.Delivery{MessageType: "test"})
				if test.want != nil {
					if !errors.Is(err, test.want) {
						t.Fatalf("expected %v: %v", test.want, err)
					}
				} else {
					var coded interface{ ErrorCode() int32 }
					if !errors.As(err, &coded) || coded.ErrorCode() != test.code {
						t.Fatalf("code %d: %v", test.code, err)
					}
					var remoteError *RemoteError
					if !errors.As(err, &remoteError) || remoteError.Message != "insufficient gold" {
						t.Fatalf("错误文本不应重复包装: %v", err)
					}
				}
			}
		})
	}
}

func TestNATSAcceptanceAndMailbox(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	finished := make(chan struct{}, 4)
	r := New(1, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, v any) (any, error) {
			d := v.(*actorpb.Delivery)
			if d.MessageType == "slow" {
				started <- struct{}{}
				<-release
				finished <- struct{}{}
			}
			return &actorpb.Result{MessageType: "ok", Payload: d.Payload}, nil
		}}, nil
	})
	b := peer(t, url, ns, "b", r)
	a := peer(t, url, ns, "a", echoRuntime())
	target := Target{Instance: "b", Identity: Identity{"room", "1"}}
	if err := a.TellAccepted(ctx, target, &actorpb.Delivery{MessageType: "slow"}); err != nil {
		t.Fatal(err)
	}
	signal(t, ctx, started)
	pending := make(chan error, 1)
	go func() { _, err := a.Call(ctx, target, &actorpb.Delivery{MessageType: "slow"}); pending <- err }()
	eventually(t, ctx, func() bool { return r.Probe(target.Identity).PendingMessages == 1 })
	if _, err := a.Call(ctx, target, &actorpb.Delivery{MessageType: "overflow"}); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("overflow: %v", err)
	}
	// 另一个实体不受这个实体或 NATS 订阅回调阻塞。
	if _, err := a.Call(ctx, Target{Instance: "b", Identity: Identity{"room", "2"}}, &actorpb.Delivery{MessageType: "fast"}); err != nil {
		t.Fatal(err)
	}
	unblock()
	signal(t, ctx, finished)
	select {
	case err := <-pending:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
func signal(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func eventually(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestNATSTimeoutAndSendFailureReleaseWaiters(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	r := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, v any) (any, error) {
			if v.(*actorpb.Delivery).MessageType == "slow" {
				close(started)
				<-release
			}
			return &actorpb.Result{MessageType: "ok", Payload: []byte("complete")}, nil
		}}, nil
	})
	a := peer(t, url, ns, "a", echoRuntime())
	b := peer(t, url, ns, "b", r)
	target := Target{Instance: "b", Identity: Identity{"room", "1"}}
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := a.Call(callCtx, target, &actorpb.Delivery{MessageType: "slow"}); returned <- err }()
	signal(t, ctx, started)
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unblock()
	if _, err := a.Call(ctx, target, &actorpb.Delivery{MessageType: "after"}); err != nil {
		t.Fatal(err)
	}
	if pendingCount(a) != 0 || a.conn.NumSubscriptions() != 2 {
		t.Fatalf("残留等待: pending=%d subscriptions=%d", pendingCount(a), a.conn.NumSubscriptions())
	}
	large := &actorpb.Delivery{MessageType: "oversize", Payload: make([]byte, int(a.conn.MaxPayload())+1)}
	for range 16 {
		if _, err := a.Call(ctx, target, large); !errors.Is(err, nats.ErrMaxPayload) {
			t.Fatalf("oversize: %v", err)
		}
	}
	if pendingCount(a) != 0 || a.conn.NumSubscriptions() != 2 {
		t.Fatal("发送失败泄漏等待记录或订阅")
	}
	if err := b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNATSSharedRepliesStayBoundedAndClose(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	ns := nats.NewInbox()
	started, release := make(chan struct{}, 32), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	r := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
			started <- struct{}{}
			<-release
			d := value.(*actorpb.Delivery)
			return &actorpb.Result{MessageType: "ok", CommandId: d.CommandId}, nil
		}}, nil
	})
	b := peer(t, url, ns, "b", r)
	a := peer(t, url, ns, "a", echoRuntime())
	var futures []*ResultFuture
	var cancels []context.CancelFunc
	for i := range 32 {
		callCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		f, err := a.CallFuture(callCtx, Target{"b", Identity{"room", strconv.Itoa(i)}}, &actorpb.Delivery{CommandId: uint64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		futures, cancels = append(futures, f), append(cancels, cancel)
	}
	for range futures {
		signal(t, ctx, started)
	}
	// 等待数达到上限时，响应订阅仍只有一个，另一个订阅是实例业务入口。
	if got := a.conn.NumSubscriptions(); got != 2 {
		t.Fatalf("32 个等待请求应复用响应订阅，实际订阅=%d", got)
	}
	if _, err := a.CallFuture(ctx, Target{"b", Identity{"room", "overflow"}}, &actorpb.Delivery{}); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("等待上限: %v", err)
	}
	for _, cancel := range cancels[:16] {
		cancel()
	}
	eventually(t, ctx, func() bool { return pendingCount(a) == 16 })
	// 提前取消的响应随后到达，不得完成其他请求；每个结果仍对应原请求号。
	unblock()
	for i, f := range futures[16:] {
		result, err := f.Await(ctx)
		if err != nil || result.CommandId != uint64(i+17) {
			t.Fatalf("请求 %d: %v %v", i+17, result, err)
		}
	}
	eventually(t, ctx, func() bool { return pendingCount(a) == 0 })
	// 有订阅但不响应，确保关闭连接真正唤醒未完成的 Future。
	if _, err := b.conn.Subscribe(ns+".silent", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := b.conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	f, err := a.requestFuture(ctx, &nats.Msg{Subject: ns + ".silent"})
	if err != nil {
		t.Fatal(err)
	}
	a.conn.Close()
	if _, err = f.Await(ctx); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("连接关闭: %v", err)
	}
	if err = b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func pendingCount(p *Transport) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

func TestNATSCrossProcess(t *testing.T) {
	url := natsURL(t)
	if ns := os.Getenv("TX_ACTORTRIAL_CHILD_NAMESPACE"); ns != "" {
		ctx := trialContext(t)
		release := make(chan struct{})
		var p *Transport
		r := New(4, time.Second, func(Context) (*Behavior, error) {
			return &Behavior{
				Receive: func(_ Context, v any) (any, error) {
					d := v.(*actorpb.Delivery)
					if d.MessageType == "slow" {
						fmt.Println("EXECUTING")
						<-release
					}
					return &actorpb.Result{MessageType: "ok", CommandId: d.CommandId, Payload: d.Payload}, nil
				},
				OnDeactivateBefore: func(c Context) error {
					_, err := p.Call(c, Target{Instance: "parent", Identity: Identity{"notice", "1"}}, &actorpb.Delivery{MessageType: "exit"})
					return err
				},
			}, nil
		})
		p = peer(t, url, ns, "child", r)
		fmt.Println("READY")
		done := make(chan struct{})
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "drain":
				go func() {
					defer close(done)
					if err := p.Drain(ctx, func(context.Context) error { fmt.Println("FLUSH"); return nil }); err != nil {
						t.Error(err)
					}
					fmt.Println("DRAINED")
				}()
			case "release":
				close(release)
			}
		}
		signal(t, ctx, done)
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ns := nats.NewInbox()
	notified := make(chan struct{}, 4)
	parent := peer(t, url, ns, "parent", New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) {
			notified <- struct{}{}
			return &actorpb.Result{MessageType: "ok"}, nil
		}}, nil
	}))
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestNATSCrossProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "TX_ACTORTRIAL_CHILD_NAMESPACE="+ns)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Process.Kill() })
	lines := make(chan string, 32)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	waitLine := func(want string) {
		t.Helper()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatalf("child exited before %s", want)
				}
				if line == want {
					return
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	waitLine("READY")
	target := Target{Instance: "child", Identity: Identity{"room", "1"}}
	payload := []byte{0, 1, 255}
	result, err := parent.Call(ctx, target, &actorpb.Delivery{MessageType: "echo", CommandId: 99, Payload: payload})
	if err != nil || result.CommandId != 99 || string(result.Payload) != string(payload) {
		t.Fatalf("cross process result=%v err=%v", result, err)
	}
	if err = parent.TellAccepted(ctx, target, &actorpb.Delivery{MessageType: "slow"}); err != nil {
		t.Fatal(err)
	}
	waitLine("EXECUTING") // 服务端确认不再意味着子进程已开始执行。
	fmt.Fprintln(stdin, "drain")
	eventually(t, ctx, func() bool {
		_, err := parent.Call(ctx, Target{Instance: "child", Identity: Identity{"probe", "1"}}, &actorpb.Delivery{MessageType: "probe"})
		return errors.Is(err, ErrDraining)
	})
	fmt.Fprintln(stdin, "release")
	_ = stdin.Close()
	signal(t, ctx, notified)
	waitLine("FLUSH")
	waitLine("DRAINED")
	for range lines {
	} // 先读完管道再 Wait，避免丢失子进程最后输出。
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err = parent.Call(ctx, target, &actorpb.Delivery{MessageType: "after"}); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("closed child: %v", err)
	}
	if err = parent.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNATSDisconnectDoesNotReplayRPC(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	address, err := neturl.Parse(url)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cut := make(chan func(), 1)
	go func() {
		front, err := listener.Accept()
		if err != nil {
			return
		}
		back, err := net.DialTimeout("tcp", address.Host, time.Second)
		if err != nil {
			front.Close()
			return
		}
		cut <- func() { listener.Close(); front.Close(); back.Close() }
		go io.Copy(front, back)
		_, _ = io.Copy(back, front)
	}()
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var executed atomic.Int32
	r := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) {
			close(started)
			<-release
			executed.Add(1)
			return &actorpb.Result{MessageType: "ok"}, nil
		}}, nil
	})
	b := peer(t, url, ns, "b", r)
	a := peer(t, "nats://"+listener.Addr().String(), ns, "a", echoRuntime())
	var disconnect func()
	select {
	case disconnect = <-cut:
		defer disconnect()
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	a.maxPending = 1
	callCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	target := Target{Instance: "b", Identity: Identity{"room", "1"}}
	go func() { _, err := a.Call(callCtx, target, &actorpb.Delivery{MessageType: "buff"}); returned <- err }()
	signal(t, ctx, started)
	if _, err = a.Call(ctx, target, &actorpb.Delivery{MessageType: "overload"}); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("pending limit: %v", err)
	}
	disconnect()
	select {
	case err = <-returned:
		if err == nil {
			t.Fatal("断线不能伪造成功")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	eventually(t, ctx, func() bool { return !a.conn.IsConnected() })
	// Future 的 deadline 先结束调用方等待，取消回调随后回收等待项；等待有界回收后再测后续发送。
	eventually(t, ctx, func() bool { return pendingCount(a) == 0 && a.conn.NumSubscriptions() == 2 })
	for range 8 {
		if _, err = a.Call(ctx, target, &actorpb.Delivery{MessageType: "disconnected"}); err == nil {
			t.Fatal("断线请求被缓存")
		}
	}
	if pendingCount(a) != 0 || a.conn.NumSubscriptions() != 2 {
		t.Fatal("断线请求残留等待或订阅")
	}
	unblock()
	if err = b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if executed.Load() != 1 {
		t.Fatalf("执行次数=%d", executed.Load())
	}
	if err = a.runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNATSResultContract(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	invalid := []*actorpb.Result{
		{ProcessedUpSeq: 1, Payload: []byte("mixed")},
		{MessageType: "ok", ProcessedUpSeq: -1},
		{ErrorMessage: "missing code"},
		{MessageType: "\xff"},
	}
	for index, result := range invalid {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			r := New(4, time.Second, func(Context) (*Behavior, error) {
				return &Behavior{Receive: func(Context, any) (any, error) { return result, nil }}, nil
			})
			p := peer(t, url, ns, "invalid", r)
			caller := peer(t, url, ns, "caller", echoRuntime())
			defer r.Drain(ctx)
			defer caller.runtime.Drain(ctx)
			for _, from := range []*Transport{p, caller} {
				_, err := from.Call(ctx, Target{Instance: "invalid", Identity: Identity{"room", "1"}}, &actorpb.Delivery{MessageType: "test"})
				if err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("非法结果应明确失败: %v", err)
				}
			}
		})
	}
}

// 固定槽位仍由现有 SlotResolver 判定，NATS Subject 只承载其唯一实例 ID。
func TestNATSSlotResolverAndOwnerHandoff(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	reader := &trialSlotReader{}
	first := discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world-a", Generation: 1}
	reader.current.Store(&first)
	directory := discovery.NewServiceDirectory()
	instances := []discovery.Instance{
		{ID: "world-a", Endpoint: "unused-a", Services: []string{"world"}, Status: discovery.StatusRunning},
		{ID: "world-b", Endpoint: "unused-b", Services: []string{"world"}, Status: discovery.StatusRunning},
	}
	if err := directory.Replace(instances); err != nil {
		t.Fatal(err)
	}
	resolver, err := discovery.NewSlotResolver(reader, directory)
	if err != nil {
		t.Fatal(err)
	}
	makeWorld := func(instance string) *Transport {
		return peer(t, url, ns, instance, New(4, time.Second, func(c Context) (*Behavior, error) {
			owner, err := resolver.ResolveSlot(c, c.Identity.Key)
			if err != nil {
				return nil, err
			}
			if owner.Assignment.RuntimeInstanceID != instance {
				return nil, &RemoteError{Code: -3, Message: "wrong slot owner"}
			}
			return &Behavior{Receive: func(Context, any) (any, error) {
				return &actorpb.Result{MessageType: "owner", Payload: []byte(instance)}, nil
			}}, nil
		}))
	}
	a, b := makeWorld("world-a"), makeWorld("world-b")
	caller := peer(t, url, ns, "caller", echoRuntime())
	target, err := resolver.ResolveSlot(ctx, "world-1")
	if err != nil {
		t.Fatal(err)
	}
	route := Target{Instance: target.Assignment.RuntimeInstanceID, Identity: Identity{"world", "world-1"}}
	d := &actorpb.Delivery{MessageType: "scout"}
	result, err := caller.Call(ctx, route, d)
	if err != nil || string(result.Payload) != "world-a" {
		t.Fatalf("owner: %v %v", result, err)
	}
	wrong := route
	wrong.Instance = "world-b"
	_, err = caller.Call(ctx, wrong, d)
	var rejected *RemoteError
	if !errors.As(err, &rejected) || rejected.Code != -3 {
		t.Fatalf("wrong owner: %v", err)
	}
	eventually(t, ctx, func() bool { return b.runtime.Probe(wrong.Identity).State == Failed })
	instances[0].Status = discovery.StatusDraining
	if err = directory.Replace(instances); err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.ResolveSlot(ctx, "world-1"); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("draining owner: %v", err)
	}
	if err = a.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	next := discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world-b", Generation: 2}
	if err = discovery.ValidateSlotPromotion(first, next); err != nil {
		t.Fatal(err)
	}
	reader.current.Store(&next)
	target, err = resolver.ResolveSlot(ctx, "world-1")
	if err != nil {
		t.Fatal(err)
	}
	if target.Assignment.Generation != 2 {
		t.Fatal("generation lost")
	}
	route.Instance = target.Assignment.RuntimeInstanceID
	// 旧 owner 完成排水且归属更新后，由交接流程明确重新激活，业务请求不隐式重试失败加载。
	if _, err = caller.RetryActivation(ctx, route); err != nil {
		t.Fatal(err)
	}
	result, err = caller.Call(ctx, route, d)
	if err != nil || string(result.Payload) != "world-b" {
		t.Fatalf("promoted: %v %v", result, err)
	}
	route.Instance = first.RuntimeInstanceID
	if _, err = caller.Call(ctx, route, d); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("stale route: %v", err)
	}
	if err = b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = caller.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

type trialSlotReader struct {
	current atomic.Pointer[discovery.SlotAssignment]
}

func (s *trialSlotReader) ResolveSlot(_ context.Context, id string) (discovery.SlotAssignment, error) {
	assignment := s.current.Load()
	if assignment == nil || assignment.SlotID != id {
		return discovery.SlotAssignment{}, discovery.ErrSlotNotFound
	}
	return *assignment, nil
}

func TestNATSPlacementChangeRejectsStaleRoute(t *testing.T) {
	url := natsURL(t)
	ctx := trialContext(t)
	ns := nats.NewInbox()
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	oldRuntime := New(4, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) {
			close(started)
			<-release
			return &actorpb.Result{MessageType: "old.done"}, nil
		}}, nil
	})
	oldRuntime.SetPlacement(NewPlacement("a", map[string][]string{"role": {"a"}}))
	newRuntime := echoRuntime()
	newRuntime.SetPlacement(NewPlacement("b", map[string][]string{"role": {"a"}}))
	a := peer(t, url, ns, "a", oldRuntime)
	b := peer(t, url, ns, "b", newRuntime)
	caller := peer(t, url, ns, "caller", echoRuntime())
	id := Identity{"role", "1"}
	oldTarget := Target{Instance: "a", Identity: id}
	d := &actorpb.Delivery{MessageType: "test"}
	completed := make(chan error, 1)
	go func() { _, err := caller.Call(ctx, oldTarget, d); completed <- err }()
	signal(t, ctx, started)
	next := NewPlacement("a", map[string][]string{"role": {"b"}})
	oldRuntime.SetPlacement(next)
	newRuntime.SetPlacement(NewPlacement("b", map[string][]string{"role": {"b"}}))
	if _, err := caller.Call(ctx, oldTarget, d); !errors.Is(err, ErrNotOwner) || errors.Is(err, ErrActivationDenied) {
		t.Fatalf("旧路由应重新解析而非业务死信: %v", err)
	}
	target, err := next.Target(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = caller.Call(ctx, target, d); err != nil {
		t.Fatal(err)
	}
	// 单机关闸不是跨进程交接屏障：新 owner 可响应时，旧请求仍未退出。
	select {
	case <-completed:
		t.Fatal("旧请求应仍在阻塞")
	default:
	}
	unblock()
	select {
	case err = <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = a.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = b.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = caller.Drain(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
