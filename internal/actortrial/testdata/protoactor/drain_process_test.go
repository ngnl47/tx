package actor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	"github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// TestActorDrainAcrossProcesses 锁定已准入工作不因关闸丢失、迟到请求明确失败的边界。
// 管道仅协调测试屏障，不新增生产控制协议；业务请求走真实 Actor Remote。
func TestActorDrainAcrossProcesses(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the process integration test")
	}
	if prefix := os.Getenv("TX_DRAIN_CHILD_PREFIX"); prefix != "" {
		runDrainProcess(t, endpoint, prefix)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/tx/tests/drain-process-%d", time.Now().UnixNano())
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestActorDrainAcrossProcesses$", "-test.v")
	child.Env = append(os.Environ(), "TX_DRAIN_CHILD_PREFIX="+prefix)
	child.Stderr = os.Stderr
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close(); _ = child.Process.Kill(); _ = child.Wait() }()
	events := make(chan string, 8)
	go func() {
		defer close(events)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if event, ok := strings.CutPrefix(scanner.Text(), "DRAIN-PROBE "); ok {
				events <- event
			} else {
				fmt.Fprintln(os.Stderr, scanner.Text())
			}
		}
	}()
	wait := func() string {
		t.Helper()
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("target exited before expected barrier")
			}
			return event
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return ""
		}
	}
	expect := func(want string) {
		t.Helper()
		if got := wait(); got != want {
			t.Fatalf("barrier = %q, want %q", got, want)
		}
	}
	address := wait()
	runtime := startDrainRuntime(t, endpoint, prefix)
	defer runtime.Shutdown()
	pid := protoactor.NewPID(address, "drain-probe")
	first, err := runtime.CallFuture(ctx, pid, Delivery{MessageType: "drain.probe", CommandID: 1})
	if err != nil {
		t.Fatal(err)
	}
	expect("started")
	second, err := runtime.CallFuture(ctx, pid, Delivery{MessageType: "drain.probe", CommandID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(input, "drain"); err != nil {
		t.Fatal(err)
	}
	expect("gated-with-two-pending")
	for _, accepted := range []bool{false, true} {
		requestCtx, cancelRequest := context.WithTimeout(ctx, time.Second)
		var err error
		if accepted {
			err = runtime.TellAccepted(requestCtx, pid, Delivery{MessageType: "drain.probe", CommandID: 3})
		} else {
			_, err = runtime.Call(requestCtx, pid, Delivery{MessageType: "drain.probe", CommandID: 3})
		}
		cancelRequest()
		var rejection *CallError
		if !errors.As(err, &rejection) || rejection.Code != CallErrorCodeRejected {
			t.Fatalf("late accepted=%v: want rejection, got %v", accepted, err)
		}
	}
	if _, err := fmt.Fprintln(input, "release"); err != nil {
		t.Fatal(err)
	}
	expect("drained-after-1-2")
	for _, future := range []*Future{first, second} {
		if _, err := future.Await(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fmt.Fprintln(input, "stop"); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Log("双进程：两条已准入请求顺序完成；迟到 Call/TellAccepted 明确拒绝；排空后目标正常退出")
}

func startDrainRuntime(t *testing.T, endpoint, prefix string, kinds ...Kind) *Runtime {
	t.Helper()
	runtime, err := New(Config{
		ClusterName: "tx-drain-process", Host: "127.0.0.1", Port: 0, LogLevel: LogLevelError,
		Etcd: EtcdConfig{Endpoints: []string{endpoint}, Prefix: prefix, DialTimeout: time.Second},
	}, kinds...)
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := runtime.Start(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Cancel() })
	return runtime
}

func runDrainProcess(t *testing.T, endpoint, prefix string) {
	release := make(chan struct{})
	processed := make(chan uint64, 4)
	kind := MustFixedKind("probe", func() protoactor.Actor {
		return &drainProcessActor{release: release, processed: processed}
	})
	runtime := startDrainRuntime(t, endpoint, prefix, kind)
	system, err := runtime.ActorSystem()
	if err != nil {
		t.Fatal(err)
	}
	// 使用 Runtime 配置后的真实邮箱，直连 PID 排除集群放置变化对屏障的干扰。
	if _, err := system.Root.SpawnNamed(runtime.fixed[0].props, "drain-probe"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("DRAIN-PROBE", system.Address())
	commands := bufio.NewScanner(os.Stdin)
	if !commands.Scan() || commands.Text() != "drain" {
		t.Fatal("missing drain command")
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.lifecycle.pending() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second request did not enter target mailbox")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = runtime.Drain(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked drain = %v", err)
	}
	fmt.Println("DRAIN-PROBE gated-with-two-pending")
	if !commands.Scan() || commands.Text() != "release" {
		t.Fatal("missing release command")
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{1, 2} {
		select {
		case got := <-processed:
			if got != want {
				t.Fatalf("executed = %d, want %d", got, want)
			}
		default:
			t.Fatal("drained before accepted work completed")
		}
	}
	select {
	case extra := <-processed:
		t.Fatalf("late request executed: %d", extra)
	default:
	}
	fmt.Println("DRAIN-PROBE drained-after-1-2")
	if !commands.Scan() || commands.Text() != "stop" {
		t.Fatal("missing stop command")
	}
	if err := runtime.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

type drainProcessActor struct {
	release   <-chan struct{}
	processed chan<- uint64
}

func (actor *drainProcessActor) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	if message.CommandId == 1 {
		fmt.Println("DRAIN-PROBE started")
		<-actor.release
	}
	actor.processed <- message.CommandId
	ctx.Respond(&actorpb.Result{MessageType: "drain.done", CommandId: message.CommandId, RequestId: message.RequestId})
}

// 验证本地等待、请求截止时间与 Runtime 关闭各自的远端调用边界。
func TestActorRemoteCallLifecycle(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the remote integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/tx/tests/call-contract-%d", time.Now().UnixNano())
	started, release := make(chan struct{}, 1), make(chan struct{}, 1)
	processed := make(chan uint64, 4)
	target := startDrainRuntime(t, endpoint, prefix, MustFixedKind("call-probe", func() protoactor.Actor {
		return &remoteCallContractActor{started: started, release: release, processed: processed}
	}))
	defer target.Shutdown()
	// 失败退出时也解除业务屏障，再关闭 Runtime。
	defer close(release)
	system, err := target.ActorSystem()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := system.Root.SpawnNamed(target.fixed[0].props, "call-probe"); err != nil {
		t.Fatal(err)
	}
	caller := startDrainRuntime(t, endpoint, prefix)
	defer caller.Shutdown()
	pid := protoactor.NewPID(system.Address(), "call-probe")
	future, err := caller.CallFuture(ctx, pid, Delivery{MessageType: "call.probe", CommandID: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitCtx, stopWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopWait()
	if _, err := future.Await(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed out Await = %v", err)
	}
	release <- struct{}{}
	result, err := future.Await(ctx)
	if err != nil || result.CommandID != 1 || result.MessageType != "call.done" || string(result.Payload) != "completed" {
		t.Fatalf("Await after timeout: result=%+v err=%v", result, err)
	}
	_, err = caller.Call(ctx, pid, Delivery{MessageType: "call.probe", CommandID: 2})
	var businessErr *CallError
	if !errors.As(err, &businessErr) || businessErr.Code != 47 || businessErr.Message != "business rejected" {
		t.Fatalf("remote business error = %v", err)
	}
	// 第二个响应形成邮箱顺序屏障，两次显式调用应各执行一次。
	for _, want := range []uint64{1, 2} {
		select {
		case got := <-processed:
			if got != want {
				t.Fatalf("processed=%d, want %d", got, want)
			}
		default:
			t.Fatal("response preceded processing")
		}
	}
	select {
	case got := <-processed:
		t.Fatalf("unexpected replay: %d", got)
	default:
	}

	// 使用独立的 Await context，确保观察的是请求截止时间清理，而非等待超时。
	requestCtx, stopRequest := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stopRequest()
	expired, err := caller.CallFuture(requestCtx, pid, Delivery{MessageType: "call.probe", CommandID: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := expired.Await(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired request = %v", err)
	}
	caller.pending.mu.Lock()
	pending := len(caller.pending.pending)
	caller.pending.mu.Unlock()
	if pending != 0 {
		t.Fatalf("expired request retained %d pending calls", pending)
	}
	// 旧响应与新请求同时在途；旧请求号不得结束新 Future。
	next, err := caller.CallFuture(ctx, pid, Delivery{MessageType: "call.probe", CommandID: 2})
	if err != nil {
		t.Fatal(err)
	}
	release <- struct{}{}
	if _, err := next.Await(ctx); !errors.As(err, &businessErr) || businessErr.Code != 47 {
		t.Fatalf("new request after late response = %v", err)
	}
	if _, err := expired.Await(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late response changed expired result: %v", err)
	}

	stopped, err := caller.CallFuture(ctx, pid, Delivery{MessageType: "call.probe", CommandID: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := caller.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := stopped.Await(ctx); !errors.Is(err, ErrRuntimeStopped) {
		t.Fatalf("pending call on shutdown = %v", err)
	}
	caller.pending.mu.Lock()
	pending = len(caller.pending.pending)
	caller.pending.mu.Unlock()
	if pending != 0 {
		t.Fatalf("shutdown retained %d pending calls", pending)
	}
	if _, err := caller.CallFuture(ctx, pid, Delivery{MessageType: "call.probe", CommandID: 2}); !errors.Is(err, ErrRuntimeStopped) {
		t.Fatalf("call after shutdown = %v", err)
	}
	// 调用方关闭不撤销已准入的远端业务，目标仍须完成排水。
	release <- struct{}{}
	if err := target.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{1, 2, 1} {
		select {
		case got := <-processed:
			if got != want {
				t.Fatalf("processed=%d, want %d", got, want)
			}
		default:
			t.Fatal("drained before remote work completed")
		}
	}

}

type remoteCallContractActor struct {
	started   chan<- struct{}
	release   <-chan struct{}
	processed chan<- uint64
}

func (a *remoteCallContractActor) Receive(ctx protoactor.Context) {
	message, ok := ctx.Message().(*actorpb.Delivery)
	if !ok {
		return
	}
	if message.CommandId == 1 {
		a.started <- struct{}{}
		<-a.release
	}
	a.processed <- message.CommandId
	request, err := DeliveryFromProto(message)
	if err != nil {
		panic(err)
	}
	if message.CommandId == 2 {
		_ = Respond(ctx, request, Result{}, NewCallError(47, errors.New("business rejected")))
		return
	}
	_ = Respond(ctx, request, Result{MessageType: "call.done", CommandID: message.CommandId, Payload: []byte("completed")}, nil)
}
