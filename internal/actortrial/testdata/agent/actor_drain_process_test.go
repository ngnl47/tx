package agent

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
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/protocol"
)

// TestActorDispatcherDrainCacheAcrossProcesses 验证缓存确实省去真实 Runtime.Call，
// 而非仅将同一个远端错误改写。身份更新通过发现快照注入，不模拟快速进程重启。
func TestActorDispatcherDrainCacheAcrossProcesses(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the process integration test")
	}
	if prefix := os.Getenv("TX_AGENT_DRAIN_CHILD_PREFIX"); prefix != "" {
		runAgentDrainTarget(t, endpoint, prefix)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/tx/tests/agent-drain-%d", time.Now().UnixNano())
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestActorDispatcherDrainCacheAcrossProcesses$", "-test.v")
	child.Env = append(os.Environ(), "TX_AGENT_DRAIN_CHILD_PREFIX="+prefix)
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
	defer func() {
		_ = input.Close()
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	events := make(chan string, 4)
	go func() {
		defer close(events)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if event, ok := strings.CutPrefix(scanner.Text(), "AGENT-DRAIN "); ok {
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
				t.Fatal("target exited before barrier")
			}
			return event
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return ""
		}
	}
	address := wait()
	runtime := startAgentDrainRuntime(t, endpoint, prefix)
	defer runtime.Shutdown()
	counted := &countedActorRuntime{Runtime: runtime}
	directory := discovery.NewServiceDirectory()
	setIdentity := func(id string) {
		t.Helper()
		if err := directory.Replace([]discovery.Instance{{ID: id, Services: []string{"game"}, Endpoint: "control:1", ActorEndpoint: address, Status: discovery.StatusRunning}}); err != nil {
			t.Fatal(err)
		}
	}
	setIdentity("game-old")
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger: counted, Directory: directory,
		Resolver: ActorDispatchResolverFunc(func(_ context.Context, request DispatchRequest) (ActorDispatch, error) {
			pid, err := txactor.FixedKindPID(address, fmt.Sprintf("probe-%d", request.CommandID))
			return ActorDispatch{Target: pid, Delivery: txactor.Delivery{MessageType: "drain.probe", RouteKey: protocol.Uint64RouteKey(1)}}, err
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := DispatchRequest{Seq: 1, Identity: protocol.SessionIdentity{
		SessionPrincipal: protocol.SessionPrincipal{AccountID: 1, ServerID: 1, RoleID: 1, SessionID: 1}, ConnectionID: 1,
	}}
	call := func(actorID uint32, rejected bool, wantCalls int) {
		t.Helper()
		request.CommandID = actorID
		result, err := dispatcher.Dispatch(ctx, request)
		if rejected {
			var failure *txactor.CallError
			if !errors.As(err, &failure) || failure.Code != txactor.CallErrorCodeRejected {
				t.Fatalf("want rejection, got %v", err)
			}
		} else if err != nil || result.ProcessedUpSeq != 1 {
			t.Fatalf("completion = %+v, %v", result, err)
		}
		if counted.calls != wantCalls {
			t.Fatalf("Runtime.Call count = %d, want %d", counted.calls, wantCalls)
		}
	}
	// 两个固定 Kind 有不同 PID，预先确认两者均可真实执行。
	call(1, false, 1)
	call(2, false, 2)
	if _, err := fmt.Fprintln(input, "drain"); err != nil {
		t.Fatal(err)
	}
	if event := wait(); event != "gated" {
		t.Fatalf("barrier = %q", event)
	}
	call(1, true, 3)
	call(2, true, 3)
	// 等待真实缓存租期，而非为关闭竞态增加延时。
	time.Sleep(1100 * time.Millisecond)
	call(2, true, 4)
	// 刚刷新的旧身份缓存仍有效；新身份必须穿透它，返回旧身份又应命中。
	setIdentity("game-new")
	call(1, true, 5)
	setIdentity("game-old")
	call(2, true, 5)
	if _, err := fmt.Fprintln(input, "stop"); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Log("真实双进程 Runtime.Call 计数：预热 1/2，首次拒绝 3，跨 Actor 命中 3，到期 4，新身份 5，旧身份命中 5")
}

// countedActorRuntime 只在测试边界计数，始终执行真实远端 Call。
type countedActorRuntime struct {
	*txactor.Runtime
	calls int
}

func (runtime *countedActorRuntime) Call(ctx context.Context, pid *protoactor.PID, delivery txactor.Delivery) (txactor.Result, error) {
	runtime.calls++
	return runtime.Runtime.Call(ctx, pid, delivery)
}

func startAgentDrainRuntime(t *testing.T, endpoint, prefix string, kinds ...txactor.Kind) *txactor.Runtime {
	t.Helper()
	runtime, err := txactor.New(txactor.Config{
		ClusterName: "tx-agent-drain", Host: "127.0.0.1", LogLevel: txactor.LogLevelError,
		Etcd: txactor.EtcdConfig{Endpoints: []string{endpoint}, Prefix: prefix, DialTimeout: time.Second},
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

func runAgentDrainTarget(t *testing.T, endpoint, prefix string) {
	producer := func() protoactor.Actor { return agentDrainProbe{} }
	runtime := startAgentDrainRuntime(t, endpoint, prefix,
		txactor.MustFixedKind("probe-1", producer), txactor.MustFixedKind("probe-2", producer))
	defer runtime.Shutdown()
	address, err := runtime.Endpoint()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("AGENT-DRAIN", address)
	commands := bufio.NewScanner(os.Stdin)
	if !commands.Scan() || commands.Text() != "drain" {
		t.Fatal("missing drain command")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("AGENT-DRAIN gated")
	if !commands.Scan() || commands.Text() != "stop" {
		t.Fatal("missing stop command")
	}
}

type agentDrainProbe struct{}

func (agentDrainProbe) Receive(ctx protoactor.Context) {
	if delivery, ok := ctx.Message().(*actorpb.Delivery); ok {
		ctx.Respond(&actorpb.Result{RequestId: delivery.RequestId, ProcessedUpSeq: delivery.UpSeq})
	}
}
