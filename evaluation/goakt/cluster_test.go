package goakteval

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/etcd"
	"github.com/tochemey/goakt/v4/remote"
)

// 两个真实进程必须命中同一个角色；发送端关闭入口不能冒充接收端排水。
// HTTP 仅用于控制探针，业务调用始终经过 Runtime 和 GoAkt 集群。
func TestEtcdTwoProcessDelivery(t *testing.T) {
	if os.Getenv("GOAKT_EVAL_ETCD") == "" {
		t.Skip("set GOAKT_EVAL_ETCD=127.0.0.1:2379 to run two real processes")
	}
	name := fmt.Sprintf("tx-role-eval-%d", time.Now().UnixNano())
	client := &http.Client{Timeout: 10 * time.Second}
	get := func(url string) (string, error) {
		resp, err := client.Get(url)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("status %d: %s", resp.StatusCode, body)
		}
		return string(body), err
	}
	start := func(label string) (string, int, func()) {
		control := evalPort(t)
		cmd := exec.Command(os.Args[0], "-test.run=^TestClusterProcess$", "-test.v")
		cmd.Dir = t.TempDir()
		logPath := filepath.Join(cmd.Dir, "node.log")
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = log, log
		cmd.Env = append(os.Environ(), "GOAKT_EVAL_NODE="+label, "GOAKT_EVAL_NAME="+name,
			fmt.Sprintf("GOAKT_EVAL_HTTP=%d", control), fmt.Sprintf("GOAKT_EVAL_REMOTE=%d", evalPort(t)),
			fmt.Sprintf("GOAKT_EVAL_DISCOVERY=%d", evalPort(t)), fmt.Sprintf("GOAKT_EVAL_PEERS=%d", evalPort(t)))
		if err = cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		exited := make(chan struct{})
		var exitErr error
		go func() { exitErr = cmd.Wait(); close(exited) }()
		t.Cleanup(func() {
			select {
			case <-exited:
			default:
				_ = cmd.Process.Kill()
				<-exited
			}
			_ = log.Close()
			if t.Failed() {
				b, _ := os.ReadFile(logPath)
				t.Logf("node %s: %s", label, b)
			}
		})
		wait := func() {
			select {
			case <-exited:
				if exitErr != nil {
					t.Fatalf("node %s exit: %v", label, exitErr)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("node %s did not exit", label)
			}
		}
		url := fmt.Sprintf("http://127.0.0.1:%d", control)
		until := time.Now().Add(40 * time.Second)
		for time.Now().Before(until) {
			if _, err = get(url + "/ready"); err == nil {
				return url, cmd.Process.Pid, wait
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("node %s startup: %v", label, err)
		return "", 0, nil
	}
	a, pidA, waitA := start("a")
	// 在第二个节点加入前激活角色，随后用响应中的 PID 证明请求确实跨进程。
	if _, err := get(a + "/call"); err != nil {
		t.Fatal(err)
	}
	b, pidB, waitB := start("b")
	var value string
	var err error
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		value, err = get(b + "/call")
		if err == nil && strings.HasPrefix(value, fmt.Sprintf("%d:", pidA)) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil || !strings.HasPrefix(value, fmt.Sprintf("%d:", pidA)) {
		t.Fatalf("remote target=%q err=%v", value, err)
	}
	for _, path := range []string{"/tell", "/accepted", "/future", "/error"} {
		if value, err = get(b + path); err != nil {
			t.Fatalf("%s: %s %v", path, value, err)
		}
	}
	// 关闭 A 的调用入口，B 仍运行。当前阶段必须准确记录这一缺口。
	if _, err = get(a + "/drain"); err != nil {
		t.Fatal(err)
	}
	if _, err = get(a + "/call"); err == nil {
		t.Fatal("local admission remained open")
	}
	value, err = get(b + "/call")
	if err != nil || !strings.HasPrefix(value, fmt.Sprintf("%d:", pidA)) {
		t.Fatalf("failed shutdown did not retain target: %s %v", value, err)
	}
	t.Logf("namespace=%s targetPID=%d callerPID=%d retained remote result=%s; cluster shutdown explicitly unsupported", name, pidA, pidB, value)
	for _, url := range []string{b, a} {
		if _, err = get(url + "/stop"); err != nil {
			t.Fatal(err)
		}
	}
	waitB()
	waitA()
}

func evalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestClusterProcess(t *testing.T) {
	if os.Getenv("GOAKT_EVAL_NODE") == "" {
		t.Skip("subprocess helper")
	}
	port := func(key string) int {
		n, err := strconv.Atoi(os.Getenv("GOAKT_EVAL_" + key))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	name, discoveryPort := os.Getenv("GOAKT_EVAL_NAME"), port("DISCOVERY")
	provider := etcd.NewDiscovery(&etcd.Config{Endpoints: []string{os.Getenv("GOAKT_EVAL_ETCD")}, ActorSystemName: name,
		Host: "127.0.0.1", DiscoveryPort: discoveryPort, TTL: 5, DialTimeout: 3 * time.Second, Timeout: 3 * time.Second})
	cfg := actor.NewClusterConfig().WithDiscovery(provider).WithDiscoveryPort(discoveryPort).WithPeersPort(port("PEERS")).
		WithGrains(new(roleGrain)).WithReplicaCount(1).WithMinimumPeersQuorum(1).WithPartitionCount(17)
	r, err := Start(context.Background(), name, 32, 32, time.Hour, func(context.Context, int64) (Handler, error) {
		count := 0
		return func(_ context.Context, d *actorpb.Delivery) (*actorpb.Result, error) {
			if d.MessageType == "error" {
				return nil, deniedError{}
			}
			count++
			return &actorpb.Result{MessageType: "result", Payload: []byte(fmt.Sprintf("%d:%d", os.Getpid(), count))}, nil
		}, nil
	}, actor.WithRemote(remote.NewConfig("127.0.0.1", port("REMOTE"))), actor.WithCluster(cfg), actor.WithShutdownTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
		defer cancel()
		message := request(1)
		if os.Getenv("GOAKT_EVAL_NODE") == "b" {
			message = request(2)
		}
		var result *actorpb.Result
		var err error
		switch req.URL.Path {
		case "/ready":
		case "/tell":
			err = r.Tell(message)
		case "/accepted":
			err = r.TellAccepted(ctx, message)
		case "/future":
			var f *Future
			f, err = r.CallFuture(ctx, message)
			if err == nil {
				result, err = f.Await(ctx)
			}
		case "/call":
			result, err = r.Call(ctx, message)
		case "/error":
			message.MessageType = "error"
			_, err = r.Call(ctx, message)
			if e, ok := err.(*CallError); ok && e.Code == 17 && e.Message == "not allowed" {
				err = nil
			} else {
				err = fmt.Errorf("coded error mismatch: %v", err)
			}
		case "/drain":
			flushed := false
			err = r.Shutdown(ctx, func(context.Context) error { flushed = true; return nil })
			if !errors.Is(err, ErrClusterDrainUnsupported) || flushed || !r.system.Running() {
				err = fmt.Errorf("unsafe shutdown: err=%v flushed=%v running=%v", err, flushed, r.system.Running())
			} else {
				err = nil
			}
		case "/stop":
			err = r.Stop(context.Background())
			close(done)
		default:
			http.NotFound(w, req)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		if result != nil {
			_, _ = w.Write(result.Payload)
		} else {
			fmt.Fprint(w, "ok")
		}
	})
	server := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", port("HTTP")), Handler: mux}
	go server.ListenAndServe()
	<-done
	_ = server.Shutdown(context.Background())
}
