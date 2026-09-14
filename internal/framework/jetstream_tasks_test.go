package framework

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	txactor "github.com/ngnl47/tx/modules/actor"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 仅用于闭环验证；公共任务协议与消费运行时留待完整 T5 收敛。
type trialTask struct {
	Identity txactor.Identity
	Delivery txactor.Delivery
}

func executeTrialTask(ctx context.Context, actor txactor.Caller, msg jetstream.Msg) error {
	var task trialTask
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		return err
	}
	if _, err := actor.Call(ctx, task.Identity, task.Delivery); err != nil {
		return err
	}
	return msg.DoubleAck(ctx)
}

// 子进程领取后直接退出，不执行业务、不 ACK；同一 durable 应允许替代进程续领。
func TestJetStreamConsumerProcess(t *testing.T) {
	stream := os.Getenv("TX_JS_CHILD_STREAM")
	if stream == "" {
		t.Skip("仅由闭环测试启动")
	}
	nc, err := nats.Connect(os.Getenv("TX_ACTORTRIAL_NATS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.Consumer(t.Context(), stream, "worker")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := msg.Metadata()
	if err != nil || meta.NumDelivered != 1 {
		t.Fatalf("首次领取: %+v %v", meta, err)
	}
}

func TestJetStreamFormalActorTaskLoop(t *testing.T) {
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Skip("设置 NATS/etcd 地址验证持久任务闭环")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	owner := rand.Text()
	prefix, streamName := "/tx/tests/jetstream/"+owner, "tx_task_"+owner
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	marker := prefix + "/owner"
	created, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Version(marker), "=", 0)).Then(clientv3.OpPut(marker, owner)).Commit()
	if err != nil || !created.Succeeded {
		t.Fatalf("隔离范围: %v", err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败范围", prefix)
			return
		}
		c, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		result, err := client.Txn(c).If(clientv3.Compare(clientv3.Value(marker), "=", owner)).Then(clientv3.OpDelete(prefix+"/", clientv3.WithPrefix())).Commit()
		if err != nil || !result.Succeeded {
			t.Errorf("清理隔离范围: %v", err)
		}
	}()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: streamName, Subjects: []string{streamName}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, Replicas: 1, Metadata: map[string]string{"owner": owner}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败流", streamName)
			return
		}
		c, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		info, err := stream.Info(c)
		if err != nil || info.Config.Metadata["owner"] != owner {
			t.Errorf("流归属校验: %v", err)
			return
		}
		if err := js.DeleteStream(c, streamName); err != nil {
			t.Error(err)
		}
	}()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var completed atomic.Int32
	kind := txactor.MustKind("role", func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(_ txactor.Context, _ any) (any, error) {
			entered <- struct{}{}
			<-release
			completed.Add(1)
			return txactor.Result{MessageType: "done"}, nil
		}}, nil
	})
	var access txactor.Runtime
	services := []Service{testService{spec: Spec{Name: "game", ActorKinds: []txactor.Kind{kind}}}, testService{spec: Spec{Name: "worker", Requires: []CModID{CModActor}}, start: func(c Context) error { access = c.Actor(); return nil }}}
	start := func(service string) *Server {
		app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", services...)
		app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
		if err := app.Start(ctx, RunOptions{Services: service, RPCListen: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		return app
	}
	game := start("game")
	defer game.Stop(context.Background())
	worker := start("worker")
	defer worker.Stop(context.Background())
	defer unblock()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(worker.discovery.Directory().RunningInstances("game")) != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	publish := func() {
		data, err := json.Marshal(trialTask{Identity: txactor.Identity{Kind: "role", Key: "reward"}, Delivery: txactor.Delivery{MessageType: "reward"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, streamName, data); err != nil {
			t.Fatal(err)
		}
	}
	publish()
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("无消费者持久投递: %+v %v", info, err)
	}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 2 * time.Second, MaxAckPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- executeTrialTask(ctx, access, msg) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	ci, err := consumer.Info(ctx)
	if err != nil || ci.NumAckPending != 1 || completed.Load() != 0 {
		t.Fatalf("业务完成前提前确认: %+v %v", ci, err)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertEmpty := func(want int32) {
		ci, err := consumer.Info(ctx)
		if err != nil || ci.NumAckPending != 0 || completed.Load() != want {
			t.Fatalf("消费确认: %+v %v completed=%d", ci, err, completed.Load())
		}
		info, err := stream.Info(ctx)
		if err != nil || info.State.Msgs != 0 {
			t.Fatalf("确认后任务未移除: %+v %v", info, err)
		}
	}
	assertEmpty(1)
	publish()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJetStreamConsumerProcess$", "-test.v")
	child.Env = append(os.Environ(), "TX_JS_CHILD_STREAM="+streamName)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("消费进程: %v\n%s", err, output)
	}
	// 新连接重新绑定同一 durable；不创建另一份消费进度。
	replacement, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	nextJS, err := jetstream.New(replacement)
	if err != nil {
		t.Fatal(err)
	}
	next, err := nextJS.Consumer(ctx, streamName, "worker")
	if err != nil {
		t.Fatal(err)
	}
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := msg.Metadata()
	if err != nil || meta.NumDelivered < 2 {
		t.Fatalf("未续领原任务: %+v %v", meta, err)
	}
	if err := executeTrialTask(ctx, access, msg); err != nil {
		t.Fatal(err)
	}
	assertEmpty(2)
	t.Log("FileStorage 无消费者保留、正式 Actor 完成后 ACK、消费进程退出后 durable 重投均通过")
}
