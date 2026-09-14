package framework

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
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

// 只有明确的永久加载拒绝转死信；其他错误保留原任务等待消费策略处理。
// 转存成功而源 ACK 丢失仍可重复，不建立完成表或宣称跨流原子事务。
func executeTrialTaskWithDeadLetter(ctx context.Context, actor txactor.Caller, js jetstream.JetStream, subject string, msg jetstream.Msg) error {
	err := executeTrialTask(ctx, actor, msg)
	if !errors.Is(err, txactor.ErrActivationDenied) {
		return err
	}
	dead := &nats.Msg{Subject: subject, Data: msg.Data(), Header: nats.Header{}}
	dead.Header.Set("Task-Failure", txactor.ErrActivationDenied.Error())
	dead.Header.Set("Task-Source", msg.Subject())
	if _, err := js.PublishMsg(ctx, dead); err != nil {
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
	var recoverBusiness atomic.Bool
	kind := txactor.MustKind("role", func(c txactor.Context) (*txactor.Behavior, error) {
		if c.Identity.Key == "expired" {
			return nil, txactor.ErrActivationDenied
		}
		return &txactor.Behavior{Receive: func(_ txactor.Context, _ any) (any, error) {
			switch c.Identity.Key {
			case "finished":
				return txactor.Result{MessageType: "task.finished"}, nil
			case "temporary":
				if !recoverBusiness.Load() {
					return nil, errors.New("temporary business failure")
				}
			default:
				entered <- struct{}{}
			}
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
	publish := func(key string) {
		data, err := json.Marshal(trialTask{Identity: txactor.Identity{Kind: "role", Key: key}, Delivery: txactor.Delivery{MessageType: "reward"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, streamName, data); err != nil {
			t.Fatal(err)
		}
	}
	publish("reward")
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
	publish("reward")
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

	// 业务明确结束也是成功消费，区别于框架加载拒绝。
	publish("finished")
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := executeTrialTask(ctx, access, msg); err != nil {
		t.Fatal(err)
	}
	assertEmpty(2)

	// 普通错误不 ACK；恢复后由 durable 重新投递，不自行循环重试 RPC。
	publish("temporary")
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := executeTrialTask(ctx, access, msg); err == nil {
		t.Fatal("业务失败被确认为成功")
	}
	ci, err = next.Info(ctx)
	if err != nil || ci.NumAckPending != 1 {
		t.Fatalf("临时失败未保留: %+v %v", ci, err)
	}
	recoverBusiness.Store(true)
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	meta, err = msg.Metadata()
	if err != nil || meta.NumDelivered < 2 {
		t.Fatalf("临时失败未重投: %+v %v", meta, err)
	}
	if err := executeTrialTask(ctx, access, msg); err != nil {
		t.Fatal(err)
	}
	assertEmpty(3)

	// 死信流未创建时转存必须失败，并保留源任务。创建后重新领取并转存。
	publish("expired")
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	deadName := streamName + "_dead"
	attemptCtx, endAttempt := context.WithTimeout(ctx, 200*time.Millisecond)
	err = executeTrialTaskWithDeadLetter(attemptCtx, access, js, deadName, msg)
	endAttempt()
	if err == nil {
		t.Fatal("死信转存失败却确认源任务")
	}
	ci, err = next.Info(ctx)
	if err != nil || ci.NumAckPending != 1 {
		t.Fatalf("死信转存失败未保留: %+v %v", ci, err)
	}
	dead, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: deadName, Subjects: []string{deadName}, Storage: jetstream.FileStorage, Replicas: 1, Metadata: map[string]string{"owner": owner}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败死信流", deadName)
			return
		}
		c, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		info, err := dead.Info(c)
		if err != nil || info.Config.Metadata["owner"] != owner {
			t.Errorf("死信流归属校验: %v", err)
			return
		}
		if err := js.DeleteStream(c, deadName); err != nil {
			t.Error(err)
		}
	}()
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := executeTrialTaskWithDeadLetter(ctx, access, js, deadName, msg); err != nil {
		t.Fatal(err)
	}
	assertEmpty(3)
	deadInfo, err := dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 1 {
		t.Fatalf("死信未持久保留: %+v %v", deadInfo, err)
	}
	stored, err := dead.GetMsg(ctx, deadInfo.State.FirstSeq)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != string(msg.Data()) || stored.Header.Get("Task-Failure") != txactor.ErrActivationDenied.Error() {
		t.Fatalf("死信丢失原任务或拒绝原因: %+v", stored)
	}
	t.Log("业务明确结束、临时失败重投、永久加载拒绝先转存再确认均通过")
	t.Log("FileStorage 无消费者保留、正式 Actor 完成后 ACK、消费进程退出后 durable 重投均通过")
}
