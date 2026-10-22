package framework

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/internal/taskqueue"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/tasks"
	txprotocol "github.com/ngnl47/tx/protocol"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func executeTrialTask(ctx context.Context, actor txactor.Caller, msg jetstream.Msg) error {
	return (taskqueue.Processor{Actor: actor, Timeout: 10 * time.Second}).Process(ctx, msg, 400*time.Millisecond)
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
	drainEntered, drainRelease := make(chan struct{}), make(chan struct{})
	finishDrain := sync.OnceFunc(func() { close(drainRelease) })
	frameworkEntered, frameworkRelease := make(chan struct{}), make(chan struct{})
	finishFramework := sync.OnceFunc(func() { close(frameworkRelease) })
	// 操作身份是业务输入快照，不要求任务执行时原连接仍在线。
	wantDelivery := txactor.Delivery{
		MessageType: "reward", CommandHash: 47,
		RouteKey: txprotocol.StringRouteKey("season.世界.490001"), Payload: []byte{0, 1, 255},
		Identity: &txprotocol.SessionIdentity{SessionPrincipal: txprotocol.SessionPrincipal{
			AccountID: 11, BorrowAccountID: 12, ServerID: 13, RoleID: 14, SessionID: 15,
			AccessMode: txprotocol.AccessModeBorrow, CanCreateRole: true,
		}, ConnectionID: 16},
	}
	kind := txactor.MustType("role", func(c txactor.Context) (*txactor.Behavior, error) {
		if c.Identity.ID == "expired" {
			return nil, txactor.ErrActivationDenied
		}
		return &txactor.Behavior{Receive: func(_ txactor.Context, message any) (any, error) {
			delivery, ok := message.(txactor.Delivery)
			if !ok || !delivery.WantsResult() || delivery.MessageType != wantDelivery.MessageType ||
				delivery.CommandHash != wantDelivery.CommandHash || delivery.RouteKey != wantDelivery.RouteKey ||
				!bytes.Equal(delivery.Payload, wantDelivery.Payload) || delivery.Identity == nil ||
				*delivery.Identity != *wantDelivery.Identity {
				return nil, errors.New("持久任务丢失调用上下文或混入客户端上行序号")
			}
			switch c.Identity.ID {
			case "framework-drain":
				close(frameworkEntered)
				<-frameworkRelease
			case "drain":
				close(drainEntered)
				<-drainRelease
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
	services := []Service{testService{spec: Spec{Name: "taskworker", Requires: []CModID{CModTaskQueue}}}, testService{spec: Spec{Name: "game", ActorTypes: []txactor.Type{kind}}}, testService{spec: Spec{Name: "worker", Requires: []CModID{CModActor}}, start: func(c Context) error { access = c.Actor(); return nil }}}
	start := func(service string) *Server {
		app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", services...)
		app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
		if err := app.Start(ctx, RunOptions{Services: []string{service}, RPCListen: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		return app
	}
	game := start("game")
	defer game.Stop(context.Background())
	worker := start("worker")
	defer worker.Stop(context.Background())
	defer unblock()
	defer finishDrain()
	defer finishFramework()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(worker.discovery.Directory().RunningInstances("game")) != 1 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	publisher := &taskRuntime{js: js, config: tasks.Config{Stream: streamName, Subject: streamName}}
	publish := func(key string) {
		// 发布请求结束不能撤销已经持久化的任务，后续执行使用消费上下文。
		request, end := context.WithCancel(ctx)
		defer end()
		if err := publisher.Publish(request, txactor.Identity{Type: "role", ID: key}, wantDelivery); err != nil {
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
	err = (taskqueue.Processor{Actor: access, JetStream: js, DeadLetter: deadName, Timeout: 10 * time.Second}).Process(attemptCtx, msg, 400*time.Millisecond)
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
	if err := (taskqueue.Processor{Actor: access, JetStream: js, DeadLetter: deadName, Timeout: 10 * time.Second}).Process(ctx, msg, 400*time.Millisecond); err != nil {
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

	// 正式消费适配复用 SDK Drain，自动续期不暴露给业务。
	drainingConn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer drainingConn.Close()
	drainingJS, err := jetstream.New(drainingConn)
	if err != nil {
		t.Fatal(err)
	}
	drainingConsumer, err := drainingJS.Consumer(ctx, streamName, "worker")
	if err != nil {
		t.Fatal(err)
	}
	publish("drain")
	consuming, err := taskqueue.Start(ctx, drainingConsumer, taskqueue.Processor{Actor: access, JetStream: js, DeadLetter: deadName, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		finishDrain()
		if err := consuming.Drain(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-drainEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 等待超时不撤销业务，自动续期必须在消费排水期间继续运行。
	waitCtx, stopWait := context.WithTimeout(ctx, 20*time.Millisecond)
	err = consuming.Drain(waitCtx)
	stopWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("排水应等待未结束业务: %v", err)
	}
	publish("temporary")
	// 续期由正式 Worker 自动执行，测试仅发起竞争领取。
	// 跨过 AckWait 后仍不能领取原任务或下一条任务，验证续期与共享在途上限。
	renewal := time.NewTicker(400 * time.Millisecond)
	defer renewal.Stop()
	holdUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(holdUntil) {
		select {
		case <-renewal.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		unexpected, err := next.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("续期或在途背压失效: msg=%v err=%v", unexpected, err)
		}
	}
	renewal.Stop()
	ci, err = next.Info(ctx)
	if err != nil || ci.NumAckPending != 1 || ci.NumPending != 1 || completed.Load() != 3 {
		t.Fatalf("慢任务期间消费状态: %+v %v completed=%d", ci, err, completed.Load())
	}
	finishDrain()
	if err := consuming.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	ci, err = next.Info(ctx)
	if err != nil || ci.NumAckPending != 0 || ci.NumPending != 1 || completed.Load() != 4 {
		t.Fatalf("慢任务完成后的消费状态: %+v %v completed=%d", ci, err, completed.Load())
	}
	drainingConn.Close()
	// 慢任务释放在途名额后，排水期间积压的任务由替代消费者首次领取。
	info, err = stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("排水后新任务丢失: %+v %v", info, err)
	}
	msg, err = next.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	meta, err = msg.Metadata()
	if err != nil || meta.NumDelivered != 1 {
		t.Fatalf("排水后新任务被旧消费者领取: %+v %v", meta, err)
	}
	if err := executeTrialTask(ctx, access, msg); err != nil {
		t.Fatal(err)
	}
	assertEmpty(5)

	// 有限投递必须先部署通知捕获。失败启动不能悄悄开启缺少恢复能力的消费。
	ci, err = consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	limited := ci.Config
	limited.MaxDeliver = 1
	if _, err := stream.UpdateConsumer(ctx, limited); err != nil {
		t.Fatal(err)
	}
	missingRecovery := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", services...)
	missingRecovery.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
	missingRecovery.config.TaskQueue = tasks.Config{URL: url, Stream: streamName, Subject: streamName, Consumer: "worker", DeadLetter: deadName, Timeout: 10 * time.Second}
	if err := missingRecovery.Start(ctx, RunOptions{Services: []string{"taskworker"}, RPCListen: "127.0.0.1:0"}); err == nil || !strings.Contains(err.Error(), "finite max deliver requires recovery") {
		missingRecovery.Stop(context.Background())
		t.Fatalf("有限 MaxDeliver 应因缺少恢复配置拒绝启动: %v", err)
	}
	eventName := streamName + "_events"
	eventSubject := taskqueue.MaxDeliverySubject(streamName, "worker")
	events, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: eventName, Subjects: []string{eventSubject}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, Metadata: map[string]string{"owner": owner}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败通知流", eventName)
			return
		}
		c, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		info, err := events.Info(c)
		if err != nil || info.Config.Metadata["owner"] != owner {
			t.Errorf("通知流归属校验: %v", err)
			return
		}
		if err := js.DeleteStream(c, eventName); err != nil {
			t.Error(err)
		}
	}()
	if _, err := events.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "recovery", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second}); err != nil {
		t.Fatal(err)
	}
	publish("exhausted-before-start")
	if _, err := next.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Next(jetstream.FetchMaxWait(3 * time.Second)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("耗尽后继续投递: %v", err)
	}
	eventInfo, err := events.Info(ctx)
	if err != nil || eventInfo.State.Msgs != 1 {
		t.Fatalf("恢复服务离线通知未捕获: %+v %v", eventInfo, err)
	}
	// 正式 CMod 与本地 Game 共址，验证 Server.Stop 先排消费，再关闭 Actor 准入。
	if err := game.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", services...)
	app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
	app.config.TaskQueue = tasks.Config{URL: url, Stream: streamName, Subject: streamName, Consumer: "worker", DeadLetter: deadName, Timeout: 10 * time.Second, RecoveryStream: eventName, RecoveryConsumer: "recovery"}
	if err := app.Start(ctx, RunOptions{Services: []string{"game", "taskworker"}, RPCListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		finishFramework()
		if err := app.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	// 恢复消费者处理启动前积压的通知，不执行该任务业务。
	for {
		info, err := events.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	id := txactor.Identity{Type: "role", ID: "framework-drain"}
	if err := newServiceContext(app.runtime, nil).TaskQueue().Publish(ctx, id, wantDelivery); err != nil {
		t.Fatal(err)
	}
	select {
	case <-frameworkEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	local := app.runtime.entities.Probe(id)
	if local.State != txactor.Ready {
		t.Fatalf("任务未由同实例 Actor 执行: %+v", local)
	}
	stopCtx, cancelStop := context.WithTimeout(ctx, 20*time.Millisecond)
	err = app.Stop(stopCtx)
	cancelStop()
	if !errors.Is(err, errStopDeferred) {
		t.Fatalf("未结束任务应保留关闭资源: %v", err)
	}
	if app.runtime == nil || app.runtime.taskQueue.conn.IsClosed() {
		t.Fatal("排水超时提前关闭任务连接")
	}
	finishFramework()
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	assertEmpty(6)
	deadInfo, err = dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 2 {
		t.Fatalf("框架恢复未转存耗尽任务: %+v %v", deadInfo, err)
	}
	t.Log("CMod 启动校验和离线积压通知恢复通过")
	t.Log("CMod 持久发布、同实例 Actor 消费、Stop 超时保留及再次 Stop 收尾均通过")
	t.Log("InProgress 跨 AckWait 续期、共享在途背压及排水后替代消费者续领均通过")
	t.Log("业务明确结束、临时失败重投、永久加载拒绝先转存再确认均通过")
	t.Log("FileStorage 无消费者保留、正式 Actor 完成后 ACK、消费进程退出后 durable 重投均通过")
}
