package taskqueue

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestMaxDeliverRecovery(t *testing.T) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("设置 NATS 地址验证耗尽通知持久恢复")
	}
	runMaxDeliverRecovery(t, url, nil)
}

func runMaxDeliverRecovery(t *testing.T, url string, restart func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	nc, err := nats.Connect(url, nats.ReconnectWait(25*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	owner := rand.Text()
	sourceName, deadName, eventName := "tx_recover_"+owner, "tx_dead_"+owner, "tx_events_"+owner
	subject := MaxDeliverySubject(sourceName, "worker")
	create := func(name, subject string, retention jetstream.RetentionPolicy) jetstream.Stream {
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Storage: jetstream.FileStorage, Retention: retention, Metadata: map[string]string{"owner": owner}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if t.Failed() {
				t.Log("保留失败流", name)
				return
			}
			clean, end := context.WithTimeout(context.Background(), time.Second)
			defer end()
			info, err := stream.Info(clean)
			if err != nil || info.Config.Metadata["owner"] != owner {
				t.Errorf("清理归属校验: %v", err)
				return
			}
			if err := js.DeleteStream(clean, name); err != nil {
				t.Error(err)
			}
		})
		return stream
	}
	source := create(sourceName, sourceName, jetstream.WorkQueuePolicy)
	events := create(eventName, subject, jetstream.WorkQueuePolicy)
	consumer, err := source.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 100 * time.Millisecond, MaxDeliver: 1})
	if err != nil {
		t.Fatal(err)
	}
	notifications, err := events.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "recovery", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"Identity":{"Type":"room","ID":"expired"},"Delivery":{"MessageType":"buff"}}`)
	published, err := js.Publish(ctx, sourceName, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = consumer.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = consumer.Next(jetstream.FetchMaxWait(300 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("耗尽后不应继续投递: %v", err)
	}
	// 恢复处理器尚未启动，服务端仍应将通知持久保存。
	event, err := notifications.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	originalEvent := append([]byte(nil), event.Data()...)
	sourceInfo, err := source.Info(ctx)
	if err != nil || sourceInfo.State.Msgs != 1 {
		t.Fatalf("耗尽源消息未保留: %+v %v", sourceInfo, err)
	}
	recovery := Recovery{JetStream: js, Source: source, Consumer: consumer, DeadLetter: deadName}
	attempt, end := context.WithTimeout(ctx, 150*time.Millisecond)
	err = recovery.Process(attempt, event)
	end()
	if err == nil {
		t.Fatal("无死信流却成功恢复")
	}
	if _, err = source.GetMsg(ctx, published.Sequence); err != nil {
		t.Fatalf("转存失败删除源任务: %v", err)
	}
	info, err := notifications.Info(ctx)
	if err != nil || info.NumAckPending != 1 {
		t.Fatalf("转存失败确认了通知: %+v %v", info, err)
	}
	dead := create(deadName, deadName, jetstream.LimitsPolicy)
	preparedInfo, err := notifications.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareRecovery(notifications, preparedInfo, recovery, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := prepared.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Drain(context.Background())
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		info, err := events.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := worker.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := source.GetMsg(ctx, published.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("源残留未删除: %v", err)
	}
	deadInfo, err := dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 1 {
		t.Fatalf("死信未持久转存: %+v %v", deadInfo, err)
	}
	stored, err := dead.GetMsg(ctx, deadInfo.State.FirstSeq)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != string(data) || stored.Header.Get("Task-Sequence") != strconv.FormatUint(published.Sequence, 10) || stored.Header.Get("Task-Failure") != "max_deliver" {
		t.Fatalf("死信内容或定位缺失: %+v", stored)
	}
	fresh, err := js.Publish(ctx, sourceName, data)
	if err != nil {
		t.Fatal(err)
	}
	// 清理后重放同一通知，不建立完成表，也不能再次产生死信。
	if _, err := js.Publish(ctx, subject, originalEvent); err != nil {
		t.Fatal(err)
	}
	if restart != nil {
		restart()
		for {
			if err := nc.FlushTimeout(100 * time.Millisecond); err == nil {
				break
			}
			select {
			case <-time.After(25 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if _, err := source.GetMsg(ctx, fresh.Sequence); err != nil {
			t.Fatalf("Broker 重启丢失已接受源任务: %v", err)
		}
		stored, err := dead.GetMsg(ctx, deadInfo.State.FirstSeq)
		if err != nil || string(stored.Data) != string(data) {
			t.Fatalf("Broker 重启丢失死信: %+v %v", stored, err)
		}
	}
	event, err = notifications.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Process(ctx, event); err != nil {
		t.Fatal(err)
	}
	deadInfo, err = dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 1 {
		t.Fatalf("源已移除仍重复转存: %+v %v", deadInfo, err)
	}
	// 历史资源代次的通知不能按重用序号删除新任务。
	var stale map[string]any
	if err := json.Unmarshal(originalEvent, &stale); err != nil {
		t.Fatal(err)
	}
	stale["timestamp"] = sourceInfo.Created.Add(-time.Second).Format(time.RFC3339Nano)
	stale["stream_seq"] = fresh.Sequence
	encoded, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, subject, encoded); err != nil {
		t.Fatal(err)
	}
	event, err = notifications.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Process(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := source.GetMsg(ctx, fresh.Sequence); err != nil {
		t.Fatalf("旧通知删除新任务: %v", err)
	}
	// 资源满额使用拒绝新写入，不能静默淘汰已接受任务。
	for _, stream := range []jetstream.Stream{source, dead} {
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		config := info.Config
		config.MaxMsgs, config.Discard = 1, jetstream.DiscardNew
		if _, err := js.UpdateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := js.Publish(ctx, sourceName, data); err == nil {
		t.Fatal("源流满额仍接受新任务")
	}
	if _, err := source.GetMsg(ctx, fresh.Sequence); err != nil {
		t.Fatalf("源流满额淘汰已接受任务: %v", err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(300 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("满额场景未耗尽: %v", err)
	}
	event, err = notifications.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.Process(ctx, event); err == nil {
		t.Fatal("死信流满额仍报告转存成功")
	}
	if _, err := source.GetMsg(ctx, fresh.Sequence); err != nil {
		t.Fatalf("死信满额时删除了源任务: %v", err)
	}
	info, err = notifications.Info(ctx)
	if err != nil || info.NumAckPending != 1 {
		t.Fatalf("死信满额时确认了恢复通知: %+v %v", info, err)
	}
	if err := dead.DeleteMsg(ctx, deadInfo.State.FirstSeq); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Process(ctx, event); err != nil {
		t.Fatalf("释放死信容量后未恢复: %v", err)
	}
	if _, err := source.GetMsg(ctx, fresh.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("释放容量后源残留未清理: %v", err)
	}

	deadInfo, err = dead.Info(ctx)
	if err != nil || deadInfo.State.Msgs != 1 {
		t.Fatalf("释放容量后死信数量错误: %+v %v", deadInfo, err)
	}
	stored, err = dead.GetMsg(ctx, deadInfo.State.FirstSeq)
	if err != nil || stored.Header.Get("Task-Sequence") != strconv.FormatUint(fresh.Sequence, 10) || string(stored.Data) != string(data) {
		t.Fatalf("释放容量后死信内容错误: %+v %v", stored, err)
	}

	// 通知是 Core 发布：用已知内容占满捕获流，观察真实耗尽通知被拒绝后的缺口。
	eventInfo, err := events.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eventConfig := eventInfo.Config
	eventConfig.MaxMsgs, eventConfig.Discard = 1, jetstream.DiscardNew
	if _, err := js.UpdateStream(ctx, eventConfig); err != nil {
		t.Fatal(err)
	}
	filler, err := js.Publish(ctx, subject, originalEvent)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	defer observed.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	stranded, err := js.Publish(ctx, sourceName, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(jetstream.FetchMaxWait(300 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("通知满额场景未耗尽: %v", err)
	}
	advisory, err := observed.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("未观察到服务端耗尽通知: %v", err)
	}
	var exhausted struct {
		Sequence uint64 `json:"stream_seq"`
	}
	if err := json.Unmarshal(advisory.Data, &exhausted); err != nil || exhausted.Sequence != stranded.Sequence {
		t.Fatalf("耗尽通知未指向目标任务: %s %v", advisory.Data, err)
	}
	eventInfo, err = events.Info(ctx)
	if err != nil || eventInfo.State.Msgs != 1 || eventInfo.State.LastSeq != filler.Sequence {
		t.Fatalf("通知流未拒绝新通知: %+v %v", eventInfo, err)
	}
	if err := events.DeleteMsg(ctx, filler.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := notifications.Next(jetstream.FetchMaxWait(300 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("容量恢复后出现预期外通知: %v", err)
	}
	if _, err := source.GetMsg(ctx, stranded.Sequence); err != nil {
		t.Fatalf("捕获缺口下源任务未保留: %v", err)
	}
	// 运维直接读取源记录，不领取任务、不改变 durable 的投递/确认位置。
	before, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(struct {
		Sequence uint64 `json:"seq"`
	}{stranded.Sequence})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := nc.RequestWithContext(ctx, "$JS.API.STREAM.MSG.GET."+sourceName, request)
	if err != nil {
		t.Fatal(err)
	}
	var inspected struct {
		Error   *jetstream.APIError `json:"error"`
		Message struct {
			Sequence uint64 `json:"seq"`
			Data     []byte `json:"data"`
		} `json:"message"`
	}
	if err := json.Unmarshal(reply.Data, &inspected); err != nil || inspected.Error != nil || inspected.Message.Sequence != stranded.Sequence || string(inspected.Message.Data) != string(data) {
		t.Fatalf("只读源记录核对失败: %s %v", reply.Data, err)
	}
	after, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Delivered.Consumer != after.Delivered.Consumer || before.Delivered.Stream != after.Delivered.Stream || before.AckFloor.Consumer != after.AckFloor.Consumer || before.AckFloor.Stream != after.AckFloor.Stream || before.NumPending != after.NumPending || before.NumAckPending != after.NumAckPending || before.NumRedelivered != after.NumRedelivered {
		t.Fatalf("只读核对改变消费状态: before=%+v after=%+v", before, after)
	}
	t.Logf("无通知残留: source_sequence=%d pending=%d ack_pending=%d redelivered=%d；直接读取未改变消费状态", stranded.Sequence, after.NumPending, after.NumAckPending, after.NumRedelivered)
	t.Log("源流满额拒绝、死信满额保留并恢复、通知满额捕获缺口均通过")
}
