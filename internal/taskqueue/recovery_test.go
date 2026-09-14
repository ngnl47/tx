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
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	nc, err := nats.Connect(url)
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
	data := []byte(`{"Identity":{"Kind":"room","Key":"expired"},"Delivery":{"MessageType":"buff"}}`)
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
	worker, err := StartRecovery(ctx, notifications, recovery, time.Second)
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
	// 清理后重放同一通知，不建立完成表，也不能再次产生死信。
	if _, err := js.Publish(ctx, subject, originalEvent); err != nil {
		t.Fatal(err)
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
	fresh, err := js.Publish(ctx, sourceName, data)
	if err != nil {
		t.Fatal(err)
	}
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
	t.Log("离线通知捕获、转存失败保留、恢复删除源残留、重复和旧代次通知均通过")
}
