package framework

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/internal/taskqueue"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/tasks"
)

// 只创建本测试拥有的临时流，直接验证真实 CMod 的准备阶段，不启动业务或集群。
func TestTaskRecoveryPolicyFailsDuringCModPreparation(t *testing.T) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		t.Skip("需要本地 JetStream")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("TX_STARTUP_%d", time.Now().UnixNano())
	create := func(config jetstream.StreamConfig) jetstream.Stream {
		stream, err := js.CreateStream(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			c, end := context.WithTimeout(context.Background(), time.Second)
			defer end()
			if err := js.DeleteStream(c, config.Name); err != nil {
				t.Error(err)
			}
		})
		return stream
	}
	source := create(jetstream.StreamConfig{Name: name, Subjects: []string{name}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy})
	create(jetstream.StreamConfig{Name: name + "_dead", Subjects: []string{name + ".dead"}, Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy})
	events := create(jetstream.StreamConfig{Name: name + "_events", Subjects: []string{taskqueue.MaxDeliverySubject(name, "worker")}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy})
	if _, err := source.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxDeliver: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "recovery", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxDeliver: 1}); err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(ctx)
	defer func() { root.Cancel(); root.Wait() }()
	app := &Server{runtime: &runtime{lifecycle: root}, config: Config{TaskQueue: tasks.Config{URL: url, Stream: name, Subject: name, Consumer: "worker", DeadLetter: name + ".dead", Timeout: time.Second, RecoveryStream: name + "_events", RecoveryConsumer: "recovery"}}}
	mod := app.tasksCMod()
	defer mod.(CModStopper).Stop(context.Background())
	if err := mod.Start(nil); err == nil || !strings.Contains(err.Error(), "without max deliver") {
		t.Fatalf("恢复策略应在 CMod 准备阶段拒绝: %v", err)
	}
}
