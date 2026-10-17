package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/modules/actor"
)

type taskMessage struct {
	jetstream.Msg
	data  []byte
	acked bool
}

func (m *taskMessage) Data() []byte                    { return m.data }
func (m *taskMessage) DoubleAck(context.Context) error { m.acked = true; return nil }

type taskCaller func(context.Context, actor.Identity, actor.Delivery) (actor.Result, error)

func (f taskCaller) Call(ctx context.Context, id actor.Identity, d actor.Delivery) (actor.Result, error) {
	return f(ctx, id, d)
}

func TestProcessorMarksTaskAndWaitsForCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "failed"}[fail], func(t *testing.T) {
			data, err := json.Marshal(Task{Identity: actor.Identity{Kind: "role", Key: "1"}, Delivery: actor.Delivery{Origin: actor.OriginClient, MessageType: "custom.business"}})
			if err != nil {
				t.Fatal(err)
			}
			msg := &taskMessage{data: data}
			entered := make(chan actor.Delivery, 1)
			release := make(chan struct{})
			want := errors.New("业务失败")
			p := Processor{Timeout: time.Second, Actor: taskCaller(func(ctx context.Context, _ actor.Identity, d actor.Delivery) (actor.Result, error) {
				entered <- d
				select {
				case <-release:
				case <-ctx.Done():
					return actor.Result{}, ctx.Err()
				}
				if fail {
					return actor.Result{}, want
				}
				return actor.Result{Completed: true}, nil
			})}
			done := make(chan error, 1)
			go func() { done <- p.Process(context.Background(), msg, time.Hour) }()
			d := <-entered
			if d.Origin != actor.OriginTask || d.MessageType != "custom.business" {
				t.Errorf("投递来源或业务名称错误: %+v", d)
			}
			if msg.acked {
				t.Error("业务完成前已经确认任务")
			}
			close(release)
			err = <-done
			if fail {
				if !errors.Is(err, want) || msg.acked {
					t.Fatalf("失败任务错误确认: %v ack=%v", err, msg.acked)
				}
			} else if err != nil || !msg.acked {
				t.Fatalf("成功任务未确认: %v ack=%v", err, msg.acked)
			}
		})
	}
}
