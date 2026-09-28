package msgq

import (
	"context"
	"errors"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
)

func TestPublishStreamSendReturnsAfterLocalAdmission(t *testing.T) {
	publisher := &grpcPublishStream{items: make(chan grpcPublishItem, 1), done: make(chan struct{})}
	defer close(publisher.done)
	payload := []byte("before")
	result := make(chan error, 1)
	go func() {
		result <- publisher.send(context.Background(), &msgqpb.PublishRequest{Payload: payload})
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("本地队列已接收，下行发布仍等待写协程")
	}
	copy(payload, "after!")
	if got := string((<-publisher.items).request.GetPayload()); got != "before" {
		t.Fatalf("入队载荷 = %q，调用方修改不应影响待发送消息", got)
	}
}

func TestPublishStreamSendWaitsForFullLocalQueue(t *testing.T) {
	publisher := &grpcPublishStream{items: make(chan grpcPublishItem, 1), done: make(chan struct{})}
	defer close(publisher.done)
	publisher.items <- grpcPublishItem{request: &msgqpb.PublishRequest{Payload: []byte("first")}}
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- publisher.send(context.Background(), &msgqpb.PublishRequest{Payload: []byte("second")})
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("队列尚未腾出空间，发送提前返回 %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := string((<-publisher.items).request.GetPayload()); got != "first" {
		t.Fatalf("第一条消息 = %q", got)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("队列腾出空间后发送没有完成")
	}
	if got := string((<-publisher.items).request.GetPayload()); got != "second" {
		t.Fatalf("第二条消息 = %q", got)
	}
}

func TestPublishStreamCloseUnblocksWhenReaderEndsBeforeCloseItem(t *testing.T) {
	publisher := &grpcPublishStream{
		items: make(chan grpcPublishItem, 1),
		done:  make(chan struct{}),
	}
	finished := make(chan error, 1)
	go func() { finished <- publisher.Close() }()
	deadline := time.After(time.Second)
	for len(publisher.items) == 0 {
		select {
		case <-deadline:
			t.Fatal("关闭请求没有进入写入队列")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	want := errors.New("remote stream ended")
	publisher.closeErr = want
	close(publisher.done)
	select {
	case err := <-finished:
		if !errors.Is(err, want) {
			t.Fatalf("Close error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("远端流结束后 Close 仍在等待已退出的写入者")
	}
}

func TestPublishStreamCloseDeadlineUnblocksFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := &grpcPublishStream{ctx: ctx, cancel: cancel, items: make(chan grpcPublishItem, 1), done: make(chan struct{})}
	publisher.items <- grpcPublishItem{}
	sendDone := make(chan error, 1)
	go func() { sendDone <- publisher.send(context.Background(), &msgqpb.PublishRequest{}) }()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer closeCancel()
	if err := publisher.closeContext(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("关闭应受截止时间约束: %v", err)
	}
	select {
	case <-sendDone:
	case <-time.After(time.Second):
		t.Fatal("关闭后发送者未退出")
	}
}
