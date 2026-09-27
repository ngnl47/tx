package msgq

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
	go func() { sendDone <- publisher.send(context.Background(), nil) }()
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
