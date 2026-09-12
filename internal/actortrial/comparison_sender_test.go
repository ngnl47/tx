package actortrial

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
)

// 只在测试覆盖构建中接入 Transport。每条消息仍独立 Publish，不增加定时凑批。
// 调用方等待本地 Publish 结果，因此现有业务 worker 的排水屏障继续覆盖回复发送。
type comparisonSender struct {
	mu         sync.Mutex
	queue      chan *comparisonSend
	done       chan struct{}
	closed     bool
	generation func() uint64
}

type comparisonSend struct {
	message    *nats.Msg
	generation uint64
	done       chan struct{}
	err        error
}

func newComparisonSender(capacity int, generation func() uint64, publish func(*nats.Msg) error) *comparisonSender {
	s := &comparisonSender{queue: make(chan *comparisonSend, capacity), done: make(chan struct{}), generation: generation}
	go func() {
		defer close(s.done)
		for item := range s.queue {
			if item.generation != generation() {
				item.err = errors.New("comparison: queued send belongs to previous connection")
			} else {
				item.err = publish(item.message)
			}
			close(item.done)
		}
	}()
	return s
}

func (s *comparisonSender) publish(msg *nats.Msg) error {
	item := &comparisonSend{message: msg, generation: s.generation(), done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nats.ErrConnectionClosed
	}
	select {
	case s.queue <- item:
		s.mu.Unlock()
		<-item.done
		return item.err
	default:
		s.mu.Unlock()
		return ErrPendingFull
	}
}

func (s *comparisonSender) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
}

func TestComparisonSenderBoundsFailureAndClose(t *testing.T) {
	ctx := trialContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	failure := errors.New("预期发送失败")
	var sent []string
	s := newComparisonSender(2, func() uint64 { return 0 }, func(msg *nats.Msg) error {
		sent = append(sent, msg.Subject)
		if msg.Subject == "hold" {
			close(entered)
			<-release
		}
		if msg.Subject == "fail" {
			return failure
		}
		return nil
	})
	defer s.stop()
	results := make(chan error, 3)
	go func() { results <- s.publish(&nats.Msg{Subject: "hold"}) }()
	signal(t, ctx, entered)
	go func() { results <- s.publish(&nats.Msg{Subject: "fail"}) }()
	eventually(t, ctx, func() bool { return len(s.queue) == 1 })
	go func() { results <- s.publish(&nats.Msg{Subject: "tail"}) }()
	eventually(t, ctx, func() bool { return len(s.queue) == 2 })
	if err := s.publish(&nats.Msg{Subject: "overflow"}); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("队列满返回 %v", err)
	}
	s.stop()
	if err := s.publish(&nats.Msg{Subject: "closed"}); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("关闭后返回 %v", err)
	}
	select {
	case <-s.done:
		t.Fatal("发送完成前提前结束排空")
	default:
	}
	unblock()
	signal(t, ctx, s.done)
	failed := 0
	for range 3 {
		select {
		case err := <-results:
			if errors.Is(err, failure) {
				failed++
			} else if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("发送等待未回收")
		}
	}
	if failed != 1 || len(sent) != 3 || sent[0] != "hold" || sent[1] != "fail" || sent[2] != "tail" {
		t.Fatalf("发送顺序或错误丢失: %v failed=%d", sent, failed)
	}
}

func TestComparisonSenderRejectsQueuedPreviousConnection(t *testing.T) {
	ctx := trialContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var generation, sent atomic.Uint64
	s := newComparisonSender(1, generation.Load, func(*nats.Msg) error {
		sent.Add(1)
		close(entered)
		<-release
		return nil
	})
	defer s.stop()
	first, queued := make(chan error, 1), make(chan error, 1)
	go func() { first <- s.publish(&nats.Msg{}) }()
	signal(t, ctx, entered)
	go func() { queued <- s.publish(&nats.Msg{}) }()
	eventually(t, ctx, func() bool { return len(s.queue) == 1 })
	generation.Add(1)
	s.stop()
	unblock()
	signal(t, ctx, s.done)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-queued; err == nil || sent.Load() != 1 {
		t.Fatalf("旧连接排队消息被补发: err=%v sent=%d", err, sent.Load())
	}
}
