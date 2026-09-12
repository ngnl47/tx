package actortrial

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

func TestComparisonBatchKeepsTellBarrier(t *testing.T) {
	p := &Transport{inbox: "r.", pending: make(map[string]*pendingCall)}
	b := &comparisonBatch{p: p, limit: 64, generation: func() uint64 { return 0 }, maxPayload: func() int64 { return 1 << 20 }, groups: map[string]map[string]struct{}{}, members: map[string]string{}}
	messages := []*nats.Msg{
		{Subject: "a", Reply: "r.1", Data: []byte{1}},
		{Subject: "b", Reply: "r.2", Data: []byte{2}},
		{Subject: "a", Reply: "r.3", Data: []byte{3}},
		{Subject: "a", Data: []byte{4}},
		{Subject: "a", Reply: "r.5", Data: []byte{5}},
	}
	var sent [][]byte
	b.publish = func(msg *nats.Msg) error {
		if msg.Header.Get(comparisonBatchHeader) == "" {
			sent = append(sent, msg.Data)
			return nil
		}
		var frame ComparisonRequestBatch
		if err := proto.Unmarshal(msg.Data, &frame); err != nil {
			t.Fatal(err)
		}
		var values []byte
		for _, entry := range frame.Entries {
			values = append(values, entry.Delivery...)
		}
		sent = append(sent, values)
		return nil
	}
	b.queue = make(chan *comparisonBatchSend, len(messages))
	b.done = make(chan struct{})
	for _, msg := range messages {
		b.queue <- &comparisonBatchSend{message: msg, done: make(chan struct{})}
	}
	close(b.queue)
	b.run()
	if want := [][]byte{{1, 3}, {4}, {5}, {2}}; !reflect.DeepEqual(sent, want) {
		t.Fatalf("发送包或 Tell 边界丢失: got %v want %v", sent, want)
	}
}

func TestComparisonBatchQueueBoundsAndGeneration(t *testing.T) {
	for _, test := range []struct {
		name                  string
		capacity, size, count int
	}{{"items", 2, 10, 2}, {"bytes", 6, 1 << 20, 4}} {
		t.Run(test.name, func(t *testing.T) {
			ctx := trialContext(t)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var generation, sent atomic.Uint64
			p := &Transport{maxPending: test.capacity, inbox: "r.", pending: map[string]*pendingCall{}}
			b := newComparisonBatch(p, 64)
			b.generation = generation.Load
			b.maxPayload = func() int64 { return 1 << 20 }
			b.publish = func(*nats.Msg) error { sent.Add(1); close(entered); <-release; return nil }
			defer b.stop(nil)
			results := make(chan error, test.count+1)
			go func() { results <- b.send(&nats.Msg{Subject: "hold"}) }()
			signal(t, ctx, entered)
			for i := range test.count {
				go func() { results <- b.send(&nats.Msg{Subject: "a", Data: make([]byte, test.size)}) }()
				eventually(t, ctx, func() bool { return len(b.queue) == i+1 })
			}
			if err := b.send(&nats.Msg{Subject: "a", Data: []byte{1}}); !errors.Is(err, ErrPendingFull) {
				t.Fatalf("预算未拒绝: %v", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := b.stop(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("排队收尾未遵守上下文: %v", err)
			}
			if err := b.send(&nats.Msg{Subject: "a"}); !errors.Is(err, nats.ErrConnectionClosed) {
				t.Fatalf("关闭后准入: %v", err)
			}
			generation.Add(1)
			unblock()
			signal(t, ctx, b.done)
			successes := 0
			for range test.count + 1 {
				if err := <-results; err == nil {
					successes++
				}
			}
			b.mu.Lock()
			remaining := b.bytes
			b.mu.Unlock()
			if successes != 1 || sent.Load() != 1 || remaining != 0 {
				t.Fatalf("旧连接补发或预算泄漏: success=%d sent=%d bytes=%d", successes, sent.Load(), remaining)
			}
		})
	}
}

func TestComparisonBatchPayloadLimits(t *testing.T) {
	p := &Transport{inbox: "r.", pending: map[string]*pendingCall{}}
	b := &comparisonBatch{p: p, limit: 64, generation: func() uint64 { return 0 }, maxPayload: func() int64 { return 1 << 20 }, groups: map[string]map[string]struct{}{}, members: map[string]string{}}
	var packets, entries, singles int
	b.publish = func(msg *nats.Msg) error {
		packets++
		if msg.Header.Get(comparisonBatchHeader) == "" {
			singles++
			if len(msg.Data) != 100<<10 {
				t.Fatal("单消息载荷改变")
			}
			return nil
		}
		if msg.Size()-len(msg.Subject)-len(msg.Reply) > 64<<10 {
			t.Fatal("封包超过包含 NATS 头的上限")
		}
		var frame ComparisonRequestBatch
		if err := proto.Unmarshal(msg.Data, &frame); err != nil {
			t.Fatal(err)
		}
		entries += len(frame.Entries)
		return nil
	}
	var items []*comparisonBatchSend
	for _, size := range []int{32 << 10, 32 << 10, 100 << 10} {
		items = append(items, &comparisonBatchSend{message: &nats.Msg{Subject: "a", Reply: "r.1", Data: make([]byte, size)}})
	}
	b.pack(items)
	if packets != 3 || entries != 2 || singles != 1 {
		t.Fatalf("封包大小边界: packets=%d entries=%d singles=%d", packets, entries, singles)
	}
}
