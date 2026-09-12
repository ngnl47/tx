package actortrial

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const comparisonBatchHeader = "Actor-Batch"

// 只在隔离覆盖构建中接线。回复和控制请求仍使用原连接直接发送。
type comparisonBatch struct {
	p          *Transport
	mu         sync.Mutex
	queue      chan *comparisonBatchSend
	bytes      int
	closed     bool
	done       chan struct{}
	limit      int
	generation func() uint64
	maxPayload func() int64
	publish    func(*nats.Msg) error
	// 两张关联表均与 Transport.pending 共用 p.mu，不保存已发送载荷。
	groups  map[string]map[string]struct{}
	members map[string]string
}

type comparisonBatchSend struct {
	message    *nats.Msg
	generation uint64
	size       int
	done       chan struct{}
	err        error
}

func newComparisonBatch(p *Transport, limit int) *comparisonBatch {
	b := &comparisonBatch{p: p, queue: make(chan *comparisonBatchSend, p.maxPending), done: make(chan struct{}), limit: limit,
		groups: make(map[string]map[string]struct{}), members: make(map[string]string),
		generation: func() uint64 { return p.conn.Stats().Reconnects }, maxPayload: func() int64 { return p.conn.MaxPayload() },
		publish: func(msg *nats.Msg) error { return p.conn.PublishMsg(msg) }}
	go b.run()
	return b
}

func (b *comparisonBatch) send(msg *nats.Msg) error {
	item := &comparisonBatchSend{message: msg, generation: b.generation(), size: msg.Size() - len(msg.Subject) - len(msg.Reply), done: make(chan struct{})}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nats.ErrConnectionClosed
	}
	if item.size > max(4<<20, int(b.maxPayload()))-b.bytes {
		b.mu.Unlock()
		return ErrPendingFull
	}
	select {
	case b.queue <- item:
		b.bytes += item.size
		b.mu.Unlock()
		<-item.done
		return item.err
	default:
		b.mu.Unlock()
		return ErrPendingFull
	}
}

func (b *comparisonBatch) stop(ctx context.Context) error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.queue)
	}
	b.mu.Unlock()
	if ctx == nil {
		return nil
	}
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *comparisonBatch) run() {
	defer close(b.done)
	for first := range b.queue {
		items := []*comparisonBatchSend{first}
	collect:
		for len(items) < 64 {
			select {
			case next, ok := <-b.queue:
				if !ok {
					break collect
				}
				items = append(items, next)
			default:
				break collect
			}
		}
		b.mu.Lock()
		for _, item := range items {
			b.bytes -= item.size
		}
		b.mu.Unlock()
		// 每次最多 64 项，稳定排序只归组当前快照，不保存每实例队列。
		slices.SortStableFunc(items, func(a, c *comparisonBatchSend) int { return strings.Compare(a.message.Subject, c.message.Subject) })
		b.pack(items)
		for _, item := range items {
			close(item.done)
		}
	}
}

func (b *comparisonBatch) pack(items []*comparisonBatchSend) {
	for len(items) > 0 {
		first := items[0]
		if first.generation != b.generation() {
			first.err = errors.New("comparison: queued send belongs to previous connection")
			items = items[1:]
			continue
		}
		maxSize := min(64<<10, int(b.maxPayload()))
		frame := &ComparisonRequestBatch{}
		msg := &nats.Msg{Subject: first.message.Subject, Header: nats.Header{comparisonBatchHeader: {"1"}}}
		size := msg.Size() - len(msg.Subject)
		count := 0
		for _, item := range items {
			if count == b.limit || item.message.Subject != msg.Subject || item.message.Reply == "" || item.generation != first.generation {
				break
			}
			entry := &ComparisonRequestEntry{Identity: item.message.Header.Get("Actor-Identity"), Delivery: item.message.Data, Reply: item.message.Reply}
			entrySize := 1 + protowire.SizeBytes(proto.Size(entry))
			if size+entrySize > maxSize {
				break
			}
			frame.Entries = append(frame.Entries, entry)
			size += entrySize
			count++
		}
		if count == 0 {
			// Tell 是发送边界；超过封包上限的合法单消息保持 SDK 原有大小契约。
			first.err = b.publish(first.message)
			items = items[1:]
			continue
		}
		data, err := proto.Marshal(frame)
		if err == nil {
			msg.Data = data
			msg.Reply = b.register(items[:count])
			err = b.publish(msg)
		}
		for _, item := range items[:count] {
			item.err = err
		}
		items = items[count:]
	}
}

func (b *comparisonBatch) register(items []*comparisonBatchSend) string {
	b.p.mu.Lock()
	defer b.p.mu.Unlock()
	b.p.nextRequest++
	reply := b.p.inbox + "b" + strconv.FormatUint(b.p.nextRequest, 10)
	members := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := item.message.Reply
		if b.p.pending[key] != nil {
			members[key] = struct{}{}
			b.members[key] = reply
		}
	}
	if len(members) > 0 {
		b.groups[reply] = members
	}
	return reply
}

// 调用位置在 finishRequest 删除 pending 的同一锁内，取消与登记不会留下孤立批次。
func (b *comparisonBatch) removeLocked(reply string) {
	group := b.members[reply]
	if group == "" {
		return
	}
	delete(b.members, reply)
	delete(b.groups[group], reply)
	if len(b.groups[group]) == 0 {
		delete(b.groups, group)
	}
}

func (b *comparisonBatch) finishReply(msg *nats.Msg) {
	if !strings.HasPrefix(msg.Subject, b.p.inbox+"b") {
		b.p.finishRequest(msg.Subject, msg, nil)
		return
	}
	b.p.mu.Lock()
	members := make([]string, 0, len(b.groups[msg.Subject]))
	for reply := range b.groups[msg.Subject] {
		members = append(members, reply)
	}
	b.p.mu.Unlock()
	for _, reply := range members {
		b.p.finishRequest(reply, msg, nil)
	}
}

func (b *comparisonBatch) receive(msg *nats.Msg) {
	var frame ComparisonRequestBatch
	if msg.Header.Get(comparisonBatchHeader) != "1" || proto.Unmarshal(msg.Data, &frame) != nil || len(frame.Entries) == 0 || len(frame.Entries) > 64 || msg.Size()-len(msg.Subject)-len(msg.Reply) > 64<<10 {
		b.p.reply(msg, nil, errors.New("actortrial: invalid batch"))
		return
	}
	for _, entry := range frame.Entries {
		// 拷贝保留原订阅以支持 RespondMsg，各项独立复用原接收与结果回调。
		single := *msg
		single.Header = nats.Header{"Actor-Identity": {entry.Identity}}
		single.Data = entry.Delivery
		single.Reply = entry.Reply
		b.p.receive(&single)
	}
}
