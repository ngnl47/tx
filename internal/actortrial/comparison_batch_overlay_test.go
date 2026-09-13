//go:build tx_nats_batch

package actortrial

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"google.golang.org/protobuf/proto"
)

// 暂停第一个本地 Publish，使随后请求确定地进入同一快照，不使用睡眠凑批。
func holdComparisonBatch(t *testing.T, p *Transport) func() {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	// 此夹具明确验证多项封包，独立于性能构建的 batch=1 对照。
	p.comparisonBatch.limit = 64
	original := p.comparisonBatch.publish
	p.comparisonBatch.publish = func(msg *nats.Msg) error {
		if msg.Subject == "comparison.hold" {
			close(entered)
			<-release
			return nil
		}
		return original(msg)
	}
	go p.comparisonBatch.send(&nats.Msg{Subject: "comparison.hold"})
	signal(t, trialContext(t), entered)
	return unblock
}

func batchWaiters(t *testing.T, p *Transport, n int) {
	t.Helper()
	eventually(t, trialContext(t), func() bool { return len(p.comparisonBatch.queue) == n })
}

func batchEmpty(t *testing.T, p *Transport) {
	t.Helper()
	eventually(t, trialContext(t), func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.pending) == 0 && len(p.comparisonBatch.groups) == 0 && len(p.comparisonBatch.members) == 0
	})
}

func TestComparisonBatchIndependentResults(t *testing.T) {
	ctx := trialContext(t)
	slow, entered := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(slow) })
	defer release()
	r := New(8, time.Second, func(c Context) (*Behavior, error) {
		if c.Identity.Key == "denied" {
			return nil, ErrActivationDenied
		}
		return &Behavior{Receive: func(c Context, v any) (any, error) {
			if c.Identity.Key == "slow" {
				close(entered)
				<-slow
			}
			if c.Identity.Key == "panic" {
				panic("预期单项失败")
			}
			d := v.(*actorpb.Delivery)
			return &actorpb.Result{MessageType: d.MessageType, RequestId: d.RequestId, Payload: d.Payload}, nil
		}}, nil
	})
	ns, url := nats.NewInbox(), natsURL(t)
	a := peer(t, url, ns, "a", echoRuntime())
	b := peer(t, url, ns, "b", r)
	defer a.runtime.Drain(ctx)
	defer b.runtime.Drain(ctx)
	unblock := holdComparisonBatch(t, a)
	keys := []string{"slow", "fast", "denied", "panic", "badIdentity", "badDelivery"}
	type outcome struct {
		result *actorpb.Result
		err    error
	}
	results := make([]chan outcome, len(keys))
	for i, key := range keys {
		msg, err := a.message(Target{"b", Identity{"room", key}}, &actorpb.Delivery{Mode: actorpb.DeliveryMode_DELIVERY_MODE_CALL, MessageType: "buff", RequestId: uint64(100 + i), Payload: []byte(key)})
		if err != nil {
			t.Fatal(err)
		}
		if key == "badIdentity" {
			msg.Header.Set("Actor-Identity", "!")
		}
		if key == "badDelivery" {
			msg.Data = []byte{0xff}
		}
		results[i] = make(chan outcome, 1)
		go func() {
			f, err := a.requestFuture(ctx, msg)
			var result *actorpb.Result
			if err == nil {
				result, err = f.Await(ctx)
			}
			results[i] <- outcome{result, err}
		}()
		batchWaiters(t, a, i+1)
	}
	unblock()
	signal(t, ctx, entered)
	for i := 1; i < len(keys); i++ {
		select {
		case got := <-results[i]:
			if i == 1 && (got.err != nil || got.result.RequestId != 101 || string(got.result.Payload) != "fast") {
				t.Fatalf("独立结果关联失败: %+v", got)
			}
			if i > 1 && got.err == nil {
				t.Fatalf("%s 失败被伪装成功", keys[i])
			}
		case <-ctx.Done():
			t.Fatal("同批慢请求阻塞了其他结果")
		}
	}
	release()
	if got := <-results[0]; got.err != nil || got.result.RequestId != 100 {
		t.Fatalf("慢项结果丢失: %+v", got)
	}
	batchEmpty(t, a)
}

func TestComparisonBatchCanceledFirstAndOuterFailure(t *testing.T) {
	for _, failure := range []string{"503", "version", "encoding", "publish", "close"} {
		t.Run(failure, func(t *testing.T) {
			ctx := trialContext(t)
			a := peer(t, natsURL(t), nats.NewInbox(), "a", echoRuntime())
			defer a.runtime.Drain(ctx)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			a.comparisonBatch.limit = 64
			a.comparisonBatch.publish = func(msg *nats.Msg) error {
				if msg.Header.Get(comparisonBatchHeader) == "" {
					close(entered)
					<-release
					return nil
				}
				// 登记已经完成后取消第一项，模拟它先结束、随后才到达整批错误。
				var frame ComparisonRequestBatch
				if err := proto.Unmarshal(msg.Data, &frame); err != nil {
					return err
				}
				a.finishRequest(frame.Entries[0].Reply, nil, context.Canceled)
				switch failure {
				case "publish":
					return nats.ErrMaxPayload
				case "close":
					a.conn.Close()
					return nats.ErrConnectionClosed
				case "version":
					msg.Header.Set(comparisonBatchHeader, "unknown")
				case "encoding":
					msg.Data = []byte{0xff}
				}
				return a.conn.PublishMsg(msg)
			}
			go a.comparisonBatch.send(&nats.Msg{Subject: "hold"})
			signal(t, ctx, entered)
			target := "absent"
			if failure == "version" || failure == "encoding" {
				target = "a"
			}
			results := make(chan error, 3)
			for i := range 3 {
				msg, _ := a.message(Target{target, Identity{"room", strconv.Itoa(i)}}, &actorpb.Delivery{MessageType: "buff"})
				go func() {
					f, err := a.requestFuture(ctx, msg)
					if err == nil {
						_, err = f.Await(ctx)
					}
					results <- err
				}()
				batchWaiters(t, a, i+1)
			}
			unblock()
			canceled := 0
			for range 3 {
				select {
				case err := <-results:
					if err == nil {
						t.Fatal("整批错误被伪装成功")
					}
					if errors.Is(err, context.Canceled) {
						canceled++
					} else if failure == "503" && !errors.Is(err, nats.ErrNoResponders) {
						t.Fatalf("503=%v", err)
					}
				case <-ctx.Done():
					t.Fatal("整批失败未回收")
				}
			}
			if canceled != 1 {
				t.Fatalf("首项取消=%d", canceled)
			}
			batchEmpty(t, a)
			for i := range 3 {
				if state := a.runtime.Probe(Identity{"room", strconv.Itoa(i)}).State; state != Unloaded {
					t.Fatalf("外层损坏仍加载实体: %v", state)
				}
			}
		})
	}
}

func TestComparisonBatchQuiesceRetainsControlConnection(t *testing.T) {
	ctx := trialContext(t)
	p := peer(t, natsURL(t), nats.NewInbox(), "a", echoRuntime())
	h := &SlotHost{Transport: p, flush: func(context.Context) error { return errors.New("预期持久化失败") }}
	if err := h.quiesce(ctx); err == nil {
		t.Fatal("丢失收尾错误")
	}
	p.comparisonBatch.mu.Lock()
	closed := p.comparisonBatch.closed
	p.comparisonBatch.mu.Unlock()
	if closed || p.conn.IsClosed() {
		t.Fatal("持久化失败提前释放发送资源")
	}
	h.flush = nil
	if err := h.quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if p.conn.IsClosed() {
		t.Fatal("槽位交接控制连接被关闭")
	}
	if err := p.Tell(ctx, Target{"a", Identity{"room", "1"}}, &actorpb.Delivery{MessageType: "buff"}); !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("排空后仍接受发送: %v", err)
	}
	if err := p.conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
}
