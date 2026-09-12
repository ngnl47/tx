package actortrial

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

type countedSlotReader struct {
	assignment discovery.SlotAssignment
	reads      atomic.Int64
}

func (s *countedSlotReader) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	s.reads.Add(1)
	return s.assignment, nil
}

// 只度量已激活世界的消息路径；权威读取计数应保持为首次加载的一次。
func BenchmarkSlotHostRemoteCall(b *testing.B) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		b.Skip("需要显式指定 TX_ACTORTRIAL_NATS_URL")
	}
	ns := nats.NewInbox()
	reader := &countedSlotReader{assignment: discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world", Generation: 1}}
	host, err := ConnectSlotHost(url, ns, "world", echoRuntime(), 32, SlotHostConfig{Store: reader, SlotID: worldSlot})
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	defer host.Drain(ctx)
	caller, err := Connect(url, ns, "caller", echoRuntime(), 32)
	if err != nil {
		b.Fatal(err)
	}
	defer caller.Drain(ctx, nil)
	target := Target{"world", Identity{"world", "1"}}
	delivery := &actorpb.Delivery{MessageType: "move", Payload: make([]byte, 128)}
	if _, err = caller.Call(ctx, target, delivery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := caller.Call(ctx, target, delivery); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if reader.reads.Load() != 1 {
		b.Fatalf("已激活消息重复读取归属: %d", reader.reads.Load())
	}
}
