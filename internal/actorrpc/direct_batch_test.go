package actorrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/protobuf/proto"
)

type partialBatchConnection struct{}

func (partialBatchConnection) Deliver(context.Context, rpc.Delivery) error { return nil }
func (partialBatchConnection) Call(context.Context, rpc.Delivery) (rpc.Result, error) {
	return rpc.Result{}, errors.New("unexpected single call")
}
func (partialBatchConnection) Close() error { return nil }
func (partialBatchConnection) CallBatch(_ context.Context, delivery rpc.Delivery, accept func(rpc.Result) error) error {
	var batch actorpb.DirectCallBatch
	if err := proto.Unmarshal(delivery.Payload, &batch); err != nil {
		return err
	}
	if len(batch.Items) != 2 {
		return errors.New("unexpected batch size")
	}
	data, err := proto.Marshal(&actorpb.DirectCallOutcome{Outcome: &actorpb.DirectCallOutcome_Result{Result: &actorpb.Result{MessageType: "pong"}}})
	if err != nil {
		return err
	}
	if err := accept(rpc.Result{MessageType: "actor.batchitem", CommandID: 0, Payload: data}); err != nil {
		return err
	}
	return errors.New("stream interrupted")
}

type partialBatchDialer struct{}

func (partialBatchDialer) Dial(context.Context, discovery.Instance) (rpc.Connection, error) {
	return partialBatchConnection{}, nil
}

func TestDirectBatchPartialStreamFailure(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "game-a", Services: []string{"game"}, Endpoint: "unused", Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, partialBatchDialer{})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	transport := NewBatchingDirectTransport(sender)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	items := make([]pendingDirectCall, 2)
	for index := range items {
		items[index] = pendingDirectCall{
			ctx:      ctx,
			target:   actorhost.Target{Instance: "game-a", Identity: actorhost.Identity{Kind: "role", Key: "one"}},
			delivery: &actorpb.Delivery{MessageType: "ping"},
			done:     make(chan directCallReply, 1),
		}
	}
	transport.send("game-a", items)
	if reply := <-items[0].done; reply.err != nil || reply.result == nil || reply.result.MessageType != "pong" {
		t.Fatalf("已返回的第一项应成功: %+v", reply)
	}
	if reply := <-items[1].done; reply.err == nil || reply.result != nil {
		t.Fatalf("未返回的第二项不能伪装为成功: %+v", reply)
	}
}
