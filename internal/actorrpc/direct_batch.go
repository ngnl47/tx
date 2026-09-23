package actorrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/protobuf/proto"
)

const (
	directBatchMaxItems = 100
	directBatchMaxBytes = 2 << 20
)

type directCallReply struct {
	result *actorpb.Result
	err    error
}

type pendingDirectCall struct {
	ctx      context.Context
	target   actorhost.Target
	delivery *actorpb.Delivery
	done     chan directCallReply
}

type directCallBucket struct {
	items []pendingDirectCall
	bytes int
	timer *time.Timer
}

// BatchingDirectTransport 在目标实例内短时聚合 Agent 直连 Call，不改变逐项完成结果。
type BatchingDirectTransport struct {
	Sender  *rpc.Sender
	window  time.Duration
	mu      sync.Mutex
	buckets map[string]*directCallBucket
}

func NewBatchingDirectTransport(sender *rpc.Sender) *BatchingDirectTransport {
	return &BatchingDirectTransport{Sender: sender, window: time.Millisecond, buckets: make(map[string]*directCallBucket)}
}

func (t *BatchingDirectTransport) Call(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	if err := ValidateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	copy := proto.Clone(delivery).(*actorpb.Delivery)
	copy.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	copy.RequestId = 1
	item := pendingDirectCall{ctx: ctx, target: target, delivery: copy, done: make(chan directCallReply, 1)}
	size := proto.Size(&actorpb.DirectCallItem{Kind: target.Identity.Kind, Key: target.Identity.Key, Delivery: copy})

	t.mu.Lock()
	bucket := t.buckets[target.Instance]
	if bucket != nil && bucket.bytes+size > directBatchMaxBytes {
		delete(t.buckets, target.Instance)
		bucket.timer.Stop()
		go t.send(target.Instance, bucket.items)
		bucket = nil
	}
	if bucket == nil {
		bucket = &directCallBucket{}
		t.buckets[target.Instance] = bucket
		bucket.timer = time.AfterFunc(t.window, func() { t.flush(target.Instance, bucket) })
	}
	bucket.items = append(bucket.items, item)
	bucket.bytes += size
	if len(bucket.items) == directBatchMaxItems {
		delete(t.buckets, target.Instance)
		bucket.timer.Stop()
		go t.send(target.Instance, bucket.items)
	}
	t.mu.Unlock()

	select {
	case reply := <-item.done:
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *BatchingDirectTransport) flush(instance string, bucket *directCallBucket) {
	t.mu.Lock()
	if t.buckets[instance] != bucket {
		t.mu.Unlock()
		return
	}
	delete(t.buckets, instance)
	t.mu.Unlock()
	t.send(instance, bucket.items)
}

func (t *BatchingDirectTransport) send(instance string, pending []pendingDirectCall) {
	items := pending[:0]
	for _, item := range pending {
		if err := item.ctx.Err(); err != nil {
			item.done <- directCallReply{err: err}
			continue
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return
	}
	request := &actorpb.DirectCallBatch{Items: make([]*actorpb.DirectCallItem, len(items))}
	deadline, _ := items[0].ctx.Deadline()
	for _, item := range items[1:] {
		itemDeadline, _ := item.ctx.Deadline()
		if itemDeadline.After(deadline) {
			deadline = itemDeadline
		}
	}
	for index, item := range items {
		itemDeadline, _ := item.ctx.Deadline()
		request.Items[index] = &actorpb.DirectCallItem{Kind: item.target.Identity.Kind, Key: item.target.Identity.Key, Delivery: item.delivery, DeadlineSlackNano: int64(deadline.Sub(itemDeadline))}
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(items[0].ctx), deadline)
	defer cancel()
	data, err := proto.Marshal(request)
	if err != nil {
		for _, item := range items {
			item.done <- directCallReply{err: err}
		}
		return
	}
	seen := make([]bool, len(items))
	err = t.Sender.CallBatch(ctx, rpc.Delivery{Route: rpc.InstanceRoute(instance), MessageType: "actor.batchcall", Payload: data}, func(response rpc.Result) error {
		index := int(response.CommandID)
		if response.MessageType != "actor.batchitem" || index < 0 || index >= len(items) || seen[index] {
			return errors.New("actortrial: invalid direct batch response")
		}
		var outcome actorpb.DirectCallOutcome
		if err := proto.Unmarshal(response.Payload, &outcome); err != nil {
			return err
		}
		var reply directCallReply
		switch value := outcome.GetOutcome().(type) {
		case *actorpb.DirectCallOutcome_Result:
			reply = directCallReply{result: value.Result, err: ResultError(value.Result)}
		case *actorpb.DirectCallOutcome_Failure:
			if value.Failure == nil {
				return errors.New("actortrial: invalid direct batch outcome")
			}
			reply.err = &actorhost.RemoteError{Code: value.Failure.Code, Message: value.Failure.Message, Kind: value.Failure.Kind}
		default:
			return errors.New("actortrial: invalid direct batch outcome")
		}
		seen[index] = true
		items[index].done <- reply
		return nil
	})
	if err == nil {
		err = errors.New("actortrial: incomplete direct batch response")
	}
	for index, item := range items {
		if !seen[index] {
			item.done <- directCallReply{err: err}
		}
	}
}

func (r *directReceiver) CallBatch(ctx context.Context, request rpc.Delivery, send func(rpc.Result) error) error {
	if request.MessageType != "actor.batchcall" {
		return errors.New("actortrial: invalid direct batch request")
	}
	owner := request.Route.Kind == rpc.RouteInstance && request.Route.InstanceID == r.instance
	var batch actorpb.DirectCallBatch
	if err := proto.Unmarshal(request.Payload, &batch); err != nil {
		return err
	}
	if len(batch.Items) == 0 || len(batch.Items) > directBatchMaxItems {
		return errors.New("actortrial: invalid direct batch size")
	}
	type completed struct {
		result rpc.Result
		err    error
	}
	finished := make(chan completed, len(batch.Items))
	for index, item := range batch.Items {
		go func() {
			var outcome *actorpb.DirectCallOutcome
			if !owner {
				outcome = directFailure(actorhost.ErrNotOwner)
			} else if item == nil {
				outcome = directFailure(errors.New("actortrial: invalid direct batch item"))
			} else {
				target := actorhost.Target{Instance: r.instance, Identity: actorhost.Identity{Kind: item.Kind, Key: item.Key}}
				deadline, ok := ctx.Deadline()
				if !ok || item.DeadlineSlackNano < 0 {
					outcome = directFailure(errors.New("actortrial: invalid direct batch deadline"))
				} else {
					itemCtx, cancel := context.WithDeadline(ctx, deadline.Add(-time.Duration(item.DeadlineSlackNano)))
					value, err := executeDirectCall(itemCtx, r.runtime, target, item.Delivery)
					cancel()
					if err != nil {
						outcome = directFailure(err)
					} else {
						outcome = &actorpb.DirectCallOutcome{Outcome: &actorpb.DirectCallOutcome_Result{Result: value}}
					}
				}
			}
			data, err := proto.Marshal(outcome)
			finished <- completed{result: rpc.Result{MessageType: "actor.batchitem", CommandID: uint64(index), Payload: data}, err: err}
		}()
	}
	for range batch.Items {
		select {
		case item := <-finished:
			if item.err != nil {
				return item.err
			}
			if err := send(item.result); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func directFailure(err error) *actorpb.DirectCallOutcome {
	failure := actorhost.RemoteErrorFrom(err)
	return &actorpb.DirectCallOutcome{Outcome: &actorpb.DirectCallOutcome_Failure{Failure: &actorpb.DirectCallFailure{Code: failure.Code, Message: failure.Message, Kind: failure.Kind}}}
}
