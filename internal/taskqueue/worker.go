// Package taskqueue 将 JetStream 任务分派到正式 Actor，负责续期、确认和消费排水。
package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/modules/actor"
)

// Task 保存业务身份与消息，不携带实例地址或另一份实体归属。
type Task struct {
	Identity actor.Identity
	Delivery actor.Delivery
}

// Processor 复用已建立的流与 Actor 入口，不负责部署资源或业务补偿。
type Processor struct {
	Actor      actor.Caller
	JetStream  jetstream.JetStream
	DeadLetter string
	Timeout    time.Duration
}

// Process 在调用期间续期；失败不确认，永久加载拒绝先转存死信再确认。
// interval 来自消费者 AckWait；续期不能提供幂等或故障下的唯一执行保证。
func (p Processor) Process(parent context.Context, msg jetstream.Msg, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, p.Timeout)
	defer cancel()
	var task Task
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		return err
	}
	// 任务来源由消费入口设置，忽略发布者携带的来源和消息名称。
	task.Delivery.Origin = actor.OriginTask
	result := make(chan error, 1)
	go func() {
		_, err := p.Actor.Call(ctx, task.Identity, task.Delivery)
		result <- err
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := msg.InProgress(); err != nil {
				slog.Error("task renewal failed", "subject", msg.Subject(), "err", err)
			}
		case err := <-result:
			if err != nil {
				if !errors.Is(err, actor.ErrActivationDenied) {
					return err
				}
				dead := &nats.Msg{Subject: p.DeadLetter, Data: msg.Data(), Header: nats.Header{}}
				dead.Header.Set("Task-Failure", actor.ErrActivationDenied.Error())
				dead.Header.Set("Task-Source", msg.Subject())
				if _, err = p.JetStream.PublishMsg(ctx, dead); err != nil {
					return err
				}
			}
			return msg.DoubleAck(ctx)
		}
	}
}

// Worker 不持有连接关闭权限。消费排水完成后，宿主再关闭 Actor 和连接。
type Worker struct{ consumption jetstream.ConsumeContext }

// PreparedConsumer 保存已校验的消费计划；准备阶段不订阅、不领取消息。
// 配置来自启动时读取的 ConsumerInfo，运行期配置变更不在本次启动快照内。
type PreparedConsumer struct {
	consumer jetstream.Consumer
	process  func(jetstream.Msg) error
}

// Prepare 校验普通任务策略并计算续期周期，不重复向 JetStream 查询配置。
func Prepare(consumer jetstream.Consumer, info *jetstream.ConsumerInfo, processor Processor) (*PreparedConsumer, error) {
	if processor.Timeout <= 0 {
		return nil, errors.New("taskqueue: timeout must be positive")
	}
	ackWait := info.Config.AckWait
	if len(info.Config.BackOff) != 0 {
		ackWait = info.Config.BackOff[0]
	}
	if info.Config.Durable == "" || info.Config.AckPolicy != jetstream.AckExplicitPolicy || ackWait <= 0 {
		return nil, errors.New("taskqueue: durable consumer, explicit ack and positive ack wait required")
	}
	return &PreparedConsumer{consumer: consumer, process: func(msg jetstream.Msg) error {
		// 启动请求结束不取消已经领取的业务，关闭由 Drain 协调。
		return processor.Process(context.Background(), msg, max(time.Nanosecond, ackWait/3))
	}}, nil
}

// Start 在业务准入开放后开始领取任务，不再执行静态校验或查询 ConsumerInfo。
// 每个 Worker 同步处理一条任务，SDK 不额外预取批量业务。
func (prepared *PreparedConsumer) Start() (*Worker, error) {
	return consume(prepared.consumer, prepared.process)
}

func consume(consumer jetstream.Consumer, process func(jetstream.Msg) error) (*Worker, error) {
	consumption, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := process(msg); err != nil {
			slog.Error("task consumption failed", "subject", msg.Subject(), "err", err)
		}
	}, jetstream.PullMaxMessages(1), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		slog.Error("task pull failed", "err", err)
	}))
	if err != nil {
		return nil, err
	}
	return &Worker{consumption: consumption}, nil
}

// Drain 停止拉取并等待回调结束；等待超时保留资源，可以再次等待。
// 成功只表示回调已结束，失败任务仍由 JetStream 保留，不代表每条业务都成功。
func (w *Worker) Drain(ctx context.Context) error {
	w.consumption.Drain()
	select {
	case <-w.consumption.Closed():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
