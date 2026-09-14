package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const maxDeliveryEvent = "io.nats.jetstream.advisory.v1.max_deliver"

// MaxDeliverySubject 是服务端发布耗尽通知的主题；由部署侧文件流提前捕获。
func MaxDeliverySubject(stream, consumer string) string {
	return "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + stream + "." + consumer
}

// Recovery 仅处理绑定源消费者的耗尽通知，不执行或重放业务。
type Recovery struct {
	JetStream  jetstream.JetStream
	Source     jetstream.Stream
	Consumer   jetstream.Consumer
	DeadLetter string
}

// Process 先转存，再移除源残留，最后确认通知；任何中间失败都保留通知以便续领。
// 不提供跨流原子性；重复通知或响应丢失可能产生重复死信，不维护成功完成表。
func (r Recovery) Process(ctx context.Context, msg jetstream.Msg) error {
	var event struct {
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
		Stream    string    `json:"stream"`
		Consumer  string    `json:"consumer"`
		Sequence  uint64    `json:"stream_seq"`
	}
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		return err
	}
	source, err := r.Source.Info(ctx)
	if err != nil {
		return err
	}
	consumer, err := r.Consumer.Info(ctx)
	if err != nil {
		return err
	}
	if event.Type != maxDeliveryEvent || event.Stream != source.Config.Name || event.Consumer != consumer.Name || event.Sequence == 0 || event.Timestamp.IsZero() {
		return errors.New("taskqueue: invalid or unrelated max delivery advisory")
	}
	// 防止历史通知误删同名重建资源；资源变更仍须先停止消费并清理积压。
	if event.Timestamp.Before(source.Created) || event.Timestamp.Before(consumer.Created) {
		slog.Error("stale task exhaustion advisory", "stream", event.Stream, "consumer", event.Consumer, "sequence", event.Sequence)
		return msg.DoubleAck(ctx)
	}
	task, err := r.Source.GetMsg(ctx, event.Sequence)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		// 可能已由前次恢复移除，也可能因保留策略丢失；不能伪称业务成功。
		slog.Error("exhausted task source missing", "stream", event.Stream, "sequence", event.Sequence)
		return msg.DoubleAck(ctx)
	}
	if err != nil {
		return err
	}
	dead := &nats.Msg{Subject: r.DeadLetter, Data: task.Data, Header: nats.Header{}}
	dead.Header.Set("Task-Failure", "max_deliver")
	dead.Header.Set("Task-Source", task.Subject)
	dead.Header.Set("Task-Stream", event.Stream)
	dead.Header.Set("Task-Consumer", event.Consumer)
	dead.Header.Set("Task-Sequence", strconv.FormatUint(event.Sequence, 10))
	if _, err := r.JetStream.PublishMsg(ctx, dead); err != nil {
		return err
	}
	if err := r.Source.DeleteMsg(ctx, event.Sequence); err != nil {
		return err
	}
	return msg.DoubleAck(ctx)
}

// StartRecovery 使用独立 durable 续领已持久捕获的通知，不能给通知自身设置耗尽上限。
func StartRecovery(ctx context.Context, consumer jetstream.Consumer, recovery Recovery, timeout time.Duration) (*Worker, error) {
	if timeout <= 0 {
		return nil, errors.New("taskqueue: recovery timeout must be positive")
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	if info.Config.Durable == "" || info.Config.AckPolicy != jetstream.AckExplicitPolicy || info.Config.MaxDeliver > 0 {
		return nil, errors.New("taskqueue: recovery consumer requires durable explicit ack without max deliver")
	}
	return consume(consumer, func(msg jetstream.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return recovery.Process(ctx, msg)
	})
}
