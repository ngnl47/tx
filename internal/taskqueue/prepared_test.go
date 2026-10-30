package taskqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// 先完成校验再启动，不在业务开放后重复查询外部配置。
func TestPreparedConsumerSeparatesValidationFromConsumption(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "普通任务", true: "耗尽恢复"}[recovery], func(t *testing.T) {
			want := errors.New("consume failed")
			consumer := &preparedConsumerProbe{err: want}
			info := &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second}}
			var prepared *PreparedConsumer
			var err error
			if recovery {
				prepared, err = PrepareRecovery(consumer, info, Recovery{}, time.Second)
			} else {
				prepared, err = Prepare(consumer, info, Processor{Timeout: time.Second})
			}
			if err != nil {
				t.Fatal(err)
			}
			if consumer.consumed != 0 || consumer.queried != 0 {
				t.Fatal("准备阶段发生消费或重复查询")
			}
			if _, err := prepared.Start(); !errors.Is(err, want) {
				t.Fatalf("Start = %v", err)
			}
			if consumer.consumed != 1 || consumer.queried != 0 {
				t.Fatal("开始阶段不应查询配置或隐藏消费错误")
			}
		})
	}
}

type preparedConsumerProbe struct {
	jetstream.Consumer
	err               error
	consumed, queried int
}

func (p *preparedConsumerProbe) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	p.queried++
	return nil, p.err
}
func (p *preparedConsumerProbe) Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	p.consumed++
	return nil, p.err
}
