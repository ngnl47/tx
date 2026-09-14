// Package tasks 提供可靠 Actor 任务的持久投递契约，消费与确认由框架负责。
package tasks

import (
	"context"
	"github.com/ngnl47/tx/modules/actor"
	"time"
)

// Publisher 确认任务已持久发布，不等待目标 Actor 执行。
type Publisher interface {
	Publish(context.Context, actor.Identity, actor.Delivery) error
}

// Config 绑定预先部署的 JetStream 资源，框架不自动修改共享流配置。
type Config struct {
	// URL 是任务使用的 NATS 连接地址。
	URL string
	// Stream 和 Subject 指定工作队列流及其投递主题。
	Stream, Subject string
	// Consumer 是共享 durable 名称；为空时仅提供发布能力。
	Consumer string
	// DeadLetter 是永久加载拒绝的死信主题，必须匹配文件存储流。
	DeadLetter string
	// RecoveryStream 与 RecoveryConsumer 绑定预先捕获耗尽通知的专用文件流和 durable。
	// 源消费者设置有限 MaxDeliver 时必填；通知消费者自身不能限制投递次数。
	RecoveryStream, RecoveryConsumer string
	// Timeout 是单次消费等待上限；不代表远端业务已停止。
	Timeout time.Duration
}
