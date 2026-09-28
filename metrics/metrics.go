// Package metrics 提供进程级消息链路指标；标签只使用固定的阶段和结果。
package metrics

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PublishKind 区分精确会话投递与角色广播，取值不能来自用户身份。
type PublishKind uint8

const (
	PublishSession PublishKind = iota
	PublishRole
)

var (
	registry = prometheus.NewRegistry()
	buckets  = []float64{0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.15, 0.25, 0.5, 1}

	agentUplink = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tx_agent_uplink_total", Help: "Agent 上行投递尝试数。",
	}, []string{"outcome"})
	agentUplinkSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_agent_uplink_dispatch_seconds", Help: "Agent 解析上行目标并写入目标入队流的耗时。", Buckets: buckets,
	})
	agentDownSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tx_agent_downlink_sent_total", Help: "Agent 成功写入客户端连接的下行帧数。",
	})
	agentDownErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tx_agent_downlink_write_errors_total", Help: "Agent 下行 Socket 写入失败次数。",
	})
	agentDownSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_agent_downlink_write_seconds", Help: "Agent 向客户端连接写入下行帧的耗时。", Buckets: buckets,
	})
	msgqPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tx_msgq_downlink_published_total", Help: "MsgQ 成功入队的下行消息数。",
	}, []string{"kind"})
	msgqPublishErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tx_msgq_downlink_publish_errors_total", Help: "MsgQ 下行发布失败次数。",
	}, []string{"kind"})
	msgqPublishSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tx_msgq_downlink_publish_seconds", Help: "MsgQ 下行消息入队耗时。", Buckets: buckets,
	}, []string{"kind"})
	rpcSubmit = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tx_rpc_stream_submit_total", Help: "目标服务流式消息入邮箱尝试数。",
	}, []string{"outcome"})
	rpcSubmitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_rpc_stream_submit_seconds", Help: "目标服务流式消息入邮箱耗时。", Buckets: buckets,
	})
	rpcStreamSendLockSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_rpc_stream_send_lock_seconds", Help: "单向流消息等待发送锁耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	rpcStreamSendSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_rpc_stream_send_seconds", Help: "单向流消息调用 gRPC Send 的耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	mailboxSendQueueSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_msgq_mailbox_send_queue_seconds", Help: "MsgQ 下行消息在共享流发送队列中的等待耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	mailboxSendSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_msgq_mailbox_send_seconds", Help: "MsgQ 下行消息调用 gRPC Send 的耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	mailboxReceiveQueueSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_agent_mailbox_receive_queue_seconds", Help: "Agent 收到下行消息后在 Session 队列中的等待耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	actorMailboxWaitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_actor_mailbox_wait_seconds", Help: "Actor 消息入邮箱后到开始执行的等待耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	publishStreamQueueSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_msgq_publish_stream_queue_seconds", Help: "下行消息在发布流本地队列中的等待耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	publishStreamSendSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_msgq_publish_stream_send_seconds", Help: "下行消息调用发布流 gRPC Send 的耗时，每 64 条抽样一条。", Buckets: buckets,
	})
	publishStreamErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tx_msgq_publish_stream_errors_total", Help: "下行发布流异步写入失败事件数。",
	})

	uplinkAccepted        = agentUplink.WithLabelValues("accepted")
	uplinkError           = agentUplink.WithLabelValues("error")
	publishSession        = msgqPublished.WithLabelValues("session")
	publishRole           = msgqPublished.WithLabelValues("role")
	publishSessionError   = msgqPublishErrors.WithLabelValues("session")
	publishRoleError      = msgqPublishErrors.WithLabelValues("role")
	publishSessionSeconds = msgqPublishSeconds.WithLabelValues("session")
	publishRoleSeconds    = msgqPublishSeconds.WithLabelValues("role")
	submitAccepted        = rpcSubmit.WithLabelValues("accepted")
	submitError           = rpcSubmit.WithLabelValues("error")
	mailboxSnapshot       atomic.Pointer[snapshotSource]
)

type snapshotSource struct {
	read func() (sessions, messages, bytes int)
}

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		agentUplink, agentUplinkSeconds, agentDownSent, agentDownErrors, agentDownSeconds,
		msgqPublished, msgqPublishErrors, msgqPublishSeconds, rpcSubmit, rpcSubmitSeconds,
		rpcStreamSendLockSeconds, rpcStreamSendSeconds,
		mailboxSendQueueSeconds, mailboxSendSeconds, mailboxReceiveQueueSeconds,
		actorMailboxWaitSeconds, publishStreamQueueSeconds, publishStreamSendSeconds, publishStreamErrors,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "tx_msgq_sessions", Help: "本进程 MsgQ 会话数。"}, func() float64 {
			if source := mailboxSnapshot.Load(); source != nil {
				sessions, _, _ := source.read()
				return float64(sessions)
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "tx_msgq_backlog_messages", Help: "本进程未确认下行消息数。"}, func() float64 {
			if source := mailboxSnapshot.Load(); source != nil {
				_, messages, _ := source.read()
				return float64(messages)
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "tx_msgq_backlog_bytes", Help: "本进程未确认下行消息正文的字节数。"}, func() float64 {
			if source := mailboxSnapshot.Load(); source != nil {
				_, _, bytes := source.read()
				return float64(bytes)
			}
			return 0
		}),
	)
}

// Handler 返回独立注册表的 Prometheus 采集入口。
func Handler() http.Handler { return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}) }

// RegisterMailboxSnapshot 仅在 MsgQ 服务运行期间提供邮箱快照。
func RegisterMailboxSnapshot(read func() (sessions, messages, bytes int)) func() {
	source := &snapshotSource{read: read}
	mailboxSnapshot.Store(source)
	return func() { mailboxSnapshot.CompareAndSwap(source, nil) }
}

// RecordAgentUplink 统计向目标服务写入上行流的结果和耗时。
func RecordAgentUplink(accepted bool, duration time.Duration) {
	if accepted {
		uplinkAccepted.Inc()
	} else {
		uplinkError.Inc()
	}
	agentUplinkSeconds.Observe(duration.Seconds())
}

// RecordAgentDownlink 只把成功写入 Socket 的帧计入发送量。
func RecordAgentDownlink(frames int, duration time.Duration, err error) {
	if err != nil {
		agentDownErrors.Inc()
	} else {
		agentDownSent.Add(float64(frames))
	}
	agentDownSeconds.Observe(duration.Seconds())
}

// RecordMsgQPublish 统计实际入队数量；部分成功的广播也保留已入队数。
func RecordMsgQPublish(kind PublishKind, delivered int, duration time.Duration, err error) {
	if kind == PublishRole {
		if delivered > 0 {
			publishRole.Add(float64(delivered))
		}
		if err != nil {
			publishRoleError.Inc()
		}
		publishRoleSeconds.Observe(duration.Seconds())
		return
	}
	if delivered > 0 {
		publishSession.Add(float64(delivered))
	}
	if err != nil {
		publishSessionError.Inc()
	}
	publishSessionSeconds.Observe(duration.Seconds())
}

// RecordRPCSubmit 统计目标服务对一条单向流消息的邮箱入队结果。
func RecordRPCSubmit(err error, duration time.Duration) {
	if err == nil {
		submitAccepted.Inc()
	} else {
		submitError.Inc()
	}
	rpcSubmitSeconds.Observe(duration.Seconds())
}

// RecordRPCStreamSend 记录单向流消息的抽样锁等待和写入耗时。
func RecordRPCStreamSend(lockWait, send time.Duration) {
	rpcStreamSendLockSeconds.Observe(lockWait.Seconds())
	rpcStreamSendSeconds.Observe(send.Seconds())
}

// RecordMailboxSend 记录 MsgQ 下行共享流的抽样排队和发送耗时。
func RecordMailboxSend(queue, send time.Duration) {
	mailboxSendQueueSeconds.Observe(queue.Seconds())
	mailboxSendSeconds.Observe(send.Seconds())
}

// RecordMailboxReceiveQueue 记录 Agent 下行消息在 Session 队列中的抽样等待耗时。
func RecordMailboxReceiveQueue(wait time.Duration) {
	mailboxReceiveQueueSeconds.Observe(wait.Seconds())
}

// RecordActorMailboxWait 记录 Actor 邮箱的抽样等待耗时。
func RecordActorMailboxWait(wait time.Duration) {
	actorMailboxWaitSeconds.Observe(wait.Seconds())
}

// RecordPublishStream 记录下行发布流的抽样排队和发送耗时。
func RecordPublishStream(queue, send time.Duration) {
	publishStreamQueueSeconds.Observe(queue.Seconds())
	publishStreamSendSeconds.Observe(send.Seconds())
}

// RecordPublishStreamError 统计异步写流失败。
func RecordPublishStreamError() { publishStreamErrors.Inc() }
