package actor

import (
	"runtime"
	"sync"
	"sync/atomic"

	rbqueue "github.com/Workiva/go-datastructures/queue"
	protoactor "github.com/asynkron/protoactor-go/actor"

	txlog "github.com/ngnl47/tx/log"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

const (
	mailboxIdle int32 = iota
	mailboxRunning
)

func observedBoundedMailbox(config Config, kind string, lifecycle *mailboxLifecycle) protoactor.MailboxProducer {
	overflow := &mailboxOverflowObserver{
		clusterName: config.ClusterName,
		host:        config.Host,
		kind:        kind,
		capacity:    config.MailboxCapacity,
	}
	return func() protoactor.Mailbox {
		return &boundedMailbox{
			user:      rbqueue.NewRingBuffer(uint64(config.MailboxCapacity)),
			overflow:  overflow,
			lifecycle: lifecycle,
		}
	}
}

type mailboxOverflowObserver struct {
	clusterName string
	host        string
	kind        string
	capacity    int
	dropped     atomic.Uint64
	rejected    atomic.Uint64
}

func (o *mailboxOverflowObserver) report() {
	dropped := o.dropped.Add(1)
	if dropped&(dropped-1) != 0 {
		return
	}
	txlog.Error("actor mailbox overflow",
		"cluster", o.clusterName,
		"host", o.host,
		"kind", o.kind,
		"capacity", o.capacity,
		"dropped", dropped,
	)
}

func (o *mailboxOverflowObserver) reportRejected(err error) {
	rejected := o.rejected.Add(1)
	if rejected&(rejected-1) != 0 {
		return
	}
	txlog.Error("actor mailbox rejected after lifecycle stopped",
		"cluster", o.clusterName,
		"host", o.host,
		"kind", o.kind,
		"rejected", rejected,
		"err", err,
	)
}

type boundedMailbox struct {
	user      *rbqueue.RingBuffer
	system    systemMailbox
	overflow  *mailboxOverflowObserver
	lifecycle *mailboxLifecycle

	schedulerStatus atomic.Int32
	userMessages    atomic.Int32
	systemMessages  atomic.Int32
	suspended       atomic.Int32
	invoker         protoactor.MessageInvoker
	dispatcher      protoactor.Dispatcher
}

func (m *boundedMailbox) PostUserMessage(message interface{}) {
	if batch, ok := message.(protoactor.MessageBatch); ok {
		for _, item := range batch.GetMessages() {
			m.PostUserMessage(item)
		}
		return
	}
	if envelope, ok := message.(*protoactor.MessageEnvelope); ok {
		if batch, ok := envelope.Message.(protoactor.MessageBatch); ok {
			for _, item := range batch.GetMessages() {
				m.PostUserMessage(item)
			}
			return
		}
	}
	tracked := tracksLifecycle(message)
	if tracked {
		if err := m.lifecycle.begin(); err != nil {
			m.overflow.reportRejected(err)
			m.rejectRequest(message, err)
			return
		}
	}
	accepted, err := m.user.Offer(message)
	if err != nil || !accepted {
		if tracked {
			m.lifecycle.end()
		}
		m.overflow.report()
		return
	}
	m.userMessages.Add(1)
	m.schedule()
}

// rejectRequest 只报告入队前的准入失败，不为普通 Tell 增加回执。
func (m *boundedMailbox) rejectRequest(message interface{}, err error) {
	envelope, ok := message.(*protoactor.MessageEnvelope)
	if !ok || envelope.Sender == nil {
		return
	}
	delivery, ok := envelope.Message.(*actorpb.Delivery)
	if !ok || (delivery.GetMode() != actorpb.DeliveryMode_DELIVERY_MODE_CALL && delivery.GetMode() != actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED) {
		return
	}
	// 只读取 ActorSystem，不从邮箱线程访问 Actor 的当前消息上下文。
	system := m.invoker.(actorSystemContext).ActorSystem()
	system.Root.Send(envelope.Sender, &actorpb.Result{
		RequestId: delivery.GetRequestId(), CommandId: delivery.GetCommandId(),
		ErrorCode: CallErrorCodeRejected, ErrorMessage: err.Error(),
	})
}

func (m *boundedMailbox) PostSystemMessage(message interface{}) {
	m.system.push(message)
	m.systemMessages.Add(1)
	m.schedule()
}

func (m *boundedMailbox) RegisterHandlers(invoker protoactor.MessageInvoker, dispatcher protoactor.Dispatcher) {
	m.invoker = invoker
	m.dispatcher = dispatcher
}

func (m *boundedMailbox) Start() {}

func (m *boundedMailbox) UserMessageCount() int {
	return int(m.userMessages.Load())
}

func (m *boundedMailbox) schedule() {
	if m.schedulerStatus.CompareAndSwap(mailboxIdle, mailboxRunning) {
		m.dispatcher.Schedule(m.processMessages)
	}
}

func (m *boundedMailbox) processMessages() {
process:
	m.run()
	m.schedulerStatus.Store(mailboxIdle)
	if m.systemMessages.Load() > 0 || (m.suspended.Load() == 0 && m.userMessages.Load() > 0) {
		if m.schedulerStatus.CompareAndSwap(mailboxIdle, mailboxRunning) {
			goto process
		}
	}
}

func (m *boundedMailbox) run() {
	var message interface{}
	processingTrackedMessage := false
	defer func() {
		if recovered := recover(); recovered != nil {
			if processingTrackedMessage {
				m.lifecycle.end()
			}
			m.invoker.EscalateFailure(recovered, message)
		}
	}()

	throughput := m.dispatcher.Throughput()
	for processed := 0; ; processed++ {
		if processed > throughput {
			processed = 0
			runtime.Gosched()
		}
		if message = m.system.pop(); message != nil {
			m.systemMessages.Add(-1)
			switch message.(type) {
			case *protoactor.SuspendMailbox:
				m.suspended.Store(1)
			case *protoactor.ResumeMailbox:
				m.suspended.Store(0)
			default:
				m.invoker.InvokeSystemMessage(message)
			}
			continue
		}
		if m.suspended.Load() == 1 {
			return
		}
		if m.user.Len() == 0 {
			return
		}
		var err error
		message, err = m.user.Get()
		if err != nil {
			return
		}
		m.userMessages.Add(-1)
		processingTrackedMessage = tracksLifecycle(message)
		m.invoker.InvokeUserMessage(message)
		if processingTrackedMessage {
			processingTrackedMessage = false
			m.lifecycle.end()
		}
	}
}

func tracksLifecycle(message interface{}) bool {
	if envelope, ok := message.(*protoactor.MessageEnvelope); ok {
		message = envelope.Message
	}
	_, ok := message.(*actorpb.Delivery)
	return ok
}

type systemMailbox struct {
	mu       sync.Mutex
	messages []interface{}
	head     int
}

func (q *systemMailbox) push(message interface{}) {
	q.mu.Lock()
	q.messages = append(q.messages, message)
	q.mu.Unlock()
}

func (q *systemMailbox) pop() interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.messages) {
		return nil
	}
	message := q.messages[q.head]
	q.messages[q.head] = nil
	q.head++
	if q.head == len(q.messages) {
		q.messages = nil
		q.head = 0
	}
	return message
}
