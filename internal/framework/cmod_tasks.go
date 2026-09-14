package framework

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ngnl47/tx/internal/taskqueue"
	"github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/tasks"
)

type taskRuntime struct {
	conn             *nats.Conn
	js               jetstream.JetStream
	config           tasks.Config
	consumer         jetstream.Consumer
	worker           *taskqueue.Worker
	recoveryConsumer jetstream.Consumer
	recovery         taskqueue.Recovery
	recoveryWorker   *taskqueue.Worker
}

func (r *taskRuntime) Publish(ctx context.Context, id actor.Identity, delivery actor.Delivery) error {
	data, err := json.Marshal(taskqueue.Task{Identity: id, Delivery: delivery})
	if err != nil {
		return err
	}
	_, err = r.js.Publish(ctx, r.config.Subject, data, jetstream.WithExpectStream(r.config.Stream))
	return err
}

func (server *Server) tasksCMod() CMod {
	return &builtInCMod{
		spec: CModSpec{ID: CModTaskQueue, Requires: []CModID{CModActor}},
		validate: func(CModContext) error {
			c := server.config.TaskQueue
			if c.URL == "" || c.Stream == "" || c.Subject == "" {
				return errors.New("tasks: URL, stream and subject required")
			}
			if c.Consumer != "" && (c.DeadLetter == "" || c.Timeout <= 0) {
				return errors.New("tasks: consumer requires dead letter subject and timeout")
			}
			return nil
		},
		start: func(c CModContext) error {
			config := server.config.TaskQueue
			conn, err := nats.Connect(config.URL)
			if err != nil {
				return err
			}
			r := &taskRuntime{conn: conn, config: config}
			server.runtime.taskQueue = r // 启动中途失败也由逆序关闭释放连接。
			r.js, err = jetstream.New(conn)
			if err != nil {
				return err
			}
			ctx := server.runtime.lifecycle
			stream, err := r.js.Stream(ctx, config.Stream)
			if err != nil {
				return err
			}
			info, err := stream.Info(ctx)
			if err != nil {
				return err
			}
			if info.Config.Storage != jetstream.FileStorage || info.Config.Retention != jetstream.WorkQueuePolicy {
				return errors.New("tasks: file-backed work queue stream required")
			}
			if config.Consumer == "" {
				return nil
			}
			r.consumer, err = stream.Consumer(ctx, config.Consumer)
			if err != nil {
				return err
			}
			// 按主题定位，避免转存时误发到非持久流。
			deadName, err := r.js.StreamNameBySubject(ctx, config.DeadLetter)
			if err != nil {
				return err
			}
			if deadName == config.Stream {
				return errors.New("tasks: dead letter stream must differ from source")
			}
			dead, err := r.js.Stream(ctx, deadName)
			if err != nil {
				return err
			}
			deadInfo, err := dead.Info(ctx)
			if err != nil {
				return err
			}
			if deadInfo.Config.Storage != jetstream.FileStorage || deadInfo.Config.Retention != jetstream.LimitsPolicy {
				return errors.New("tasks: file-backed limits retention dead letter stream required")
			}
			infoConsumer, err := r.consumer.Info(ctx)
			if err != nil {
				return err
			}
			if config.RecoveryStream == "" && config.RecoveryConsumer == "" && infoConsumer.Config.MaxDeliver <= 0 {
				return nil
			}
			if config.RecoveryStream == "" || config.RecoveryConsumer == "" {
				return errors.New("tasks: finite max deliver requires recovery stream and consumer")
			}
			events, err := r.js.Stream(ctx, config.RecoveryStream)
			if err != nil {
				return err
			}
			eventInfo, err := events.Info(ctx)
			if err != nil {
				return err
			}
			eventSubject := taskqueue.MaxDeliverySubject(config.Stream, config.Consumer)
			if eventInfo.Config.Storage != jetstream.FileStorage || eventInfo.Config.Retention != jetstream.WorkQueuePolicy || len(eventInfo.Config.Subjects) != 1 || eventInfo.Config.Subjects[0] != eventSubject {
				return errors.New("tasks: dedicated file work queue for max delivery advisories required")
			}
			r.recoveryConsumer, err = events.Consumer(ctx, config.RecoveryConsumer)
			if err != nil {
				return err
			}
			r.recovery = taskqueue.Recovery{JetStream: r.js, Source: stream, Consumer: r.consumer, DeadLetter: config.DeadLetter}
			return nil
		},
		stop: func(context.Context) error {
			if r := server.runtime.taskQueue; r != nil {
				r.conn.Close()
			}
			return nil
		},
	}
}

// 消费必须晚于服务初始化及 RPC 启动，不能在 CMod 启动阶段触发业务加载。
func (server *Server) startTaskConsumer(ctx context.Context) error {
	r := server.runtime.taskQueue
	if r == nil || r.consumer == nil {
		return nil
	}
	if r.recoveryConsumer != nil {
		worker, err := taskqueue.StartRecovery(ctx, r.recoveryConsumer, r.recovery, r.config.Timeout)
		r.recoveryWorker = worker
		if err != nil {
			return err
		}
	}
	worker, err := taskqueue.Start(ctx, r.consumer, taskqueue.Processor{Actor: server.runtime.actor, JetStream: r.js, DeadLetter: r.config.DeadLetter, Timeout: r.config.Timeout})
	r.worker = worker
	return err
}

func (server *Server) drainTaskConsumer(ctx context.Context) error {
	server.mu.Lock()
	r := server.runtime
	server.mu.Unlock()
	if r == nil || r.taskQueue == nil {
		return nil
	}
	if r.taskQueue.worker != nil {
		if err := r.taskQueue.worker.Drain(ctx); err != nil {
			return err
		}
	}
	if r.taskQueue.recoveryWorker != nil {
		return r.taskQueue.recoveryWorker.Drain(ctx)
	}
	return nil
}
