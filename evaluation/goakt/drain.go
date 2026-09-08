package goakteval

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
)

type shutdownState struct {
	sync.Mutex
	stopped bool
	err     error
}

// Drain 只验证本进程经四种投递入口进入、没有后台消息生产者的 Role Grain。
// 它不停止运行时、不执行持久化，也不允许重新打开准入。
func (r *Runtime) Drain(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("drain deadline required")
	}
	r.ingress.BeginDrain()
	if err := r.ingress.WaitContext(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 已准入调用均已完成提交或返回错误，此后不会再注册新角色或投递业务。
	// 当前系统没有启用集群：Grains 只读取 GoAkt 的进程内目录，不查询 etcd。
	for _, id := range r.system.Grains(ctx, time.Until(deadline)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := r.system.AskGrain(ctx, id, &emptypb.Empty{}, time.Until(deadline))
		if err != nil {
			return fmt.Errorf("drain role %s: %w", id.Name(), err)
		}
		if _, ok := response.(*emptypb.Empty); !ok {
			return errors.New("invalid drain acknowledgement")
		}
	}
	return nil
}

// Shutdown 在业务排空后才调用项目收尾；失败时保持运行时和关闭的准入，供上层有限重试。
// 不在这里实现重试次数，也不把 GoAkt Stop 成功当作持久化成功。
func (r *Runtime) Shutdown(ctx context.Context, flush func(context.Context) error) error {
	r.shutdown.Lock()
	defer r.shutdown.Unlock()
	if r.shutdown.stopped {
		return r.shutdown.err
	}
	if err := r.Drain(ctx); err != nil {
		return err
	}
	if err := flush(ctx); err != nil {
		return err
	}
	r.shutdown.err = r.system.Stop(ctx)
	r.shutdown.stopped = true
	return r.shutdown.err
}
