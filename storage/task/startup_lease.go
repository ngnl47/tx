package task

import (
	"context"
	"time"
)

// StartupLeaseStatus 表示启动恢复租约的当前状态。
type StartupLeaseStatus uint8

const (
	StartupLeaseStatusMissing StartupLeaseStatus = iota
	StartupLeaseStatusAcquired
	StartupLeaseStatusRunning
	StartupLeaseStatusComplete
)

// StartupLease 协调一组并发启动的进程只执行一次昂贵恢复。
// token fencing 保证过期 owner 不能覆盖后继 owner 的状态。
type StartupLease interface {
	Acquire(context.Context, string, time.Duration) (StartupLeaseStatus, error)
	Renew(context.Context, string, time.Duration) (bool, error)
	Complete(context.Context, string, time.Duration) (bool, error)
	Release(context.Context, string) (bool, error)
}
