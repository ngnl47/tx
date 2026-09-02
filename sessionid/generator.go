// Package sessionid 提供进程内并发安全的 SessionID 分配能力。
package sessionid

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	EpochUnix    int64 = 1767225600
	MaxSessionID int64 = 1<<53 - 1

	timestampMask int64  = 1<<29 - 1
	maxSequence   uint16 = 1<<12 - 1
	maxWorkSlot   uint16 = 1<<12 - 1
)

var (
	ErrInvalidWorkSlot  = errors.New("sessionid: invalid workSlot")
	ErrClockBeforeEpoch = errors.New("sessionid: clock is before epoch")
)

// Allocator 分配当前集群内有效 Session 生命周期范围内唯一的 SessionID。
type Allocator interface {
	AllocateSessionID(context.Context) (int64, error)
}

// Generator 使用秒级时间、workSlot 和秒内序列分配 53 位 SessionID。
type Generator struct {
	mu           sync.Mutex
	workSlot     uint16
	lastSecond   int64
	nextSequence uint16
	now          func() time.Time
	wait         func(context.Context, time.Duration) error
}

var _ Allocator = (*Generator)(nil)

// New 创建绑定指定 workSlot 的 SessionID 生成器。
func New(workSlot uint16) (*Generator, error) {
	if workSlot == 0 || workSlot > maxWorkSlot {
		return nil, ErrInvalidWorkSlot
	}
	return &Generator{
		workSlot: workSlot,
		now:      time.Now,
		wait:     waitContext,
	}, nil
}

// AllocateSessionID 分配一个正数且不超过 JavaScript 安全整数上限的 SessionID。
func (generator *Generator) AllocateSessionID(ctx context.Context) (int64, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()

	for {
		now := generator.now()
		rawSecond := now.Unix()
		if rawSecond < EpochUnix {
			return 0, ErrClockBeforeEpoch
		}
		if generator.lastSecond != 0 && rawSecond < generator.lastSecond {
			if err := generator.wait(ctx, time.Unix(generator.lastSecond, 0).Sub(now)); err != nil {
				return 0, err
			}
			continue
		}
		if rawSecond != generator.lastSecond {
			generator.lastSecond = rawSecond
			generator.nextSequence = 0
		} else if generator.nextSequence > maxSequence {
			if err := generator.wait(ctx, time.Unix(rawSecond+1, 0).Sub(now)); err != nil {
				return 0, err
			}
			continue
		}

		sequence := generator.nextSequence
		generator.nextSequence++
		timestamp := (rawSecond - EpochUnix) & timestampMask
		return (timestamp << 24) | (int64(generator.workSlot) << 12) | int64(sequence), nil
	}
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
