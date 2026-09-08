package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ngnl47/tx/storage/task"
)

var ErrBackendNotConfigured = errors.New("memory task: backend is not configured")

type Backend struct {
	mu      sync.Mutex
	seq     int64
	pending map[string]*entry
	leased  map[string]*entry
}

type entry struct {
	task        task.DelayTask
	followup    task.DelayTask
	hasFollowup bool
	attempts    int
	token       string
}

func NewBackend() *Backend {
	return &Backend{
		pending: map[string]*entry{},
		leased:  map[string]*entry{},
	}
}

func (b *Backend) Enqueue(ctx context.Context, item task.DelayTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := task.ValidateDelayTask(item); err != nil {
		return err
	}
	if b == nil {
		return ErrBackendNotConfigured
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := item.IdempotencyKey()
	if existing := b.pending[key]; existing != nil {
		existing.task = item
		return nil
	}
	if existing := b.leased[key]; existing != nil {
		existing.followup = item
		existing.hasFollowup = true
		return nil
	}
	b.pending[key] = &entry{task: item}
	return nil
}

func (b *Backend) ClaimDue(ctx context.Context, now time.Time, limit int) ([]task.DelayTaskLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrBackendNotConfigured
	}
	if limit <= 0 {
		return nil, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	keys := make([]string, 0, len(b.pending))
	for key, item := range b.pending {
		if !item.task.DueAt.After(now) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		left := b.pending[keys[i]].task
		right := b.pending[keys[j]].task
		if left.DueAt.Equal(right.DueAt) {
			return keys[i] < keys[j]
		}
		return left.DueAt.Before(right.DueAt)
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}

	leases := make([]task.DelayTaskLease, 0, len(keys))
	for _, key := range keys {
		item := b.pending[key]
		delete(b.pending, key)
		b.seq++
		item.token = fmt.Sprintf("lease-%d", b.seq)
		item.attempts++
		b.leased[key] = item
		leases = append(leases, task.DelayTaskLease{
			Task:     item.task,
			Token:    item.token,
			Attempts: item.attempts,
		})
	}
	return leases, nil
}

func (b *Backend) Complete(ctx context.Context, lease task.DelayTaskLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil {
		return ErrBackendNotConfigured
	}
	if err := task.ValidateDelayTaskLease(lease); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := lease.Task.IdempotencyKey()
	item := b.leased[key]
	if item == nil || item.token != lease.Token {
		return nil
	}
	delete(b.leased, key)
	if item.hasFollowup {
		item.task = item.followup
		item.followup = task.DelayTask{}
		item.hasFollowup = false
		item.attempts = 0
		item.token = ""
		b.pending[key] = item
	}
	return nil
}

func (b *Backend) Retry(ctx context.Context, lease task.DelayTaskLease, next time.Time, reason error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil {
		return ErrBackendNotConfigured
	}
	if err := task.ValidateDelayTaskLease(lease); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := lease.Task.IdempotencyKey()
	item := b.leased[key]
	if item == nil || item.token != lease.Token {
		return task.ErrLeaseLost
	}
	delete(b.leased, key)
	if item.hasFollowup {
		item.task = item.followup
		item.followup = task.DelayTask{}
		item.hasFollowup = false
	}
	item.task.DueAt = next
	item.token = ""
	b.pending[key] = item
	_ = reason
	return nil
}
