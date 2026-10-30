package actorplacement

import (
	"context"
	"sync"
	"time"

	"github.com/maypok86/otter/v2"
	txactor "github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
)

// Cache 只保存不可变寻址结果，不承担业务实体的安全卸载。
// 等待者可独立取消；每次冷加载由 Tasks 托管，关停会等待实际查询收尾。
type Cache struct {
	mu          sync.Mutex
	values      *otter.Cache[txactor.Identity, string]
	pending     map[txactor.Identity]*slotLoad
	tasks       lifecycle.Tasks
	resolve     txactor.SlotResolver
	lastWarning time.Time
}
type slotLoad struct {
	done  chan struct{}
	query context.Context
	slot  string
	err   error
}

func NewCache(tasks lifecycle.Tasks, resolve txactor.SlotResolver) (*Cache, error) {
	values, err := otter.New(&otter.Options[txactor.Identity, string]{MaximumSize: 65536})
	if err != nil {
		return nil, err
	}
	return &Cache{values: values, pending: make(map[txactor.Identity]*slotLoad), tasks: tasks, resolve: resolve}, nil
}

func (c *Cache) Resolve(ctx context.Context, id txactor.Identity) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var warningCount int
	c.mu.Lock()
	if slot, ok := c.values.GetIfPresent(id); ok {
		c.mu.Unlock()
		return slot, nil
	}
	call := c.pending[id]
	if call == nil {
		query, cancel := context.WithTimeout(c.tasks, QueryTimeout)
		call = &slotLoad{done: make(chan struct{}), query: query}
		c.pending[id] = call
		if err := c.tasks.Go(func(context.Context) {
			defer cancel()
			slot, err := c.resolve.Resolve(query, id)
			if query.Err() != nil {
				err = query.Err()
			}
			c.mu.Lock()
			if err == nil {
				c.values.Set(id, slot)
			}
			call.slot, call.err = slot, err
			delete(c.pending, id)
			close(call.done)
			c.mu.Unlock()
		}); err != nil {
			cancel()
			delete(c.pending, id)
			c.mu.Unlock()
			return "", err
		}
		if pending := len(c.pending); pending > queryWarningThreshold {
			now := time.Now()
			if now.Sub(c.lastWarning) >= queryWarningInterval {
				c.lastWarning = now
				warningCount = pending
			}
		}
	}
	c.mu.Unlock()
	// 告警只观察实际在途数量，不拒绝查询，也不在缓存锁内写日志。
	if warningCount > 0 {
		txlog.Warn("actor placement cold queries exceeded warning threshold", "inFlight", warningCount, "threshold", queryWarningThreshold)
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-call.query.Done():
		// 查询已经成功结束时，清理 Context 不应把成功结果改成取消。
		select {
		case <-call.done:
		default:
			return "", call.query.Err()
		}
	case <-call.done:
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return call.slot, call.err
}

// QueryTimeout 限制冷查询等待，超时不代表业务解析器已经退出。
const QueryTimeout = 3 * time.Second

// 这是观察阈值而非接纳上限；持续过载最多每分钟告警一次。
const (
	queryWarningThreshold = 1024
	queryWarningInterval  = time.Minute
)
