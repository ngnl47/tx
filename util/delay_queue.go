package util

import (
	"container/heap"
	"errors"
	"strconv"
	"sync"
	"time"
)

// TaskID 任务ID
type TaskID int64

// String 返回任务ID的字符串表示
func (id TaskID) String() string {
	return strconv.FormatInt(int64(id), 10)
}

// ErrDelayQueueStopped 表示队列已经开始停止，不再接纳任务。
var ErrDelayQueueStopped = errors.New("delay queue: stopped")

// DelayQueue 是业务拥有的本地有序定时器，不接入 CMod 或持久化任务系统。
// 回调按到期时间串行执行，同一到期时间按添加顺序执行；慢回调会推迟后续任务。
type DelayQueue struct {
	mu           sync.Mutex
	tasks        *taskHeap     // 自定义排序的小顶堆
	tickInterval time.Duration // tick 间隔
	stop         chan struct{}
	done         chan struct{}
	seq          int64
	started      bool
	stopped      bool
	taskIndex    map[TaskID]*delayedTask // 包括已派发但尚未完成的周期任务
}

// delayedTask 延迟任务
type delayedTask struct {
	executeAt time.Time
	task      func()
	id        TaskID
	interval  time.Duration
	index     int // 已从堆中取出时为 -1
}

// taskHeap 任务堆，实现 heap.Interface
type taskHeap []*delayedTask

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// 首先按执行时间排序
	if !h[i].executeAt.Equal(h[j].executeAt) {
		return h[i].executeAt.Before(h[j].executeAt)
	}
	// 执行时间相同，按序列号排序（保证添加顺序）
	return h[i].id < h[j].id
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x interface{}) {
	task := x.(*delayedTask)
	*h = append(*h, task)
	task.index = len(*h) - 1
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	x.index = -1
	return x
}

// NewDelayQueue 创建一个新的延迟队列
func NewDelayQueue(tickInterval time.Duration) *DelayQueue {
	tasks := &taskHeap{}
	return &DelayQueue{
		tasks:        tasks,
		tickInterval: tickInterval,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		taskIndex:    make(map[TaskID]*delayedTask),
	}
}

// Start 启动延迟队列。重复调用无效果；Stop 后不能重新启动。
func (dq *DelayQueue) Start() {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.started {
		return
	}

	ticker := time.NewTicker(dq.tickInterval)
	dq.started = true
	go dq.run(ticker)
}

// Stop 停止接纳与续排，等待已经派发的到期批次完成，然后释放尚未派发的任务。
// 所有并发 Stop 均等待实际结束；不能在本队列的回调中同步调用 Stop。
// Start 前调用无效果；回调退出由业务保证，Stop 不强制中断回调。
func (dq *DelayQueue) Stop() {
	dq.mu.Lock()
	if !dq.started {
		dq.mu.Unlock()
		return
	}
	if !dq.stopped {
		dq.stopped = true
		close(dq.stop)
	}
	dq.mu.Unlock()
	<-dq.done
}

// AddScheduled 添加周期任务：首次在下一次调度时执行，随后按回调完成时间加 interval 续排。
// 周期任务保持同一个 TaskID，不补跑错过的周期。
func (dq *DelayQueue) AddScheduled(interval time.Duration, f func()) (*TaskID, error) {
	if f == nil {
		return nil, nil
	}
	if interval <= 0 {
		return nil, nil
	}

	return dq.add(0, interval, f)
}

// AfterFunc 添加单次延时任务；停止后或参数无效时返回 nil。
func (dq *DelayQueue) AfterFunc(d time.Duration, f func()) *TaskID {
	taskID, _ := dq.Add(d, f)
	return taskID
}

// Add 添加仅执行一次的延时任务
func (dq *DelayQueue) Add(delay time.Duration, f func()) (*TaskID, error) {
	if f == nil {
		return nil, nil
	}
	if delay < 0 {
		return nil, nil
	}

	return dq.add(delay, 0, f)
}

func (dq *DelayQueue) add(delay, interval time.Duration, f func()) (*TaskID, error) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	if dq.stopped {
		return nil, ErrDelayQueueStopped
	}
	dq.seq++
	taskID := TaskID(dq.seq)
	task := &delayedTask{executeAt: time.Now().Add(delay), task: f, id: taskID, interval: interval}
	dq.taskIndex[taskID] = task
	heap.Push(dq.tasks, task)
	return &taskID, nil
}

// Remove 取消尚未开始的回调以及周期任务的后续执行，不中断已经开始的回调。
func (dq *DelayQueue) Remove(taskID *TaskID) error {
	if taskID == nil {
		return nil
	}

	dq.mu.Lock()
	defer dq.mu.Unlock()

	task, ok := dq.taskIndex[*taskID]
	if !ok {
		return nil
	}

	if task.index >= 0 {
		heap.Remove(dq.tasks, task.index)
	}
	delete(dq.taskIndex, *taskID)
	return nil
}

func (dq *DelayQueue) run(ticker *time.Ticker) {
	defer func() {
		ticker.Stop()
		dq.mu.Lock()
		*dq.tasks = nil
		clear(dq.taskIndex)
		dq.mu.Unlock()
		close(dq.done)
	}()
	for {
		select {
		case <-ticker.C:
			for _, task := range dq.takeDue(time.Now()) {
				dq.execute(task)
			}
		case <-dq.stop:
			return
		}
	}
}

// takeDue 在锁内确定本次派发批次；回调始终在锁外执行，允许自行添加或取消任务。
func (dq *DelayQueue) takeDue(now time.Time) []*delayedTask {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	var due []*delayedTask
	if !dq.stopped {
		for dq.tasks.Len() > 0 && !(*dq.tasks)[0].executeAt.After(now) {
			due = append(due, heap.Pop(dq.tasks).(*delayedTask))
		}
	}
	return due
}

func (dq *DelayQueue) execute(task *delayedTask) {
	dq.mu.Lock()
	if dq.taskIndex[task.id] != task {
		dq.mu.Unlock()
		return
	}
	if task.interval == 0 {
		delete(dq.taskIndex, task.id)
	}
	dq.mu.Unlock()
	task.task()
	if task.interval > 0 {
		dq.mu.Lock()
		if !dq.stopped && dq.taskIndex[task.id] == task {
			task.executeAt = time.Now().Add(task.interval)
			heap.Push(dq.tasks, task)
		} else {
			delete(dq.taskIndex, task.id)
		}
		dq.mu.Unlock()
	}
}
