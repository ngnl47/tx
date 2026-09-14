package actorhost

import "errors"

var ErrCapacity = errors.New("actortrial: activation capacity unavailable")

type ActivationState string

const (
	Unloaded     ActivationState = "unloaded"
	Initializing ActivationState = "initializing"
	Ready        ActivationState = "ready"
	Closing      ActivationState = "closing"
	Failed       ActivationState = "failed"
)

// ActivationStatus 仅含运行时元数据，查询不读取业务对象或未初始化的 GD。
type ActivationStatus struct {
	State ActivationState
	// PendingMessages 只在探测时读取队列长度，不添加消息路径采集计数。
	PendingMessages int          `json:",omitempty"`
	Failure         *RemoteError `json:",omitempty"`
}

// ActivationLimits 的零上限不限制；Actors 包含初始化、运行和清理中的全部激活。
// Admit 是锁内轻量只读准入，不能执行 I/O 或回调 Runtime；不阻断已有激活。
type ActivationLimits struct {
	Actors          int
	Initializations int
	Admit           func() bool
}

type ActivationUsage struct {
	Actors       int
	Initializing int
}

func (r *Runtime) SetActivationLimits(limits ActivationLimits) {
	if limits.Actors < 0 || limits.Initializations < 0 {
		panic("actortrial: invalid activation limits")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) != 0 || r.draining {
		panic("actortrial: configure activation limits before use")
	}
	r.limits = limits
}

func (r *Runtime) ActivationUsage() ActivationUsage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ActivationUsage{Actors: len(r.entries), Initializing: r.initializing}
}

// Probe 不加载也不重试，未加载不等同于业务实体不存在。
func (r *Runtime) Probe(id Identity) ActivationStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statusLocked(id)
}

// EnsureActivation 发起或复用加载，立即返回状态；已缓存的失败不会再次执行工厂。
func (r *Runtime) EnsureActivation(id Identity) (ActivationStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.ensureLocked(id)
	return r.statusLocked(id), err
}

// RetryActivation 仅清除已结束激活的失败记录；关闭中的旧激活不会被替换。
func (r *Runtime) RetryActivation(id Identity) (ActivationStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, id)
	_, err := r.ensureLocked(id)
	return r.statusLocked(id), err
}

func (r *Runtime) statusLocked(id Identity) ActivationStatus {
	if e := r.entries[id]; e != nil {
		state := Ready
		if e.initializing {
			state = Initializing
		}
		if e.closed {
			state = Closing
		}
		return ActivationStatus{State: state, PendingMessages: len(e.queue)}
	}
	if failure := r.failures[id]; failure != nil {
		copy := *failure
		return ActivationStatus{State: Failed, Failure: &copy}
	}
	return ActivationStatus{State: Unloaded}
}

// CheckActivationCapacity 只为低频选址提供快照，不能替代激活时的原子预留。
func (r *Runtime) CheckActivationCapacity(id Identity) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining {
		return ErrDraining
	}
	if e := r.entries[id]; e != nil {
		if e.closed {
			return ErrDeactivated
		}
		return nil
	}
	return r.checkCapacityLocked()
}

func (r *Runtime) checkCapacityLocked() error {
	l := r.limits
	if (l.Actors > 0 && len(r.entries) >= l.Actors) || (l.Initializations > 0 && r.initializing >= l.Initializations) || (l.Admit != nil && !l.Admit()) {
		return ErrCapacity
	}
	return nil
}

func (r *Runtime) rememberFailureLocked(id Identity, err error) {
	if r.failures == nil {
		r.failures = make(map[Identity]*RemoteError)
	}
	// ponytail: 原型仅留最多 1024 条失败诊断，任意淘汰；持久存在性仍由业务加载准入判断。
	// 淘汰后 Probe 只返回未加载；新的 Ensure/业务投递可重新尝试，不作为永久拒绝登记表。
	if len(r.failures) >= 1024 {
		for old := range r.failures {
			delete(r.failures, old)
			break
		}
	}
	r.failures[id] = RemoteErrorFrom(err)
}
