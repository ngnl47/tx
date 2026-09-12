package actortrial

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
	State         ActivationState
	ReservedBytes int64
	Failure       *RemoteError `json:",omitempty"`
}

// ActivationLimits 的零上限表示不限制。估算包含加载峰值，激活存续期间保留额度。
// Estimate、Admit 是锁内的轻量纯查询，不得加载世界、访问网络或回调 Runtime。
// Admit 可读取外部已有压力采样；预算不是独立 Go 堆，也不能保证进程不会 OOM。
type ActivationLimits struct {
	Bytes           int64
	Initializations int
	Estimate        func(Identity) int64
	Admit           func() bool
}

type ActivationUsage struct {
	ReservedBytes int64
	Initializing  int
}

func (r *Runtime) SetActivationLimits(limits ActivationLimits) {
	if limits.Bytes < 0 || limits.Initializations < 0 || (limits.Bytes > 0 && limits.Estimate == nil) {
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
	return r.usage
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
		return ActivationStatus{State: state, ReservedBytes: e.reservedBytes}
	}
	if failure := r.failures[id]; failure != nil {
		copy := *failure
		return ActivationStatus{State: Failed, Failure: &copy}
	}
	return ActivationStatus{State: Unloaded}
}

func (r *Runtime) reserveLocked(id Identity) (int64, error) {
	bytes, err := r.estimateLocked(id)
	if err != nil {
		return 0, err
	}
	r.usage.ReservedBytes += bytes
	r.usage.Initializing++
	return bytes, nil
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
	_, err := r.estimateLocked(id)
	return err
}

func (r *Runtime) estimateLocked(id Identity) (int64, error) {
	l := r.limits
	if (l.Initializations > 0 && r.usage.Initializing >= l.Initializations) || (l.Admit != nil && !l.Admit()) {
		return 0, ErrCapacity
	}
	var bytes int64
	if l.Bytes > 0 {
		bytes = l.Estimate(id)
		if bytes <= 0 {
			panic("actortrial: non-positive activation estimate")
		}
		if bytes > l.Bytes-r.usage.ReservedBytes {
			return 0, ErrCapacity
		}
	}
	return bytes, nil
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
	r.failures[id] = remoteError(err)
}
