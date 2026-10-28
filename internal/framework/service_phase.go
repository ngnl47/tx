package framework

import (
	"sync/atomic"

	"github.com/ngnl47/tx/lifecycle"
)

type servicePhase struct {
	status atomic.Pointer[servicePhaseSnapshot]
}

// opened 只区分正常排水与启动失败清理，不能让失败启动因进入 Draining 而开放消息。
type servicePhaseSnapshot struct {
	lifecycle.ServiceStatus
	opened bool
}

func (p *servicePhase) snapshot() lifecycle.ServiceStatus {
	if status := p.status.Load(); status != nil {
		return status.ServiceStatus
	}
	return lifecycle.ServiceStatus{State: lifecycle.ServiceCreated}
}

func (p *servicePhase) set(state lifecycle.ServiceState, err error) {
	for {
		previous := p.status.Load()
		next := servicePhaseSnapshot{ServiceStatus: lifecycle.ServiceStatus{State: state, Failure: err}, opened: state == lifecycle.ServiceRunning}
		if previous != nil {
			next.opened = next.opened || previous.opened
		}
		if err == nil && previous != nil {
			next.Failure = previous.Failure
		}
		if p.status.CompareAndSwap(previous, &next) {
			return
		}
	}
}

// ServiceStatus 返回选中服务的生命周期快照；停止后仍可检查阶段及失败原因。
func (server *Server) ServiceStatus(name string) (lifecycle.ServiceStatus, bool) {
	server.mu.Lock()
	phase, ok := server.servicePhases[name]
	server.mu.Unlock()
	if !ok {
		return lifecycle.ServiceStatus{}, false
	}
	return phase.snapshot(), true
}

func (r *runtime) actorAdmission(actorType string, activation bool) error {
	phase := r.servicePhases[r.actorCatalog[actorType].service]
	if phase == nil {
		return lifecycle.ErrNotReady
	}
	snapshot := phase.status.Load()
	if snapshot == nil {
		return lifecycle.ErrNotReady
	}
	state := snapshot.State
	// 排水仍由已有 Actor/任务准入屏障管理，已领取任务必须能完成投递。
	if state == lifecycle.ServiceRunning || state == lifecycle.ServiceDraining && snapshot.opened {
		return nil
	}
	if activation && state == lifecycle.ServiceStarting {
		return nil
	}
	return lifecycle.ErrNotReady
}
