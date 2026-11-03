package msgq

import (
	"context"
	"github.com/ngnl47/tx/modules/discovery"
)

// TerminateLogicalSession 仅查询既有角色归属；离线退出不分配 MsgQ，不建立邮箱。
func (r *SessionClientRouter) TerminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	if roleID <= 0 || sessionID <= 0 {
		return ErrInvalidSession
	}
	binding, found, err := r.store.FindRole(ctx, roleID)
	if err != nil {
		return err
	}
	if !found {
		store, ok := r.store.(AdmissionStore)
		if !ok {
			return ErrAdmissionStoreNil
		}
		for {
			old, err := store.LoadAdmission(ctx, roleID)
			if err != nil {
				return err
			}
			if old.SessionID != sessionID {
				return nil
			}
			if old.Attached {
				return ErrRouteUnavailable
			}
			next := old
			next.SessionID = 0
			next.Ready = false
			next.Header = SessionHeader{}
			changed, err := store.CompareAdmission(ctx, roleID, old, next)
			if err != nil {
				return err
			}
			if changed {
				return nil
			}
			if err = ctx.Err(); err != nil {
				return err
			}
		}
	}
	instance, available := r.directory.LookupInstance(binding.InstanceID)
	if !available || instance.Status != discovery.StatusRunning || !providesService(instance, r.serviceName) {
		return ErrRouteUnavailable
	}
	client, err := r.connection(ctx, instance)
	if err != nil {
		return err
	}
	control, ok := client.(SessionControlClient)
	if !ok {
		return ErrAdmissionStoreNil
	}
	return control.TerminateLogicalSession(ctx, roleID, sessionID)
}

// InspectSession 只读权威许可，不因查询建立角色归属。
func (r *SessionClientRouter) InspectSession(ctx context.Context, roleID int64) (AdmissionRecord, error) {
	if roleID <= 0 {
		return AdmissionRecord{}, ErrInvalidSession
	}
	store, ok := r.store.(AdmissionStore)
	if !ok {
		return AdmissionRecord{}, ErrAdmissionStoreNil
	}
	return store.LoadAdmission(ctx, roleID)
}
func (r *FixedSessionClientRouter) TerminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	control, ok := r.client.(SessionControlClient)
	if !ok {
		return ErrAdmissionStoreNil
	}
	return control.TerminateLogicalSession(ctx, roleID, sessionID)
}
func (r *FixedSessionClientRouter) InspectSession(ctx context.Context, roleID int64) (AdmissionRecord, error) {
	control, ok := r.client.(SessionControlClient)
	if !ok {
		return AdmissionRecord{}, ErrAdmissionStoreNil
	}
	return control.InspectSession(ctx, roleID)
}
