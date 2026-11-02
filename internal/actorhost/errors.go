package actorhost

import (
	"errors"
	"fmt"
	"github.com/ngnl47/tx/lifecycle"
)

// ErrNotOwner 是可重新解析路由的拒绝，不是业务永久加载拒绝。
var ErrNotOwner = errors.New("actortrial: not current owner")

// RemoteError 保留业务错误码；框架拒绝仍可用 errors.Is 分类。
type RemoteError struct {
	Code          int32
	Message       string
	Kind          string
	RouteRejected bool `json:",omitempty"`
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("actortrial remote (%d): %s", e.Code, e.Message)
}
func (e *RemoteError) ErrorCode() int32 { return e.Code }
func (e *RemoteError) Is(target error) bool {
	switch target {
	case ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity, lifecycle.ErrNotReady:
		return e.Kind == target.Error()
	}
	return false
}
func RemoteErrorFrom(err error) *RemoteError {
	if previous, ok := err.(*RemoteError); ok {
		copy := *previous
		return &copy
	}
	failure := &RemoteError{Code: -1, Message: err.Error(), RouteRejected: IsRouteRejection(err)}
	for _, kind := range []error{ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity, lifecycle.ErrNotReady} {
		if errors.Is(err, kind) {
			failure.Kind = kind.Error()
			if kind != ErrPanicked {
				failure.Code = -3
			}
			break
		}
	}
	var coded interface{ ErrorCode() int32 }
	if errors.As(err, &coded) {
		failure.Code = coded.ErrorCode()
	}
	return failure
}

// RouteRejection 仅由框架准入或权威加载校验产生，不把业务同名错误解释为路由失效。
type RouteRejection struct{ Err error }

func (e *RouteRejection) Error() string { return e.Err.Error() }
func (e *RouteRejection) Unwrap() error { return e.Err }
func IsRouteRejection(err error) bool {
	if err == nil {
		return false
	}
	var source interface{ routeRejected() bool }
	return errors.As(err, &source) && source.routeRejected()
}
func (*RouteRejection) routeRejected() bool { return true }
func (e *RemoteError) routeRejected() bool  { return e.RouteRejected }

// executionError 保留业务错误链，但下游拒绝不代表当前 Actor 拒绝接纳。
type executionError struct{ error }

func (e *executionError) Unwrap() error     { return e.error }
func (*executionError) routeRejected() bool { return false }
func executionResult(err error) error {
	if IsRouteRejection(err) {
		return &executionError{err}
	}
	return err
}

func admissionError(err error) error {
	if errors.Is(err, ErrDraining) || errors.Is(err, ErrNotOwner) {
		return &RouteRejection{Err: err}
	}
	return err
}
