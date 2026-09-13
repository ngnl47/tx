package actorhost

import (
	"errors"
	"fmt"
)

// RemoteError 保留业务错误码；框架拒绝仍可用 errors.Is 分类。
type RemoteError struct {
	Code    int32
	Message string
	Kind    string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("actortrial remote (%d): %s", e.Code, e.Message)
}
func (e *RemoteError) ErrorCode() int32 { return e.Code }
func (e *RemoteError) Is(target error) bool {
	switch target {
	case ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity:
		return e.Kind == target.Error()
	}
	return false
}
func RemoteErrorFrom(err error) *RemoteError {
	if previous, ok := err.(*RemoteError); ok {
		copy := *previous
		return &copy
	}
	failure := &RemoteError{Code: -1, Message: err.Error()}
	for _, kind := range []error{ErrActivationDenied, ErrDeactivated, ErrMailboxFull, ErrDraining, ErrPanicked, ErrNotOwner, ErrCapacity} {
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
