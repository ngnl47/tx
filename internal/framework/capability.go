package framework

// CapabilityError 标识公开能力视图的声明或使用阶段错误，不用于业务运行错误。
type CapabilityError struct{ Err error }

func (e CapabilityError) Error() string { return e.Err.Error() }
func (e CapabilityError) Unwrap() error { return e.Err }

// RecoverCapability 仅在装配回调边界恢复能力错误，交给现有启动回滚；普通 panic 保持原样。
func RecoverCapability(err *error) {
	if value := recover(); value != nil {
		if failure, ok := value.(CapabilityError); ok {
			*err = failure
			return
		}
		panic(value)
	}
}
