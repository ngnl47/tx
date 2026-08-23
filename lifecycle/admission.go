package lifecycle

// Admission 向服务入口登记一个请求。请求被接受后，返回的完成回调必须且只能执行一次。
type Admission interface {
	Enter() (func(), error)
}

// AdmissionFunc 将函数适配为 Admission。
type AdmissionFunc func() (func(), error)

func (f AdmissionFunc) Enter() (func(), error) {
	return f()
}
