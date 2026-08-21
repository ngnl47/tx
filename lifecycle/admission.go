package lifecycle

// Admission registers one request with a service ingress. The returned
// completion callback must run exactly once after an accepted request finishes.
type Admission interface {
	Enter() (func(), error)
}

// AdmissionFunc adapts a function into Admission.
type AdmissionFunc func() (func(), error)

func (f AdmissionFunc) Enter() (func(), error) {
	return f()
}
