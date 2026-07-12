package meta

// Overlay is a reserved explicit metadata override hook. It can be applied
// during initialization, before the registry is frozen.
type Overlay interface {
	Apply(TypeSpec) (TypeSpec, error)
}

// OverlayFunc adapts a function to Overlay.
type OverlayFunc func(TypeSpec) (TypeSpec, error)

func (fn OverlayFunc) Apply(spec TypeSpec) (TypeSpec, error) {
	return fn(spec)
}
