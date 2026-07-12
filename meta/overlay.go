package meta

// Overlay 是预留的显式元数据覆盖钩子，只能在初始化阶段、registry 冻结前应用。
type Overlay interface {
	Apply(TypeSpec) (TypeSpec, error)
}

// OverlayFunc 将函数适配为 Overlay。
type OverlayFunc func(TypeSpec) (TypeSpec, error)

func (fn OverlayFunc) Apply(spec TypeSpec) (TypeSpec, error) {
	return fn(spec)
}
