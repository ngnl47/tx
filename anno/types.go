package anno

type Scope string

const (
	ScopeType  Scope = "type"
	ScopeField Scope = "field"
)

type Directive struct {
	Name   string
	Scope  Scope
	Params map[string]string
	Flags  map[string]bool
	Field  string
}
