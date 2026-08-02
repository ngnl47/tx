package tests

import "github.com/ngnl47/tx/change"

func testBackendResolver(backend change.NameBackend, names map[change.FieldID]string) change.FieldNameResolver {
	return testFieldNameResolver{
		change.NameBackendGo: names,
		backend:              names,
	}
}

func testNamedBackendResolver(namesByBackend map[change.NameBackend]map[change.FieldID]string) change.FieldNameResolver {
	return testFieldNameResolver(namesByBackend)
}

func testDeltaValues(values map[string]any) change.DeltaValueResolver {
	return change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		value, ok := values[path.FullPath(testFieldNameResolver{
			change.NameBackendGo:      deltaTestGoNames(),
			change.NameBackendJSON:    deltaTestJSONNames(),
			change.NameBackendBSON:    deltaTestBSONNames(),
			change.NameBackendMsgpack: deltaTestJSONNames(),
		}, change.NameBackendGo).String()]
		return value, ok
	})
}

func deltaTestGoNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		deltaFieldName:  "Name",
		deltaFieldItems: "Items",
		deltaFieldQty:   "Quantity",
	}
}

func deltaTestJSONNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		deltaFieldName:  "name",
		deltaFieldItems: "items",
		deltaFieldQty:   "quantity",
	}
}

func deltaTestBSONNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		deltaFieldName:  "name",
		deltaFieldItems: "items",
		deltaFieldQty:   "quantity",
	}
}
