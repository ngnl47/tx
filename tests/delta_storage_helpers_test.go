package tests

import "github.com/ngnl47/tx/change"

func testResolverForKind(kind change.FieldNameKind, names map[change.FieldID]string) change.FieldNameResolver {
	return testFieldNameResolver{
		change.FieldNameGo: names,
		kind:               names,
	}
}

func testNamedFieldResolver(namesByKind map[change.FieldNameKind]map[change.FieldID]string) change.FieldNameResolver {
	return testFieldNameResolver(namesByKind)
}

func testDeltaValues(values map[string]any) change.DeltaValueResolver {
	return change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		value, ok := values[path.FullPath(testFieldNameResolver{
			change.FieldNameGo:      deltaTestGoNames(),
			change.FieldNameJSON:    deltaTestJSONNames(),
			change.FieldNameBSON:    deltaTestBSONNames(),
			change.FieldNameMsgpack: deltaTestJSONNames(),
		}, change.FieldNameGo).String()]
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
