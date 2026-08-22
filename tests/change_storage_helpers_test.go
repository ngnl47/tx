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

func testChangeValues(values map[string]any) change.ValueResolver {
	return change.ValueResolverFunc(func(path change.Path) (any, bool) {
		value, ok := values[path.FullPath(testFieldNameResolver{
			change.FieldNameGo:      changeTestGoNames(),
			change.FieldNameJSON:    changeTestJSONNames(),
			change.FieldNameBSON:    changeTestBSONNames(),
			change.FieldNameMsgpack: changeTestJSONNames(),
		}, change.FieldNameGo).String()]
		return value, ok
	})
}

func changeTestGoNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		changeFieldName:  "Name",
		changeFieldItems: "Items",
		changeFieldQty:   "Quantity",
	}
}

func changeTestJSONNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		changeFieldName:  "name",
		changeFieldItems: "items",
		changeFieldQty:   "quantity",
	}
}

func changeTestBSONNames() map[change.FieldID]string {
	return map[change.FieldID]string{
		changeFieldName:  "name",
		changeFieldItems: "items",
		changeFieldQty:   "quantity",
	}
}
