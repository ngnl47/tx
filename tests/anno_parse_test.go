package tests

import (
	"reflect"
	"testing"

	"github.com/ngnl47/tx/anno"
)

func TestAnnoParseTypeTagLine(t *testing.T) {
	directive, err := anno.ParseTypeTagLine("// @redis:`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" key:\"role\" omitempty:\"true\"`")
	if err != nil {
		t.Fatalf("ParseTypeTagLine returned error: %v", err)
	}
	if directive.Name != "redis" || directive.Scope != anno.ScopeType {
		t.Fatalf("directive = %#v", directive)
	}
	routeBy, _ := directive.Param("routeBy")
	omitEmpty, _ := directive.Param("omitempty")
	if routeBy != "roleId" || omitEmpty != "true" {
		t.Fatalf("arguments=%#v", directive.Arguments)
	}
}

func TestAnnoParseTypeMarkerTagLine(t *testing.T) {
	for _, line := range []string{
		"// @storage",
		"// @storage:``",
	} {
		t.Run(line, func(t *testing.T) {
			directive, err := anno.ParseTypeTagLine(line)
			if err != nil {
				t.Fatalf("ParseTypeTagLine returned error: %v", err)
			}
			if directive.Name != "storage" || directive.Scope != anno.ScopeType {
				t.Fatalf("directive = %#v", directive)
			}
			if len(directive.Arguments) != 0 {
				t.Fatalf("arguments=%#v", directive.Arguments)
			}
		})
	}
}

func TestAnnoParseTypeSingleValueTagLine(t *testing.T) {
	type customTag struct{}

	directive, err := anno.ParseTypeTagLineWithDefinitions(
		"// @custom:`\"角色\"`",
		anno.Definitions{anno.TypeTag[customTag]("custom")},
	)
	if err != nil {
		t.Fatalf("ParseTypeTagLineWithDefinitions returned error: %v", err)
	}
	if directive.Name != "custom" || directive.Scope != anno.ScopeType {
		t.Fatalf("directive = %#v", directive)
	}
	value, _ := directive.Param("value")
	if value != "角色" {
		t.Fatalf("arguments=%#v", directive.Arguments)
	}
}

func TestAnnoParseFieldTagDoesNotUseTypeSingleValueRule(t *testing.T) {
	type customFieldTag struct{}

	directives, err := anno.ParseStructTagWithDefinitions(
		reflect.StructTag(`custom:"quoted"`),
		anno.ScopeField,
		anno.Definitions{anno.FieldTag[customFieldTag]("custom")},
	)
	if err != nil {
		t.Fatalf("ParseStructTagWithDefinitions returned error: %v", err)
	}
	if len(directives) != 1 {
		t.Fatalf("directives=%#v", directives)
	}
	if _, ok := directives[0].Param("value"); ok {
		t.Fatalf("field tag unexpectedly mapped single value to value argument: %#v", directives[0])
	}
	if !directives[0].HasFlag("quoted") {
		t.Fatalf("field tag should keep compact flag semantics: %#v", directives[0])
	}
}

func TestAnnoParseTypeDocAndStructTag(t *testing.T) {
	typeDirectives, err := anno.ParseTypeDoc([]string{
		"// @storage",
		"// @storage:`mode:\"fusion\" delaySaveTime:\"600s\" delayDelTime:\"24h\"`",
		"// @mongodb:`routeType:\"role\" routeBy:\"roleId\" idField:\"roleId\" collection:\"roles\" enableSharding:\"true\"`",
	})
	if err != nil {
		t.Fatalf("ParseTypeDoc returned error: %v", err)
	}
	fieldDirectives, err := anno.ParseStructTag(reflect.StructTag(`redis:"subKey:items" mongodb:"index,unique"`), anno.ScopeField)
	if err != nil {
		t.Fatalf("ParseStructTag returned error: %v", err)
	}
	if len(typeDirectives) != 3 || len(fieldDirectives) != 2 {
		t.Fatalf("type=%#v field=%#v", typeDirectives, fieldDirectives)
	}
	subKey, _ := fieldDirectives[0].Param("subKey")
	if subKey != "items" {
		t.Fatalf("field directives = %#v", fieldDirectives)
	}
}

func TestAnnoParseRejectsUnknownTag(t *testing.T) {
	_, err := anno.ParseTypeTagLine(`// @unknown:"x:y"`)
	if err == nil {
		t.Fatal("expected unknown tag error")
	}
}

func TestAnnoParseWithEmptyDefinitionsDoesNotEnableBuiltins(t *testing.T) {
	_, err := anno.ParseTypeTagLineWithDefinitions(`// @storage`, nil)
	if err == nil {
		t.Fatal("ParseTypeTagLineWithDefinitions returned nil error for an empty annotation whitelist")
	}

	directives, err := anno.ParseStructTagWithDefinitions(
		reflect.StructTag(`redis:"subKey:items"`),
		anno.ScopeField,
		nil,
	)
	if err != nil {
		t.Fatalf("ParseStructTagWithDefinitions returned error: %v", err)
	}
	if len(directives) != 0 {
		t.Fatalf("directives = %#v, want none for an empty annotation whitelist", directives)
	}
}

func TestAnnoParseRejectsLegacyTypeTagSyntax(t *testing.T) {
	_, err := anno.ParseTypeTagLine(`// @redis:"routeType:role,routeBy:roleId"`)
	if err == nil {
		t.Fatal("expected removed type tag syntax error")
	}
}

func TestAnnoParseRejectsBareTypeTagValue(t *testing.T) {
	_, err := anno.ParseTypeTagLine("// @redis:`routeType:role`")
	if err == nil {
		t.Fatal("expected bare type tag value error")
	}
}
