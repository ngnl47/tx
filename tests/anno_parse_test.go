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
	if directive.Params["routeBy"] != "roleId" || directive.Params["omitempty"] != "true" {
		t.Fatalf("params=%#v flags=%#v", directive.Params, directive.Flags)
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
			if len(directive.Params) != 0 || len(directive.Flags) != 0 {
				t.Fatalf("params=%#v flags=%#v", directive.Params, directive.Flags)
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
	if directive.Params["value"] != "角色" {
		t.Fatalf("params=%#v", directive.Params)
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
	if fieldDirectives[0].Params["subKey"] != "items" {
		t.Fatalf("field directives = %#v", fieldDirectives)
	}
}

func TestAnnoParseRejectsUnknownTag(t *testing.T) {
	_, err := anno.ParseTypeTagLine(`// @unknown:"x:y"`)
	if err == nil {
		t.Fatal("expected unknown tag error")
	}
}

func TestAnnoParseRejectsLegacyTypeTagSyntax(t *testing.T) {
	_, err := anno.ParseTypeTagLine(`// @redis:"routeType:role,routeBy:roleId"`)
	if err == nil {
		t.Fatal("expected legacy type tag syntax error")
	}
}

func TestAnnoParseRejectsBareTypeTagValue(t *testing.T) {
	_, err := anno.ParseTypeTagLine("// @redis:`routeType:role`")
	if err == nil {
		t.Fatal("expected bare type tag value error")
	}
}
