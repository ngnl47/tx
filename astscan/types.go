package astscan

import (
	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/meta"
)

// Options 控制 Go 文件的扫描方式。
type Options struct {
	ImportPath string // 非空时覆盖从 go.mod 推导出的 import path。
	// Annotations 列出本次扫描接受的 type/field 注解。
	// 为空时使用 anno.BuiltinDefinitions()，保留默认存储注解行为。
	Annotations anno.Definitions
}

// PackageSet 是一个包目录或一组文件的 AST 扫描结果。
//
// Types 保存 Go 类型结构，以及存在时的内置存储元数据。TypeDirectives 和
// FieldDirectives 保存所有解析出的注解，包括 storage 不理解的调用方自定义注解。
type PackageSet struct {
	Types           map[string]meta.TypeSpec
	TypeDirectives  map[string][]anno.Directive
	FieldDirectives map[string]map[string][]anno.Directive
}

// Lookup 根据类型引用返回扫描得到的类型元数据。
func (set *PackageSet) Lookup(ref meta.TypeRef) (meta.TypeSpec, bool) {
	if set == nil {
		return meta.TypeSpec{}, false
	}
	spec, ok := set.Types[ref.FullName]
	if !ok {
		return meta.TypeSpec{}, false
	}
	return spec, true
}

// TypeSpecs 返回所有扫描得到的类型规格。
func (set *PackageSet) TypeSpecs() []meta.TypeSpec {
	if set == nil {
		return nil
	}
	specs := make([]meta.TypeSpec, 0, len(set.Types))
	for _, spec := range set.Types {
		specs = append(specs, spec)
	}
	return specs
}

// TypeAnnotations 返回 typeName 绑定的 type 级注解。
//
// typeName 示例："example.com/project/data.Role"。
func (set *PackageSet) TypeAnnotations(typeName string) []anno.Directive {
	if set == nil {
		return nil
	}
	return append([]anno.Directive(nil), set.TypeDirectives[typeName]...)
}

// FieldAnnotations 返回绑定到字段别名的 field 级注解。
//
// 示例：
//
//	set.FieldAnnotations("example.com/project/data.Role", "roleId")
func (set *PackageSet) FieldAnnotations(typeName, fieldAlias string) []anno.Directive {
	if set == nil || set.FieldDirectives[typeName] == nil {
		return nil
	}
	return append([]anno.Directive(nil), set.FieldDirectives[typeName][fieldAlias]...)
}
