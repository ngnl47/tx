package astscan

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/meta"
)

// ScanDir 扫描 root 目录下的所有非测试 Go 文件。
//
// 扫描器会提取 Go 类型结构、解析字段别名、绑定 type 级注释注解，并绑定
// field 级 struct tag 注解。递归遍历包目录刻意留给调用方处理，这样每个
// PackageSet 都只代表一个包。
func ScanDir(root string, opts Options) (*PackageSet, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("astscan: read dir %s: %w", root, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		files = append(files, filepath.Join(root, entry.Name()))
	}
	sort.Strings(files)
	return ScanFiles(files, opts)
}

// ScanFiles 扫描明确给出的一组 Go 文件；这些文件应属于同一个包。
//
// 注解绑定流程：
//  1. 解析包含注释的 Go 文件。
//  2. 对每个结构体类型，读取类似下面的 doc 行：
//     `// @custom:`owner:"roleId" ttl:"600"`.
//  3. 对每个字段，读取类似 `customField:"name:owner"` 的 struct tag。
//  4. 将 field 指令绑定到解析后的字段别名
//     （优先级：`alias` tag > `json`/`bson`/`msgpack` tag > Go 字段名 lower-camel）。
//  5. 将所有指令保存到 PackageSet，同时把内置 storage 指令转换到
//     meta.TypeSpec，供后续 storage/gen 代码消费。
func ScanFiles(files []string, opts Options) (*PackageSet, error) {
	if len(files) == 0 {
		return &PackageSet{
			Types:           map[string]meta.TypeSpec{},
			TypeDirectives:  map[string][]anno.Directive{},
			FieldDirectives: map[string]map[string][]anno.Directive{},
		}, nil
	}
	defs := opts.Annotations
	if len(defs) == 0 {
		defs = anno.BuiltinDefinitions()
	}

	fset := token.NewFileSet()
	parsed := make([]*ast.File, 0, len(files))
	for _, file := range files {
		astFile, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("astscan: parse %s: %w", file, err)
		}
		parsed = append(parsed, astFile)
	}

	packageName := parsed[0].Name.Name
	importPath, err := resolveImportPath(files[0], packageName, opts)
	if err != nil {
		return nil, err
	}

	specs := make(map[string]meta.TypeSpec)
	typeDirectives := make(map[string][]anno.Directive)
	fieldDirectives := make(map[string]map[string][]anno.Directive)
	for _, astFile := range parsed {
		for _, decl := range astFile.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, rawSpec := range gen.Specs {
				typeSpec, ok := rawSpec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				ref := meta.TypeRef{
					ImportPath: importPath,
					Package:    packageName,
					Name:       typeSpec.Name.Name,
					FullName:   importPath + "." + typeSpec.Name.Name,
				}
				docGroup := typeSpec.Doc
				if docGroup == nil {
					docGroup = gen.Doc
				}
				if err := validateStructTags(structType); err != nil {
					return nil, fmt.Errorf("astscan: scan tags for %s: %w", ref.FullName, err)
				}
				storageSpec, directives, fields, err := scanMetadata(fset, docGroup, structType, defs)
				if err != nil {
					return nil, fmt.Errorf("astscan: scan metadata for %s: %w", ref.FullName, err)
				}
				typeDirectives[ref.FullName] = append(typeDirectives[ref.FullName], directives...)
				if len(fields) > 0 {
					fieldDirectives[ref.FullName] = fields
				}
				storageSpec.Type = meta.Type{
					Ref:    ref,
					Fields: scanFields(fset, structType),
				}
				if err := meta.Validate(storageSpec, storageSpec); hasStorageMetadata(storageSpec) && err != nil {
					return nil, fmt.Errorf("astscan: validate metadata for %s: %w", ref.FullName, err)
				}
				specs[ref.FullName] = storageSpec
			}
		}
	}

	for _, astFile := range parsed {
		for _, decl := range astFile.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			receiver := receiverTypeName(fn.Recv.List[0].Type)
			if receiver == "" {
				continue
			}
			fullName := importPath + "." + receiver
			spec, ok := specs[fullName]
			if !ok {
				continue
			}
			spec.Type.Methods = append(spec.Type.Methods, meta.Method{
				Name:     fn.Name.Name,
				Exported: fn.Name.IsExported(),
			})
			specs[fullName] = spec
		}
	}

	return &PackageSet{
		Types:           specs,
		TypeDirectives:  typeDirectives,
		FieldDirectives: fieldDirectives,
	}, nil
}

func scanMetadata(fset *token.FileSet, doc *ast.CommentGroup, structType *ast.StructType, defs anno.Definitions) (meta.TypeSpec, []anno.Directive, map[string][]anno.Directive, error) {
	var comments []string
	if doc != nil {
		for _, comment := range doc.List {
			comments = append(comments, comment.Text)
		}
	}
	directives, err := anno.ParseTypeDocWithDefinitions(comments, defs)
	if err != nil {
		return meta.TypeSpec{}, nil, nil, err
	}
	fieldDirectiveMap := map[string][]anno.Directive{}
	storageDirectives := filterStorageDirectives(directives)
	for _, field := range structType.Fields.List {
		fieldTag := ""
		if field.Tag != nil {
			fieldTag = strings.Trim(field.Tag.Value, "`")
		}
		fieldDirectives, err := anno.ParseStructTagWithDefinitions(reflect.StructTag(fieldTag), anno.ScopeField, defs)
		if err != nil {
			return meta.TypeSpec{}, nil, nil, err
		}
		if len(fieldDirectives) == 0 {
			continue
		}
		for _, info := range fieldInfosForMetadata(fset, field) {
			for _, directive := range fieldDirectives {
				nameDirective := directive
				nameDirective.Field = info.name
				fieldDirectiveMap[info.name] = append(fieldDirectiveMap[info.name], nameDirective)
				if info.alias != "" && info.alias != info.name {
					aliasDirective := directive
					aliasDirective.Field = info.alias
					fieldDirectiveMap[info.alias] = append(fieldDirectiveMap[info.alias], aliasDirective)
				}
				if isStorageDirective(nameDirective) {
					storageDirectives = append(storageDirectives, nameDirective)
				}
			}
		}
	}
	spec, err := meta.Normalize(storageDirectives)
	if err != nil {
		return meta.TypeSpec{}, nil, nil, err
	}
	return spec, directives, fieldDirectiveMap, nil
}

func filterStorageDirectives(directives []anno.Directive) []anno.Directive {
	var out []anno.Directive
	for _, directive := range directives {
		if isStorageDirective(directive) {
			out = append(out, directive)
		}
	}
	return out
}

func isStorageDirective(directive anno.Directive) bool {
	switch directive.Name {
	case "storage", "redis", "mongodb":
		return true
	default:
		return false
	}
}

func hasStorageMetadata(spec meta.TypeSpec) bool {
	return spec.Storage.Mode != "" || spec.Redis != nil || spec.Mongo != nil
}

func validateStructTags(structType *ast.StructType) error {
	for _, field := range structType.Fields.List {
		tag := ""
		if field.Tag != nil {
			tag = strings.Trim(field.Tag.Value, "`")
		}
		if err := validateIgnoredJSONTag(reflect.StructTag(tag)); err != nil {
			return err
		}
	}
	return nil
}

func validateIgnoredJSONTag(tag reflect.StructTag) error {
	name, rest, hasOptions := strings.Cut(tag.Get("json"), ",")
	if name != "-" || !hasOptions {
		return nil
	}
	if rest == "" {
		return fmt.Errorf("json:\"-\" must not declare options")
	}
	return fmt.Errorf("json:\"-\" must not declare options: %s", rest)
}

type metadataFieldInfo struct {
	name  string
	alias string
}

func fieldInfosForMetadata(fset *token.FileSet, field *ast.Field) []metadataFieldInfo {
	_ = fset
	tag := ""
	if field.Tag != nil {
		tag = strings.Trim(field.Tag.Value, "`")
	}
	if len(field.Names) == 0 {
		name := embeddedFieldName(field.Type)
		if name == "" {
			return nil
		}
		return []metadataFieldInfo{{name: name, alias: aliasForField(name, tag)}}
	}
	infos := make([]metadataFieldInfo, 0, len(field.Names))
	for _, name := range field.Names {
		if fieldIgnored(tag) {
			continue
		}
		infos = append(infos, metadataFieldInfo{name: name.Name, alias: aliasForField(name.Name, tag)})
	}
	return infos
}

func scanFields(fset *token.FileSet, structType *ast.StructType) []meta.Field {
	fields := make([]meta.Field, 0, len(structType.Fields.List))
	for _, field := range structType.Fields.List {
		fieldType := exprString(fset, field.Type)
		tag := ""
		if field.Tag != nil {
			tag = strings.Trim(field.Tag.Value, "`")
		}

		if len(field.Names) == 0 {
			name := embeddedFieldName(field.Type)
			if name == "" {
				continue
			}
			fields = append(fields, meta.Field{
				Name:        name,
				Alias:       aliasForField(name, tag),
				JSONName:    jsonNameForField(name, tag),
				BSONName:    bsonNameForField(name, tag),
				MsgpackName: msgpackNameForField(name, tag),
				Type:        fieldType,
				Exported:    ast.IsExported(name),
				Embedded:    true,
			})
			continue
		}

		for _, name := range field.Names {
			if fieldIgnored(tag) {
				continue
			}
			fields = append(fields, meta.Field{
				Name:        name.Name,
				Alias:       aliasForField(name.Name, tag),
				JSONName:    jsonNameForField(name.Name, tag),
				BSONName:    bsonNameForField(name.Name, tag),
				MsgpackName: msgpackNameForField(name.Name, tag),
				Type:        fieldType,
				Exported:    name.IsExported(),
			})
		}
	}
	return fields
}

func aliasForField(name, tag string) string {
	return defaultBackendName(name, tag)
}

func defaultBackendName(name, tag string) string {
	structTag := reflect.StructTag(tag)
	if value := tagName(structTag.Get("alias")); value != "" {
		return value
	}
	return lowerCamel(name)
}

func jsonNameForField(name, tag string) string {
	structTag := reflect.StructTag(tag)
	if value := tagName(structTag.Get("json")); value != "" {
		return value
	}
	return defaultBackendName(name, tag)
}

func bsonNameForField(name, tag string) string {
	structTag := reflect.StructTag(tag)
	if value := tagName(structTag.Get("bson")); value != "" {
		return value
	}
	return defaultBackendName(name, tag)
}

func msgpackNameForField(name, tag string) string {
	structTag := reflect.StructTag(tag)
	if value := tagName(structTag.Get("msgpack")); value != "" {
		return value
	}
	return defaultBackendName(name, tag)
}

func fieldIgnored(tag string) bool {
	structTag := reflect.StructTag(tag)
	for _, key := range []string{"alias", "json", "bson", "msgpack"} {
		if tagName(structTag.Get(key)) == "-" {
			return true
		}
	}
	return false
}

func tagName(value string) string {
	if value == "" {
		return ""
	}
	name, _, _ := strings.Cut(value, ",")
	return name
}

func lowerCamel(name string) string {
	if name == "" {
		return ""
	}
	r, size := utf8.DecodeRuneInString(name)
	if r == utf8.RuneError && size == 0 {
		return name
	}
	return string(unicode.ToLower(r)) + name[size:]
}

func exprString(fset *token.FileSet, expr ast.Expr) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, expr); err != nil {
		return ""
	}
	return buf.String()
}

func embeddedFieldName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return embeddedFieldName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	default:
		return ""
	}
}

func receiverTypeName(expr ast.Expr) string {
	return embeddedFieldName(expr)
}

func resolveImportPath(file, packageName string, opts Options) (string, error) {
	if opts.ImportPath != "" {
		return opts.ImportPath, nil
	}
	dir := filepath.Dir(file)
	moduleRoot, modulePath, ok := findModule(dir)
	if !ok {
		return packageName, nil
	}
	rel, err := filepath.Rel(moduleRoot, dir)
	if err != nil {
		return "", fmt.Errorf("astscan: resolve import path for %s: %w", file, err)
	}
	if rel == "." {
		return modulePath, nil
	}
	return modulePath + "/" + filepath.ToSlash(rel), nil
}

func findModule(dir string) (root, modulePath string, ok bool) {
	for {
		modPath := filepath.Join(dir, "go.mod")
		data, err := os.ReadFile(modPath)
		if err == nil {
			modulePath = parseModulePath(string(data))
			if modulePath != "" {
				return dir, modulePath, true
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", "", false
		}
		dir = parent
	}
}

func parseModulePath(goMod string) string {
	for _, line := range strings.Split(goMod, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "module" {
			return fields[1]
		}
	}
	return ""
}
