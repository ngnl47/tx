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

	"github.com/ngnl47/gobricks/anno"
	"github.com/ngnl47/gobricks/meta"
)

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

func ScanFiles(files []string, opts Options) (*PackageSet, error) {
	if len(files) == 0 {
		return &PackageSet{Types: map[string]meta.TypeSpec{}}, nil
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
				storageSpec, err := scanMetadata(fset, docGroup, structType)
				if err != nil {
					return nil, fmt.Errorf("astscan: scan metadata for %s: %w", ref.FullName, err)
				}
				storageSpec.Type = meta.Type{
					Ref:    ref,
					Fields: scanFields(fset, structType),
				}
				if err := anno.Validate(storageSpec, storageSpec); hasStorageMetadata(storageSpec) && err != nil {
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

	return &PackageSet{Types: specs}, nil
}

func scanMetadata(fset *token.FileSet, doc *ast.CommentGroup, structType *ast.StructType) (meta.TypeSpec, error) {
	var comments []string
	if doc != nil {
		for _, comment := range doc.List {
			comments = append(comments, comment.Text)
		}
	}
	directives, err := anno.ParseTypeDoc(comments)
	if err != nil {
		return meta.TypeSpec{}, err
	}
	for _, field := range structType.Fields.List {
		fieldTag := ""
		if field.Tag != nil {
			fieldTag = strings.Trim(field.Tag.Value, "`")
		}
		fieldDirectives, err := anno.ParseStructTag(reflect.StructTag(fieldTag), anno.ScopeField)
		if err != nil {
			return meta.TypeSpec{}, err
		}
		if len(fieldDirectives) == 0 {
			continue
		}
		for _, info := range fieldInfosForMetadata(fset, field) {
			for _, directive := range fieldDirectives {
				directive.Field = info.alias
				directives = append(directives, directive)
			}
		}
	}
	return anno.Normalize(directives)
}

func hasStorageMetadata(spec meta.TypeSpec) bool {
	return spec.Storage.Mode != "" || spec.Redis != nil || spec.Mongo != nil || len(spec.Fields) > 0
}

type metadataFieldInfo struct {
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
		return []metadataFieldInfo{{alias: aliasForField(name, tag)}}
	}
	infos := make([]metadataFieldInfo, 0, len(field.Names))
	for _, name := range field.Names {
		alias := aliasForField(name.Name, tag)
		if alias == "-" {
			continue
		}
		infos = append(infos, metadataFieldInfo{alias: alias})
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
				Name:     name,
				Alias:    aliasForField(name, tag),
				Type:     fieldType,
				Exported: ast.IsExported(name),
			})
			continue
		}

		for _, name := range field.Names {
			alias := aliasForField(name.Name, tag)
			if alias == "-" {
				continue
			}
			fields = append(fields, meta.Field{
				Name:     name.Name,
				Alias:    alias,
				Type:     fieldType,
				Exported: name.IsExported(),
			})
		}
	}
	return fields
}

func aliasForField(name, tag string) string {
	structTag := reflect.StructTag(tag)
	for _, key := range []string{"alias", "json"} {
		value := tagName(structTag.Get(key))
		if value != "" {
			return value
		}
		if value == "-" {
			return "-"
		}
	}
	return lowerCamel(name)
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
