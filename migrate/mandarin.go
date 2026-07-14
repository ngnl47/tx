package migrate

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
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ngnl47/tx/meta"
)

func MandarinDir(root string) ([]meta.TypeSpec, Report, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, Report{}, fmt.Errorf("migrate: read dir %s: %w", root, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		files = append(files, filepath.Join(root, entry.Name()))
	}
	sort.Strings(files)
	return MandarinFiles(files)
}

func MandarinFiles(files []string) ([]meta.TypeSpec, Report, error) {
	var specs []meta.TypeSpec
	var report Report
	if len(files) == 0 {
		return specs, report, nil
	}

	fset := token.NewFileSet()
	importPath, err := resolveImportPath(files[0])
	if err != nil {
		return nil, report, err
	}
	for _, file := range files {
		astFile, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
		if err != nil {
			return nil, report, fmt.Errorf("migrate: parse %s: %w", file, err)
		}
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
				fullName := importPath + "." + typeSpec.Name.Name
				docGroup := typeSpec.Doc
				if docGroup == nil {
					docGroup = gen.Doc
				}
				spec, migrated := migrateType(fset, fullName, docGroup, structType, &report)
				if migrated {
					specs = append(specs, spec)
					report.MigratedTypes++
				}
			}
		}
	}
	return specs, report, nil
}

func migrateType(fset *token.FileSet, fullName string, docGroup *ast.CommentGroup, structType *ast.StructType, report *Report) (meta.TypeSpec, bool) {
	comments := legacyComments(docGroup)
	if len(comments) == 0 {
		return meta.TypeSpec{}, false
	}
	spec := meta.TypeSpec{
		Type:   meta.Type{Ref: meta.TypeRef{FullName: fullName}},
		Fields: map[string]meta.FieldSpec{},
	}
	fieldAliases := map[string]struct{}{}
	for _, field := range structType.Fields.List {
		for _, info := range fieldInfos(fset, field) {
			fieldAliases[info.alias] = struct{}{}
			spec.Type.Fields = append(spec.Type.Fields, meta.Field{
				Name:  info.name,
				Alias: info.alias,
			})
			fieldSpec, ok := migrateField(info.tag)
			if ok {
				spec.Fields[info.alias] = fieldSpec
			}
		}
	}

	for _, comment := range comments {
		switch comment.name {
		case "storage":
			spec.Storage = migrateStorage(fullName, comment.content, report)
		case "redis":
			redis := migrateRedis(fullName, comment.content, report)
			spec.Redis = &redis
		case "mongodb":
			mongo := migrateMongo(fullName, comment.content, report)
			spec.Mongo = &mongo
		}
	}
	checkFields(fullName, spec, fieldAliases, report)
	return spec, true
}

type legacyComment struct {
	name    string
	content string
}

func legacyComments(group *ast.CommentGroup) []legacyComment {
	if group == nil {
		return nil
	}
	var out []legacyComment
	for _, comment := range group.List {
		text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
		if !strings.HasPrefix(text, "@") {
			continue
		}
		name, rest, ok := strings.Cut(strings.TrimPrefix(text, "@"), ":")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name != "storage" && name != "redis" && name != "mongodb" {
			continue
		}
		out = append(out, legacyComment{name: name, content: strings.Trim(strings.TrimSpace(rest), `"`)})
	}
	return out
}

func migrateStorage(typeName, content string, report *Report) meta.StorageSpec {
	var out meta.StorageSpec
	for _, param := range parseParams(content) {
		switch param.key {
		case "mode":
			if param.value == "redisDelayMongoDB" {
				out.Mode = meta.StorageModeFusion
				report.LegacyAliases = append(report.LegacyAliases, typeName+" storage.mode redisDelayMongoDB")
				continue
			}
			out.Mode = meta.StorageMode(param.value)
		case "delaySaveTime", "saveTime":
			out.DelaySave = meta.Duration{Duration: parseLegacyDuration(param.value)}
			report.LegacyAliases = append(report.LegacyAliases, typeName+" storage."+param.key)
		case "delayDelTime", "delTime":
			out.DelayDelete = meta.Duration{Duration: parseLegacyDuration(param.value)}
			report.LegacyAliases = append(report.LegacyAliases, typeName+" storage."+param.key)
		default:
			report.UnknownParams = append(report.UnknownParams, Diagnostic{Type: typeName, Property: "storage." + param.key})
		}
	}
	return out
}

func migrateRedis(typeName, content string, report *Report) meta.RedisSpec {
	var out meta.RedisSpec
	for _, param := range parseParams(content) {
		switch param.key {
		case "routeType", "shardType", "config":
			out.Route.Type = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" redis."+param.key)
		case "routeBy", "shardBy":
			out.Route.By = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" redis."+param.key)
		case "idField", "id":
			out.IDField = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" redis."+param.key)
		case "key":
			out.Key = param.value
		case "omitempty":
			out.OmitEmpty = true
		case "storeType":
			out.StoreType = param.value
		default:
			report.UnknownParams = append(report.UnknownParams, Diagnostic{Type: typeName, Property: "redis." + param.key})
		}
	}
	return out
}

func migrateMongo(typeName, content string, report *Report) meta.MongoSpec {
	var out meta.MongoSpec
	for _, param := range parseParams(content) {
		switch param.key {
		case "routeType", "shardType", "config":
			out.Route.Type = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" mongo."+param.key)
		case "routeBy", "shardBy":
			out.Route.By = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" mongo."+param.key)
		case "idField", "id":
			out.IDField = param.value
			report.LegacyAliases = append(report.LegacyAliases, typeName+" mongo."+param.key)
		case "collection", "c", "col":
			out.Collection = param.value
			if param.key != "collection" {
				report.LegacyAliases = append(report.LegacyAliases, typeName+" mongo."+param.key)
			}
		case "enableSharding":
			out.Sharding.Enabled = true
		default:
			report.UnknownParams = append(report.UnknownParams, Diagnostic{Type: typeName, Property: "mongo." + param.key})
		}
	}
	return out
}

type param struct {
	key   string
	value string
}

func parseParams(content string) []param {
	parts := strings.Split(content, ",")
	params := make([]param, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, ":")
		if !ok {
			params = append(params, param{key: part})
			continue
		}
		params = append(params, param{key: strings.TrimSpace(key), value: strings.TrimSpace(value)})
	}
	return params
}

func parseLegacyDuration(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Duration(seconds) * time.Second
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return dur
}

type fieldInfo struct {
	name  string
	alias string
	tag   reflect.StructTag
}

func fieldInfos(fset *token.FileSet, field *ast.Field) []fieldInfo {
	tag := reflect.StructTag("")
	if field.Tag != nil {
		tag = reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	}
	if len(field.Names) == 0 {
		name := embeddedFieldName(field.Type)
		if name == "" {
			return nil
		}
		return []fieldInfo{{name: name, alias: aliasForField(name, tag), tag: tag}}
	}
	infos := make([]fieldInfo, 0, len(field.Names))
	for _, name := range field.Names {
		alias := aliasForField(name.Name, tag)
		if alias == "-" {
			continue
		}
		infos = append(infos, fieldInfo{name: name.Name, alias: alias, tag: tag})
	}
	_ = fset
	return infos
}

func migrateField(tag reflect.StructTag) (meta.FieldSpec, bool) {
	var out meta.FieldSpec
	ok := false
	if redisTag := tag.Get("redis"); redisTag != "" {
		field := meta.RedisFieldSpec{}
		for _, param := range parseParams(redisTag) {
			switch param.key {
			case "subKey":
				field.SubKey = param.value
			case "ignore":
				field.Ignore = true
			case "fullUpdate":
				field.FullUpdate = true
			}
		}
		out.Redis = &field
		ok = true
	}
	if mongoTag := tag.Get("mongodb"); mongoTag != "" {
		field := meta.MongoFieldSpec{}
		index := meta.IndexSpec{}
		hasIndex := false
		for _, param := range parseParams(mongoTag) {
			switch param.key {
			case "ignore":
				field.Ignore = true
			case "index":
				hasIndex = true
			case "unique":
				index.Unique = true
				hasIndex = true
			case "order", "indexType":
				index.Order = param.value
				hasIndex = true
			case "weight":
				index.Weight, _ = strconv.Atoi(param.value)
				hasIndex = true
			case "expire", "expireAfter":
				index.ExpireAfter = param.value
				hasIndex = true
			}
		}
		if hasIndex {
			field.Indexes = append(field.Indexes, index)
		}
		out.Mongo = &field
		ok = true
	}
	return out, ok
}

func checkFields(typeName string, spec meta.TypeSpec, aliases map[string]struct{}, report *Report) {
	check := func(property, field string) {
		if field == "" {
			return
		}
		if _, ok := aliases[field]; ok {
			return
		}
		report.MissingFields = append(report.MissingFields, Diagnostic{Type: typeName, Field: field, Property: property})
	}
	if spec.Redis != nil {
		check("redis.id", spec.Redis.IDField)
		check("redis.route.by", spec.Redis.Route.By)
	}
	if spec.Mongo != nil {
		check("mongo.id", spec.Mongo.IDField)
		check("mongo.route.by", spec.Mongo.Route.By)
	}
}

func aliasForField(name string, tag reflect.StructTag) string {
	for _, key := range []string{"alias", "json"} {
		value := tagName(tag.Get(key))
		if value != "" {
			return value
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

func resolveImportPath(file string) (string, error) {
	dir := filepath.Dir(file)
	moduleRoot, modulePath, ok := findModule(dir)
	if !ok {
		return filepath.Base(dir), nil
	}
	rel, err := filepath.Rel(moduleRoot, dir)
	if err != nil {
		return "", fmt.Errorf("migrate: resolve import path for %s: %w", file, err)
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

func exprString(fset *token.FileSet, expr ast.Expr) string {
	var buf bytes.Buffer
	_ = printer.Fprint(&buf, fset, expr)
	return buf.String()
}
