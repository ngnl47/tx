package anno

import (
	"fmt"
	"reflect"
	"strings"
)

// ParseTypeTagLine 解析一行内置 type 级注解注释。
//
// 它使用 BuiltinDefinitions，因此接受如下存储相关写法：
//
//	// @storage:`mode:"fusion" delaySaveTime:"600s"`
//	// @redis:"routeType:role,routeBy:roleId,idField:roleId,key:role"
func ParseTypeTagLine(text string) (Directive, error) {
	return ParseTypeTagLineWithDefinitions(text, BuiltinDefinitions())
}

// ParseTypeTagLineWithDefinitions 使用调用方提供的定义白名单解析一行 type 级注解。
//
// 支持的内容形式：
//
//	// @custom:`owner:"roleId" ttl:"600"`     // struct-tag 风格
//	// @custom:"owner:roleId,ttl:600,enabled" // 紧凑旧风格
//
// 解析结果刻意保持非强类型：两个示例都会生成字符串参数，紧凑语法里的裸选项
// 会放入 Directive.Flags。
func ParseTypeTagLineWithDefinitions(text string, defs Definitions) (Directive, error) {
	defs = defs.WithBuiltinsIfEmpty()
	text = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "//"))
	if !strings.HasPrefix(text, "@") {
		return Directive{}, fmt.Errorf("anno: not a directive: %q", text)
	}
	name, rest, ok := strings.Cut(strings.TrimPrefix(text, "@"), ":")
	if !ok {
		return Directive{}, fmt.Errorf("anno: malformed directive: %q", text)
	}
	name = strings.TrimSpace(name)
	if !defs.Has(name, ScopeType) {
		return Directive{}, fmt.Errorf("%w: %s", ErrUnknownTag, name)
	}
	content := strings.TrimSpace(rest)
	if strings.HasPrefix(content, "`") && strings.HasSuffix(content, "`") {
		content = strings.Trim(content, "`")
	} else {
		content = strings.Trim(content, `"`)
	}
	return parseContent(name, ScopeType, content), nil
}

// ParseTypeDoc 从类型注释中解析内置注解指令。
//
// 非指令注释行会被忽略。移除 Go 行注释前缀后，以 "@" 开头的行会被视为指令。
func ParseTypeDoc(comments []string) ([]Directive, error) {
	return ParseTypeDocWithDefinitions(comments, BuiltinDefinitions())
}

// ParseTypeDocWithDefinitions 使用调用方提供的定义解析一组类型注释行中的注解指令。
//
// 该函数只负责解析并校验注解名称；注释属于哪个类型声明由 AST 扫描代码决定。
func ParseTypeDocWithDefinitions(comments []string, defs Definitions) ([]Directive, error) {
	var directives []Directive
	for _, comment := range comments {
		text := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
		if !strings.HasPrefix(text, "@") {
			continue
		}
		directive, err := ParseTypeTagLineWithDefinitions(comment, defs)
		if err != nil {
			return nil, err
		}
		directives = append(directives, directive)
	}
	return directives, nil
}

// ParseStructTag 从 Go struct tag 中解析内置 field 级注解。
//
// 示例：
//
//	`redis:"subKey:items" mongodb:"index,unique"`
func ParseStructTag(tag reflect.StructTag, scope Scope) ([]Directive, error) {
	return ParseStructTagWithDefinitions(tag, scope, BuiltinDefinitions())
}

// ParseStructTagWithDefinitions 使用调用方提供的 field 定义解析 Go struct tag。
//
// 只有 Scope 为 ScopeField 的定义会被处理。返回的指令尚未绑定字段别名；
// AST 扫描会按项目字段别名规则填充 Directive.Field。
func ParseStructTagWithDefinitions(tag reflect.StructTag, scope Scope, defs Definitions) ([]Directive, error) {
	if scope != ScopeField {
		return nil, fmt.Errorf("anno: struct tags only support field scope")
	}
	var directives []Directive
	for _, def := range defs.WithBuiltinsIfEmpty() {
		if def.Scope != ScopeField {
			continue
		}
		content := tag.Get(def.Name)
		if content == "" {
			continue
		}
		directives = append(directives, parseContent(def.Name, ScopeField, content))
	}
	return directives, nil
}

func parseContent(name string, scope Scope, content string) Directive {
	directive := Directive{
		Name:   name,
		Scope:  scope,
		Params: map[string]string{},
		Flags:  map[string]bool{},
	}
	if strings.Contains(content, `:"`) {
		tag := reflect.StructTag(content)
		for _, part := range strings.Fields(content) {
			key, _, ok := strings.Cut(part, ":")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			if value := tag.Get(key); value != "" {
				directive.Params[key] = value
			}
		}
		return directive
	}
	for _, part := range strings.Split(content, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, ":")
		if !ok {
			directive.Flags[part] = true
			continue
		}
		directive.Params[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return directive
}
