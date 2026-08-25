package anno

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ParseTypeTagLine 解析一行内置 type 级注解注释。
//
// 它使用 BuiltinDefinitions，因此接受如下存储相关写法：
//
//	// @storage:`mode:"fusion" delaySaveTime:"600s"`
//	// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"role"`
func ParseTypeTagLine(text string) (Directive, error) {
	return ParseTypeTagLineWithDefinitions(text, BuiltinDefinitions())
}

// ParseTypeTagLineWithDefinitions 使用调用方提供的定义白名单解析一行 type 级注解。
//
// 支持的内容形式：
//
//	// @custom
//	// @custom:``
//	// @custom:`"single-value"`
//	// @custom:`owner:"roleId" ttl:"600"`
//
// 解析结果刻意保持非强类型。quoted single value 会写入名为 value 的参数；
// struct tag 中的所有字段值都会生成有序字符串参数。
func ParseTypeTagLineWithDefinitions(text string, defs Definitions) (Directive, error) {
	text = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "//"))
	if !strings.HasPrefix(text, "@") {
		return Directive{}, fmt.Errorf("anno: not a directive: %q", text)
	}
	name, rest, ok := strings.Cut(strings.TrimPrefix(text, "@"), ":")
	name = strings.TrimSpace(name)
	if !defs.Has(name, ScopeType) {
		return Directive{}, fmt.Errorf("%w: %s", ErrUnknownTag, name)
	}
	if !ok {
		return parseContent(name, ScopeType, "")
	}
	content := strings.TrimSpace(rest)
	if strings.HasPrefix(content, "`") && strings.HasSuffix(content, "`") {
		content = strings.Trim(content, "`")
	} else {
		return Directive{}, fmt.Errorf("anno: type directive %s must use struct tag syntax", name)
	}
	if content != "" && !strings.Contains(content, `:"`) {
		value, err := strconv.Unquote(content)
		if err != nil {
			return Directive{}, fmt.Errorf("anno: type directive %s must use quoted single value or struct tag fields", name)
		}
		directive, err := parseContent(name, ScopeType, "")
		if err != nil {
			return Directive{}, err
		}
		directive.Arguments = append(directive.Arguments, Argument{Name: "value", Value: value, HasValue: true})
		return directive, nil
	}
	return parseContent(name, ScopeType, content)
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
	for _, def := range defs {
		if def.Scope != ScopeField {
			continue
		}
		content := tag.Get(def.Name)
		if content == "" {
			continue
		}
		directive, err := parseContent(def.Name, ScopeField, content)
		if err != nil {
			return nil, err
		}
		directives = append(directives, directive)
	}
	return directives, nil
}

func parseContent(name string, scope Scope, content string) (Directive, error) {
	directive := Directive{
		Name:  name,
		Scope: scope,
	}
	if strings.Contains(content, `:"`) {
		arguments, err := parseStructTagArguments(content)
		if err != nil {
			return Directive{}, fmt.Errorf("anno: parse %s parameters: %w", name, err)
		}
		directive.Arguments = arguments
		return directive, nil
	}
	for _, part := range strings.Split(content, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, ":")
		if !ok {
			directive.Arguments = append(directive.Arguments, Argument{Name: part})
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return Directive{}, fmt.Errorf("anno: %s parameter name is empty", name)
		}
		directive.Arguments = append(directive.Arguments, Argument{
			Name:     key,
			Value:    strings.TrimSpace(value),
			HasValue: true,
		})
	}
	return directive, nil
}

func parseStructTagArguments(content string) ([]Argument, error) {
	var arguments []Argument
	for content != "" {
		content = strings.TrimLeft(content, " \t\r\n")
		if content == "" {
			break
		}
		colon := strings.IndexByte(content, ':')
		if colon <= 0 || colon+1 >= len(content) || content[colon+1] != '"' {
			return nil, fmt.Errorf("invalid struct tag fragment %q", content)
		}
		name := content[:colon]
		if strings.ContainsAny(name, " \t\r\n\"") {
			return nil, fmt.Errorf("invalid struct tag name %q", name)
		}
		quoted := content[colon+1:]
		end := quotedStringEnd(quoted)
		if end == 0 {
			return nil, fmt.Errorf("unterminated value for %q", name)
		}
		value, err := strconv.Unquote(quoted[:end])
		if err != nil {
			return nil, fmt.Errorf("invalid value for %q: %w", name, err)
		}
		arguments = append(arguments, Argument{Name: name, Value: value, HasValue: true})
		content = quoted[end:]
	}
	return arguments, nil
}

func quotedStringEnd(value string) int {
	if len(value) < 2 || value[0] != '"' {
		return 0
	}
	escaped := false
	for i := 1; i < len(value); i++ {
		switch {
		case escaped:
			escaped = false
		case value[i] == '\\':
			escaped = true
		case value[i] == '"':
			return i + 1
		}
	}
	return 0
}
