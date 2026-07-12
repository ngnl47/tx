package anno

import (
	"fmt"
	"reflect"
	"strings"
)

func ParseTypeTagLine(text string) (Directive, error) {
	text = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(text), "//"))
	if !strings.HasPrefix(text, "@") {
		return Directive{}, fmt.Errorf("anno: not a directive: %q", text)
	}
	name, rest, ok := strings.Cut(strings.TrimPrefix(text, "@"), ":")
	if !ok {
		return Directive{}, fmt.Errorf("anno: malformed directive: %q", text)
	}
	name = strings.TrimSpace(name)
	if !isTypeTag(name) {
		return Directive{}, fmt.Errorf("%w: %s", ErrUnknownTag, name)
	}
	content := strings.Trim(strings.TrimSpace(rest), `"`)
	return parseContent(name, ScopeType, content), nil
}

func ParseTypeDoc(comments []string) ([]Directive, error) {
	var directives []Directive
	for _, comment := range comments {
		text := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
		if !strings.HasPrefix(text, "@") {
			continue
		}
		directive, err := ParseTypeTagLine(comment)
		if err != nil {
			return nil, err
		}
		directives = append(directives, directive)
	}
	return directives, nil
}

func ParseStructTag(tag reflect.StructTag, scope Scope) ([]Directive, error) {
	if scope != ScopeField {
		return nil, fmt.Errorf("anno: struct tags only support field scope")
	}
	var directives []Directive
	for _, name := range []string{"redis", "mongodb"} {
		content := tag.Get(name)
		if content == "" {
			continue
		}
		directives = append(directives, parseContent(name, ScopeField, content))
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

func isTypeTag(name string) bool {
	switch name {
	case "storage", "redis", "mongodb":
		return true
	default:
		return false
	}
}
