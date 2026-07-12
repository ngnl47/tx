package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func LoadFile(path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("schema: read %s: %w", path, err)
	}
	doc, err := LoadBytes(data)
	if err != nil {
		return nil, fmt.Errorf("schema: load %s: %w", path, err)
	}
	return doc, nil
}

func LoadBytes(data []byte) (*Document, error) {
	var doc Document
	if err := json.Unmarshal(data, &doc); err == nil {
		normalizeDocument(&doc)
		return &doc, nil
	}

	value, err := parseYAMLSubset(string(data))
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("schema: encode yaml subset: %w", err)
	}
	if err := json.Unmarshal(encoded, &doc); err != nil {
		return nil, fmt.Errorf("schema: decode yaml subset: %w", err)
	}
	normalizeDocument(&doc)
	return &doc, nil
}

func normalizeDocument(doc *Document) {
	if doc.Types == nil {
		doc.Types = make(map[string]TypeSpec)
	}
	for name, spec := range doc.Types {
		if spec.Fields == nil {
			spec.Fields = make(map[string]FieldSpec)
		}
		doc.Types[name] = spec
	}
}

type yamlLine struct {
	indent int
	text   string
}

func parseYAMLSubset(input string) (map[string]interface{}, error) {
	lines := yamlLines(input)
	if len(lines) == 0 {
		return nil, fmt.Errorf("schema: empty schema")
	}
	idx := 0
	value, err := parseYAMLMap(lines, &idx, lines[0].indent)
	if err != nil {
		return nil, err
	}
	if idx != len(lines) {
		return nil, fmt.Errorf("schema: unexpected yaml content %q", lines[idx].text)
	}
	return value, nil
}

func yamlLines(input string) []yamlLine {
	rawLines := strings.Split(input, "\n")
	lines := make([]yamlLine, 0, len(rawLines))
	for _, raw := range rawLines {
		cut := strings.TrimSpace(raw)
		if cut == "" || strings.HasPrefix(cut, "#") {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " "))
		lines = append(lines, yamlLine{indent: indent, text: cut})
	}
	return lines
}

func parseYAMLMap(lines []yamlLine, idx *int, indent int) (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for *idx < len(lines) {
		line := lines[*idx]
		if line.indent < indent {
			break
		}
		if line.indent > indent {
			return nil, fmt.Errorf("schema: unexpected indent near %q", line.text)
		}
		if strings.HasPrefix(line.text, "- ") {
			return nil, fmt.Errorf("schema: unexpected list item %q", line.text)
		}

		key, raw, ok := strings.Cut(line.text, ":")
		if !ok {
			return nil, fmt.Errorf("schema: expected key/value near %q", line.text)
		}
		key = strings.TrimSpace(key)
		raw = strings.TrimSpace(raw)
		(*idx)++
		if raw != "" {
			out[key] = parseYAMLScalar(raw)
			continue
		}
		if *idx >= len(lines) || lines[*idx].indent <= indent {
			out[key] = map[string]interface{}{}
			continue
		}
		if strings.HasPrefix(lines[*idx].text, "- ") {
			list, err := parseYAMLList(lines, idx, lines[*idx].indent)
			if err != nil {
				return nil, err
			}
			out[key] = list
			continue
		}
		child, err := parseYAMLMap(lines, idx, lines[*idx].indent)
		if err != nil {
			return nil, err
		}
		out[key] = child
	}
	return out, nil
}

func parseYAMLList(lines []yamlLine, idx *int, indent int) ([]interface{}, error) {
	var out []interface{}
	for *idx < len(lines) {
		line := lines[*idx]
		if line.indent < indent {
			break
		}
		if line.indent > indent {
			return nil, fmt.Errorf("schema: unexpected list indent near %q", line.text)
		}
		if !strings.HasPrefix(line.text, "- ") {
			break
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line.text, "- "))
		(*idx)++
		if raw == "" {
			if *idx >= len(lines) || lines[*idx].indent <= indent {
				out = append(out, map[string]interface{}{})
				continue
			}
			child, err := parseYAMLMap(lines, idx, lines[*idx].indent)
			if err != nil {
				return nil, err
			}
			out = append(out, child)
			continue
		}
		if key, value, ok := strings.Cut(raw, ":"); ok {
			item := map[string]interface{}{strings.TrimSpace(key): parseYAMLScalar(strings.TrimSpace(value))}
			if *idx < len(lines) && lines[*idx].indent > indent {
				child, err := parseYAMLMap(lines, idx, lines[*idx].indent)
				if err != nil {
					return nil, err
				}
				for childKey, childValue := range child {
					item[childKey] = childValue
				}
			}
			out = append(out, item)
			continue
		}
		out = append(out, parseYAMLScalar(raw))
	}
	return out, nil
}

func parseYAMLScalar(raw string) interface{} {
	raw = strings.Trim(raw, `"'`)
	switch raw {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	if i, err := strconv.Atoi(raw); err == nil {
		return i
	}
	return raw
}

func stableTypeNames(doc *Document) []string {
	names := make([]string, 0, len(doc.Types))
	for name := range doc.Types {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
