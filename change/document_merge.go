package change

import "strings"

// MergeDocuments 尝试把 next 合并到 base，返回合并后的新文档。
//
// 合并只在最终语义可以由单个 Document 无歧义表达时成功。例如同一字段后写覆盖先写、
// 两个嵌套 map 可以递归合并、父字段整体替换可以覆盖已有子字段变更。若先把父字段写成
// 标量，再对它的子路径做 patch，则无法安全压缩为一个文档，返回 ok=false。
func MergeDocuments(base Document, next Document) (merged Document, ok bool) {
	out := cloneDocument(base)
	for path, value := range next.Set {
		if hasUnsetParent(out.Unset, path) {
			return Document{}, false
		}
		value = cloneDocumentValue(value)
		if existing, exists := out.Set[path]; exists {
			mergedValue, canMerge := mergeDocumentValue(existing, value)
			if !canMerge {
				return Document{}, false
			}
			out.Set[path] = mergedValue
			removeCoveredUnsets(out.Unset, path)
			continue
		}
		if parent, tail, exists := nearestSetParent(out.Set, path); exists {
			parentValue, canMerge := setDocumentChild(out.Set[parent], tail, value)
			if !canMerge {
				return Document{}, false
			}
			out.Set[parent] = parentValue
			removeCoveredUnsets(out.Unset, path)
			continue
		}
		removeCoveredSets(out.Set, path)
		removeCoveredUnsets(out.Unset, path)
		out.Set[path] = value
	}
	for path := range next.Unset {
		if parent, tail, exists := nearestSetParent(out.Set, path); exists {
			parentValue, canMerge := unsetDocumentChild(out.Set[parent], tail)
			if !canMerge {
				return Document{}, false
			}
			out.Set[parent] = parentValue
			removeCoveredUnsets(out.Unset, path)
			continue
		}
		removeCoveredSets(out.Set, path)
		removeCoveredUnsets(out.Unset, path)
		out.Unset[path] = struct{}{}
	}
	return out, true
}

// CompactDocuments 顺序压缩一组变更文档。
//
// 相邻文档能安全合并时会合成一个文档；遇到无法安全合并的顺序边界时保留分段，
// 后续文档继续尝试合并到新的分段。返回值中的文档均为独立副本。
func CompactDocuments(docs []Document) []Document {
	out := make([]Document, 0, len(docs))
	for _, doc := range docs {
		if len(out) == 0 {
			out = append(out, cloneDocument(doc))
			continue
		}
		merged, ok := MergeDocuments(out[len(out)-1], doc)
		if !ok {
			out = append(out, cloneDocument(doc))
			continue
		}
		out[len(out)-1] = merged
	}
	return out
}

func cloneDocument(doc Document) Document {
	out := Document{
		Set:   map[string]any{},
		Unset: map[string]struct{}{},
	}
	for path, value := range doc.Set {
		out.Set[path] = cloneDocumentValue(value)
	}
	for path := range doc.Unset {
		out.Unset[path] = struct{}{}
	}
	return out
}

func cloneDocumentValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneDocumentValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneDocumentValue(item)
		}
		return out
	default:
		return value
	}
}

func mergeDocumentValue(base any, next any) (any, bool) {
	baseMap, baseIsMap := base.(map[string]any)
	nextMap, nextIsMap := next.(map[string]any)
	switch {
	case baseIsMap && nextIsMap:
		out := cloneDocumentValue(baseMap).(map[string]any)
		for key, value := range nextMap {
			if existing, exists := out[key]; exists {
				merged, ok := mergeDocumentValue(existing, value)
				if !ok {
					return nil, false
				}
				out[key] = merged
				continue
			}
			out[key] = cloneDocumentValue(value)
		}
		return out, true
	case baseIsMap:
		return cloneDocumentValue(next), true
	case nextIsMap:
		return nil, false
	default:
		return cloneDocumentValue(next), true
	}
}

func setDocumentChild(root any, tail []string, value any) (any, bool) {
	rootMap, ok := root.(map[string]any)
	if !ok {
		return nil, false
	}
	out := cloneDocumentValue(rootMap).(map[string]any)
	current := out
	for _, part := range tail[:len(tail)-1] {
		child, exists := current[part]
		if !exists {
			childMap := map[string]any{}
			current[part] = childMap
			current = childMap
			continue
		}
		childMap, ok := child.(map[string]any)
		if !ok {
			return nil, false
		}
		current[part] = cloneDocumentValue(childMap)
		current = current[part].(map[string]any)
	}
	leaf := tail[len(tail)-1]
	if existing, exists := current[leaf]; exists {
		merged, ok := mergeDocumentValue(existing, value)
		if !ok {
			return nil, false
		}
		current[leaf] = merged
	} else {
		current[leaf] = cloneDocumentValue(value)
	}
	return out, true
}

func unsetDocumentChild(root any, tail []string) (any, bool) {
	rootMap, ok := root.(map[string]any)
	if !ok {
		return nil, false
	}
	out := cloneDocumentValue(rootMap).(map[string]any)
	current := out
	for _, part := range tail[:len(tail)-1] {
		child, exists := current[part]
		if !exists {
			return out, true
		}
		childMap, ok := child.(map[string]any)
		if !ok {
			return nil, false
		}
		current[part] = cloneDocumentValue(childMap)
		current = current[part].(map[string]any)
	}
	delete(current, tail[len(tail)-1])
	return out, true
}

func nearestSetParent(set map[string]any, path string) (parent string, tail []string, ok bool) {
	for candidate := range set {
		if !isParentPath(candidate, path) {
			continue
		}
		if parent == "" || len(candidate) > len(parent) {
			parent = candidate
		}
	}
	if parent == "" {
		return "", nil, false
	}
	return parent, strings.Split(strings.TrimPrefix(path, parent+"."), "."), true
}

func hasUnsetParent(unset map[string]struct{}, path string) bool {
	for candidate := range unset {
		if isParentPath(candidate, path) {
			return true
		}
	}
	return false
}

func removeCoveredSets(set map[string]any, path string) {
	for candidate := range set {
		if candidate == path || isParentPath(path, candidate) {
			delete(set, candidate)
		}
	}
}

func removeCoveredUnsets(unset map[string]struct{}, path string) {
	for candidate := range unset {
		if candidate == path || isParentPath(path, candidate) {
			delete(unset, candidate)
		}
	}
}

func isParentPath(parent string, child string) bool {
	return parent != "" && child != parent && strings.HasPrefix(child, parent+".")
}
