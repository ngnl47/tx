package change

import (
	"strconv"
)

const MaxPathDepth = 4

// KeyKind 描述结构化变更路径节点上的集合 key 类型。
type KeyKind uint8

const (
	KeyNone KeyKind = iota
	KeyInt
	KeyUint
	KeyString
)

// PathKey 是集合 key/index 的强类型值表达，避免在路径热数据里使用 any。
type PathKey struct {
	Kind KeyKind
	Int  int64
	Uint uint64
	Str  string
}

// PathNode 表示一层结构化路径，字段编号和可选集合 key 绑定在同一个节点。
type PathNode struct {
	Field FieldID
	Key   PathKey
}

// Path 是一条定长结构化变更路径。Depth 为 0 表示空/无效路径。
type Path struct {
	Nodes [MaxPathDepth]PathNode
	Depth uint8
}

// Valid 判断路径是否能安全用于解析和存储边界转换。
func (p Path) Valid() bool {
	if p.Depth == 0 || int(p.Depth) > len(p.Nodes) {
		return false
	}
	for i := 0; i < int(p.Depth); i++ {
		if p.Nodes[i].Field == FieldNone || !p.Nodes[i].Key.Valid() {
			return false
		}
	}
	return true
}

// Valid 判断 key 类型是否为运行时支持的强类型 key。
func (k PathKey) Valid() bool {
	switch k.Kind {
	case KeyNone, KeyInt, KeyUint, KeyString:
		return true
	default:
		return false
	}
}

// JoinPath 把子路径追加到父路径后。返回 ok=false 表示超过 MaxPathDepth。
func JoinPath(parent, child Path) (Path, bool) {
	out, complete := JoinPathLimited(parent, child)
	if !complete {
		return Path{}, false
	}
	return out, true
}

// JoinPathLimited 把子路径追加到父路径，并保留不超过 MaxPathDepth 的最近祖先路径。
// complete=false 表示原始路径超过上限；返回路径仍可用于一次安全的整体替换。
func JoinPathLimited(parent, child Path) (out Path, complete bool) {
	depth := int(parent.Depth) + int(child.Depth)
	complete = depth <= MaxPathDepth
	remaining := MaxPathDepth
	if parent.Depth > 0 {
		count := min(int(parent.Depth), remaining)
		copy(out.Nodes[:count], parent.Nodes[:count])
		out.Depth = uint8(count)
		remaining -= count
	}
	if child.Depth > 0 && remaining > 0 {
		count := min(int(child.Depth), remaining)
		copy(out.Nodes[out.Depth:int(out.Depth)+count], child.Nodes[:count])
		out.Depth += uint8(count)
	}
	return out, complete
}

// FullPath 按指定字段名体系把 Path 转换为字符串路径。
func (p Path) FullPath(resolver FieldNameResolver, kind FieldNameKind) DocumentPath {
	if p.Depth == 0 || int(p.Depth) > len(p.Nodes) || resolver == nil {
		return nil
	}
	out := make(DocumentPath, 0, int(p.Depth)*2)
	for i := 0; i < int(p.Depth); i++ {
		node := p.Nodes[i]
		if node.Field == FieldNone {
			return nil
		}
		name := resolver.FieldName(node.Field, kind)
		if pathResolver, ok := resolver.(PathFieldNameResolver); ok {
			name = pathResolver.PathFieldName(p, i, kind)
		}
		if name == "" {
			return nil
		}
		out = append(out, name)
		out = node.Key.appendPath(out)
		if out == nil {
			return nil
		}
	}
	return out
}

// Equal 判断两条结构化路径是否完全相同。
func (p Path) Equal(other Path) bool {
	if p.Depth == 0 || int(p.Depth) > len(p.Nodes) ||
		other.Depth == 0 || int(other.Depth) > len(other.Nodes) ||
		p.Depth != other.Depth {
		return false
	}
	for i := 0; i < int(p.Depth); i++ {
		if !p.Nodes[i].Equal(other.Nodes[i]) {
			return false
		}
	}
	return true
}

// IsPrefixOf 判断当前路径是否为 other 的严格祖先路径。
func (p Path) IsPrefixOf(other Path) bool {
	if !p.Valid() || !other.Valid() || p.Depth > other.Depth {
		return false
	}
	for i := 0; i < int(p.Depth); i++ {
		current := p.Nodes[i]
		next := other.Nodes[i]
		if current.Field != next.Field {
			return false
		}
		if current.Key.Equal(next.Key) {
			continue
		}
		// 方案 A 把字段和集合 key 放在同一个节点中，因此 Items 是
		// Items[1001] 的祖先，尽管两条路径的显式 Depth 都可能是 1。
		return i == int(p.Depth)-1 && current.Key.Kind == KeyNone && next.Key.Kind != KeyNone
	}
	return p.Depth < other.Depth
}

// Equal 判断两个路径节点是否完全相同。
func (n PathNode) Equal(other PathNode) bool {
	return n.Field == other.Field && n.Key.Equal(other.Key)
}

// Equal 判断两个路径 key 是否完全相同。
func (k PathKey) Equal(other PathKey) bool {
	if k.Kind != other.Kind {
		return false
	}
	switch k.Kind {
	case KeyNone:
		return true
	case KeyInt:
		return k.Int == other.Int
	case KeyUint:
		return k.Uint == other.Uint
	case KeyString:
		return k.Str == other.Str
	default:
		return false
	}
}

func (k PathKey) appendPath(path DocumentPath) DocumentPath {
	switch k.Kind {
	case KeyNone:
		return path
	case KeyInt:
		return append(path, strconv.FormatInt(k.Int, 10))
	case KeyUint:
		return append(path, strconv.FormatUint(k.Uint, 10))
	case KeyString:
		return append(path, k.Str)
	default:
		return nil
	}
}

// Op 描述同步遍历中的变更操作。
type Op uint8

const (
	OpSet Op = iota + 1
	// OpReplace 覆盖路径当前值，不保留旧对象中的未出现字段。
	OpReplace
	OpDelete
	OpClear
)

// Valid 判断 op 是否为已定义的变更操作。
func (op Op) Valid() bool {
	switch op {
	case OpSet, OpReplace, OpDelete, OpClear:
		return true
	default:
		return false
	}
}
