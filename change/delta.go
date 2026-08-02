package change

import "strconv"

const MaxDeltaDepth = 4

// KeyKind 描述 Delta 路径节点上的集合 key 类型。
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

// PathNode 表示一层 Delta 路径，字段编号和可选集合 key 绑定在同一个节点。
type PathNode struct {
	Field FieldID
	Key   PathKey
}

// DeltaPath 是一条定长 Delta 路径。Depth 为 0 表示空/无效路径。
type DeltaPath struct {
	Nodes [MaxDeltaDepth]PathNode
	Depth uint8
}

// FullPath 按指定字段名体系把 DeltaPath 转换为字符串路径。
func (p DeltaPath) FullPath(resolver FieldNameResolver, backend NameBackend) Path {
	if p.Depth == 0 || resolver == nil {
		return nil
	}
	out := make(Path, 0, int(p.Depth)*2)
	for i := 0; i < int(p.Depth); i++ {
		node := p.Nodes[i]
		if node.Field == FieldNone {
			return nil
		}
		name := resolver.FieldName(node.Field, backend)
		if name == "" {
			return nil
		}
		out = append(out, name)
		out = node.Key.appendPath(out)
	}
	return out
}

// Equal 判断两条 Delta 路径是否完全相同。
func (p DeltaPath) Equal(other DeltaPath) bool {
	if p.Depth != other.Depth {
		return false
	}
	for i := 0; i < int(p.Depth); i++ {
		if !p.Nodes[i].Equal(other.Nodes[i]) {
			return false
		}
	}
	return true
}

// PrefixOf 判断当前路径是否是另一条路径的严格祖先。
func (p DeltaPath) PrefixOf(other DeltaPath) bool {
	if p.Depth == 0 || p.Depth >= other.Depth {
		return false
	}
	for i := 0; i < int(p.Depth); i++ {
		if !p.Nodes[i].prefixMatches(other.Nodes[i]) {
			return false
		}
	}
	return true
}

// Equal 判断两个路径节点是否完全相同。
func (n PathNode) Equal(other PathNode) bool {
	return n.Field == other.Field && n.Key.Equal(other.Key)
}

func (n PathNode) prefixMatches(other PathNode) bool {
	if n.Field != other.Field {
		return false
	}
	if n.Key.Kind == KeyNone {
		return true
	}
	return n.Key.Equal(other.Key)
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

func (k PathKey) appendPath(path Path) Path {
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
		return path
	}
}

// DeltaKind 描述 DeltaEntry 的变更语义。
type DeltaKind uint8

const (
	DeltaKindSet DeltaKind = iota + 1
	DeltaDelete
	DeltaClear
)

// DeltaEntry 是一条扁平紧凑的字段级变更记录。
type DeltaEntry struct {
	Path  DeltaPath
	Kind  DeltaKind
	Value any
}

// DeltaSet 是按发生顺序追加的一组 DeltaEntry。
type DeltaSet []DeltaEntry

// Clone 返回一份可独立修改的 DeltaSet 切片副本。
func (ds DeltaSet) Clone() DeltaSet {
	if len(ds) == 0 {
		return nil
	}
	out := make(DeltaSet, len(ds))
	copy(out, ds)
	return out
}

// RemoveField 移除指定根字段及其子路径上的 DeltaEntry。
func (ds DeltaSet) RemoveField(field FieldID) DeltaSet {
	if len(ds) == 0 {
		return ds
	}
	write := 0
	for read := 0; read < len(ds); read++ {
		if ds[read].Path.Depth > 0 && ds[read].Path.Nodes[0].Field == field {
			continue
		}
		if write != read {
			ds[write] = ds[read]
		}
		write++
	}
	clear(ds[write:])
	return ds[:write]
}

// Compact 原地压缩确定冗余的 DeltaEntry。
//
// 该方法只做不需要外部状态即可确定正确的压缩：同路径保留后者，后来的
// 祖先路径移除更早的子孙路径。早先的祖先路径不会吞掉后来的子孙路径，
// 因为 Compact 不读取当前对象，无法证明祖先值已包含后续修改。
func (ds DeltaSet) Compact() DeltaSet {
	if len(ds) <= 1 {
		return ds
	}
	for i := len(ds) - 1; i > 0; i-- {
		if ds[i].Path.Depth == 0 {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			if ds[j].Path.Depth == 0 {
				continue
			}
			if ds[i].Path.Equal(ds[j].Path) || ds[i].Path.PrefixOf(ds[j].Path) {
				ds[j].Path.Depth = 0
			}
		}
	}
	write := 0
	for read := 0; read < len(ds); read++ {
		if ds[read].Path.Depth == 0 {
			continue
		}
		if write != read {
			ds[write] = ds[read]
		}
		write++
	}
	clear(ds[write:])
	return ds[:write]
}
