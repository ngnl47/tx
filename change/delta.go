package change

import (
	"fmt"
	"strconv"
)

const MaxDeltaDepth = 4

const compactIndexedThreshold = 128

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

// JoinDeltaPath 把子路径追加到父路径后。返回 ok=false 表示超过 MaxDeltaDepth。
func JoinDeltaPath(parent, child DeltaPath) (DeltaPath, bool) {
	out, complete := JoinDeltaPathLimited(parent, child)
	if !complete {
		return DeltaPath{}, false
	}
	return out, true
}

// JoinDeltaPathLimited 把子路径追加到父路径，并保留不超过 MaxDeltaDepth 的最近祖先路径。
// complete=false 表示原始路径超过上限；返回路径仍可用于一次安全的整体替换。
func JoinDeltaPathLimited(parent, child DeltaPath) (out DeltaPath, complete bool) {
	depth := int(parent.Depth) + int(child.Depth)
	complete = depth <= MaxDeltaDepth
	remaining := MaxDeltaDepth
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
		if pathResolver, ok := resolver.(DeltaPathFieldNameResolver); ok {
			name = pathResolver.DeltaPathFieldName(p, i, backend)
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
		return nil
	}
}

// DeltaKind 描述 DeltaEntry 的变更语义。
type DeltaKind uint8

const (
	DeltaKindSet DeltaKind = iota + 1
	// DeltaKindReplace 覆盖路径当前值，不保留旧对象中的未出现字段。
	DeltaKindReplace
	DeltaDelete
	DeltaClear
)

// Valid 判断 kind 是否为已定义的 Delta 变更语义。
func (kind DeltaKind) Valid() bool {
	switch kind {
	case DeltaKindSet, DeltaKindReplace, DeltaDelete, DeltaClear:
		return true
	default:
		return false
	}
}

// DeltaEntry 是一条扁平紧凑的字段级变更记录。
type DeltaEntry struct {
	Path DeltaPath
	Kind DeltaKind
}

// DeltaSet 是按发生顺序追加的一组 DeltaEntry。
type DeltaSet []DeltaEntry

// ValidateKinds 校验非空路径条目的变更语义。
//
// 必须在 Compact 或存储边界归一化前调用，避免未知 kind 被同路径覆盖或降级逻辑掩盖。
func (ds DeltaSet) ValidateKinds() error {
	for index, delta := range ds {
		if delta.Path.Depth == 0 || delta.Kind.Valid() {
			continue
		}
		return fmt.Errorf("change: unknown delta kind %d at index %d", delta.Kind, index)
	}
	return nil
}

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
	if len(ds) <= compactIndexedThreshold || ds.hasLaterRootAncestor() {
		return ds.compactQuadratic()
	}
	return ds.compactIndexed()
}

func (ds DeltaSet) compactQuadratic() DeltaSet {
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
	return compactDeltaEntries(ds)
}

// compactIndexed 对较大的 DeltaSet 建立后发生路径索引。路径最大深度固定为 4，
// 因此每条旧路径只需查询自身和不超过 15 个可能祖先，不再两两比较全部条目。
func (ds DeltaSet) compactIndexed() DeltaSet {
	laterPaths := make(map[uint64]int, len(ds))
	next := make([]int, len(ds))
	for i := len(ds) - 1; i >= 0; i-- {
		path := ds[i].Path
		if path.Depth == 0 {
			continue
		}
		if compactPathCoveredByLater(path, ds, laterPaths, next) {
			ds[i].Path.Depth = 0
			continue
		}
		hash := compactPathHash(path)
		next[i] = laterPaths[hash]
		laterPaths[hash] = i + 1
	}
	return compactDeltaEntries(ds)
}

func (ds DeltaSet) hasLaterRootAncestor() bool {
	for i := 1; i < len(ds); i++ {
		path := ds[i].Path
		if path.Depth != 1 || path.Nodes[0].Key.Kind != KeyNone {
			continue
		}
		for j := 0; j < i; j++ {
			if path.PrefixOf(ds[j].Path) {
				return true
			}
		}
	}
	return false
}

func compactPathCoveredByLater(path DeltaPath, deltas DeltaSet, laterPaths map[uint64]int, next []int) bool {
	depth := int(path.Depth)
	if compactPathExists(path, deltas, laterPaths, next) {
		return true
	}
	for prefixDepth := 1; prefixDepth < depth; prefixDepth++ {
		for wildcardMask := 0; wildcardMask < 1<<prefixDepth; wildcardMask++ {
			candidate := compactPathPrefix(path, prefixDepth, wildcardMask)
			if compactPathExists(candidate, deltas, laterPaths, next) {
				return true
			}
		}
	}
	return false
}

func compactPathExists(path DeltaPath, deltas DeltaSet, laterPaths map[uint64]int, next []int) bool {
	for index := laterPaths[compactPathHash(path)]; index != 0; index = next[index-1] {
		if deltas[index-1].Path.Equal(path) {
			return true
		}
	}
	return false
}

// compactPathPrefix 只复制有效路径节点；wildcardMask 的位置会把集合 key
// 归一化为 KeyNone，用于匹配严格祖先路径的“任意集合元素”语义。
func compactPathPrefix(path DeltaPath, depth int, wildcardMask int) DeltaPath {
	key := DeltaPath{Depth: uint8(depth)}
	for i := 0; i < depth; i++ {
		node := path.Nodes[i]
		if wildcardMask&(1<<i) != 0 {
			node.Key = PathKey{}
		}
		key.Nodes[i] = node
	}
	return key
}

func compactPathHash(path DeltaPath) uint64 {
	const (
		offset uint64 = 1469598103934665603
		prime  uint64 = 1099511628211
	)
	hash := offset
	mix := func(value uint64) {
		hash ^= value
		hash *= prime
	}
	mix(uint64(path.Depth))
	for i := 0; i < int(path.Depth); i++ {
		node := path.Nodes[i]
		mix(uint64(node.Field))
		mix(uint64(node.Key.Kind))
		switch node.Key.Kind {
		case KeyInt:
			mix(uint64(node.Key.Int))
		case KeyUint:
			mix(node.Key.Uint)
		case KeyString:
			for j := 0; j < len(node.Key.Str); j++ {
				mix(uint64(node.Key.Str[j]))
			}
		}
	}
	return hash
}

func compactDeltaEntries(ds DeltaSet) DeltaSet {
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
