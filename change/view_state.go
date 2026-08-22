package change

import "fmt"

// ViewState 保存物化 Store 视图的规范化路径与操作，不保存业务值。
// 业务值由生成的强类型 StoreView 持有。
type ViewState struct {
	changes     []viewStateChange
	index       map[uint64]int
	descendants map[uint64]int
}

type viewStateChange struct {
	path Path
	op   Op
}

// CaptureViewState 同步复制 Source 当前的规范化变更状态。
func CaptureViewState(source Source) (ViewState, error) {
	if err := ValidateSource(source); err != nil {
		return ViewState{}, err
	}
	var state ViewState
	if err := source.VisitChanges(VisitorFunc(func(path Path, op Op) error {
		return state.Record(path, op)
	})); err != nil {
		return ViewState{}, err
	}
	return state, nil
}

// Record 把一条后发生的变更合并到当前规范化状态。
func (s *ViewState) Record(path Path, op Op) error {
	if s == nil {
		return fmt.Errorf("change: view state is nil")
	}
	if !path.Valid() {
		return fmt.Errorf("%w: %#v", ErrInvalidPath, path)
	}
	if !op.Valid() {
		return fmt.Errorf("change: unknown operation %d", op)
	}
	path = canonicalViewPath(path)
	s.ensureIndex()

	if i, ok := s.pathIndex(path); ok {
		s.changes[i].op = op
		return nil
	}
	if i, ok := s.ancestorIndex(path); ok {
		switch s.changes[i].op {
		case OpSet, OpReplace:
			return nil
		case OpClear, OpDelete:
			s.changes[i].op = OpReplace
			return nil
		}
	}
	if s.descendants[viewPathHash(path)] > 0 {
		s.removeDescendants(path)
	}
	s.changes = append(s.changes, viewStateChange{path: path, op: op})
	s.indexChange(len(s.changes) - 1)
	return nil
}

func (s *ViewState) ensureIndex() {
	if s.index != nil {
		return
	}
	s.rebuildIndex()
}

func (s *ViewState) rebuildIndex() {
	s.index = make(map[uint64]int, len(s.changes))
	s.descendants = make(map[uint64]int, len(s.changes))
	for i := range s.changes {
		s.changes[i].path = canonicalViewPath(s.changes[i].path)
		s.indexChange(i)
	}
}

func (s *ViewState) indexChange(i int) {
	path := s.changes[i].path
	hash := viewPathHash(path)
	if current, ok := s.index[hash]; !ok {
		s.index[hash] = i
	} else if current >= 0 && !s.changes[current].path.Equal(path) {
		// 哈希碰撞只关闭该桶的快速路径，完整 Path 仍是正确性依据。
		s.index[hash] = -1
	}
	for depth := 1; depth < int(path.Depth); depth++ {
		ancestor := path
		ancestor.Depth = uint8(depth)
		clear(ancestor.Nodes[depth:])
		s.descendants[viewPathHash(ancestor)]++
	}
	for nodeIndex := 0; nodeIndex < int(path.Depth); nodeIndex++ {
		if path.Nodes[nodeIndex].Key.Kind == KeyNone {
			continue
		}
		ancestor := path
		ancestor.Depth = uint8(nodeIndex + 1)
		ancestor.Nodes[nodeIndex].Key = PathKey{}
		clear(ancestor.Nodes[nodeIndex+1:])
		s.descendants[viewPathHash(ancestor)]++
	}
}

func (s *ViewState) ancestorIndex(path Path) (int, bool) {
	for depth := 1; depth < int(path.Depth); depth++ {
		ancestor := path
		ancestor.Depth = uint8(depth)
		clear(ancestor.Nodes[depth:])
		if current, ok := s.pathIndex(ancestor); ok {
			return current, true
		}
	}
	for nodeIndex := 0; nodeIndex < int(path.Depth); nodeIndex++ {
		if path.Nodes[nodeIndex].Key.Kind == KeyNone {
			continue
		}
		ancestor := path
		ancestor.Depth = uint8(nodeIndex + 1)
		ancestor.Nodes[nodeIndex].Key = PathKey{}
		clear(ancestor.Nodes[nodeIndex+1:])
		if current, ok := s.pathIndex(ancestor); ok {
			return current, true
		}
	}
	return 0, false
}

func (s *ViewState) removeDescendants(path Path) {
	write := 0
	for i := range s.changes {
		if path.IsPrefixOf(s.changes[i].path) {
			continue
		}
		s.changes[write] = s.changes[i]
		write++
	}
	s.changes = s.changes[:write]
	s.rebuildIndex()
}

func canonicalViewPath(path Path) Path {
	for i := 0; i < int(path.Depth); i++ {
		key := path.Nodes[i].Key
		switch key.Kind {
		case KeyNone:
			path.Nodes[i].Key = PathKey{}
		case KeyInt:
			path.Nodes[i].Key = PathKey{Kind: KeyInt, Int: key.Int}
		case KeyUint:
			path.Nodes[i].Key = PathKey{Kind: KeyUint, Uint: key.Uint}
		case KeyString:
			path.Nodes[i].Key = PathKey{Kind: KeyString, Str: key.Str}
		}
	}
	clear(path.Nodes[path.Depth:])
	return path
}

func (s *ViewState) pathIndex(path Path) (int, bool) {
	hash := viewPathHash(path)
	current, ok := s.index[hash]
	if !ok {
		return 0, false
	}
	if current >= 0 {
		if s.changes[current].path.Equal(path) {
			return current, true
		}
		return 0, false
	}
	for i := range s.changes {
		if viewPathHash(s.changes[i].path) == hash && s.changes[i].path.Equal(path) {
			return i, true
		}
	}
	return 0, false
}

func viewPathHash(path Path) uint64 {
	const offset64 = 14695981039346656037
	hash := uint64(offset64)
	hash = mixViewPathHash(hash, uint64(path.Depth))
	for i := 0; i < int(path.Depth); i++ {
		node := path.Nodes[i]
		hash = mixViewPathHash(hash, uint64(node.Field))
		hash = mixViewPathHash(hash, uint64(node.Key.Kind))
		switch node.Key.Kind {
		case KeyInt:
			hash = mixViewPathHash(hash, uint64(node.Key.Int))
		case KeyUint:
			hash = mixViewPathHash(hash, node.Key.Uint)
		case KeyString:
			for stringIndex := 0; stringIndex < len(node.Key.Str); stringIndex++ {
				hash = mixViewPathHash(hash, uint64(node.Key.Str[stringIndex]))
			}
		}
	}
	return hash
}

func mixViewPathHash(hash, value uint64) uint64 {
	const prime64 = 1099511628211
	return (hash ^ value) * prime64
}

// Merge 按先后顺序把 next 的状态合并到当前状态。
func (s *ViewState) Merge(next ViewState) error {
	if s == nil {
		return fmt.Errorf("change: view state is nil")
	}
	for _, item := range next.changes {
		if err := s.Record(item.path, item.op); err != nil {
			return err
		}
	}
	return nil
}

// HasChanges 实现 Source。
func (s *ViewState) HasChanges() bool {
	return s != nil && len(s.changes) > 0
}

// VisitChanges 实现 Source。
func (s *ViewState) VisitChanges(visitor Visitor) error {
	if visitor == nil {
		return ErrNilVisitor
	}
	if s == nil {
		return nil
	}
	for _, item := range s.changes {
		if err := visitor.VisitChange(item.path, item.op); err != nil {
			return err
		}
	}
	return nil
}

// HasPath 判断路径是否被视图状态直接选择。
func (s *ViewState) HasPath(path Path) bool {
	if s == nil || !path.Valid() {
		return false
	}
	s.ensureIndex()
	_, ok := s.pathIndex(canonicalViewPath(path))
	return ok
}

// HasField 判断顶层字段或其任意子路径是否被视图状态选择。
func (s *ViewState) HasField(field FieldID) bool {
	if s == nil || field == FieldNone {
		return false
	}
	for _, item := range s.changes {
		if item.path.Depth > 0 && item.path.Nodes[0].Field == field {
			return true
		}
	}
	return false
}
