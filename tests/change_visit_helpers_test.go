package tests

import "github.com/ngnl47/tx/change"

const (
	changeFieldName  change.FieldID = 1
	changeFieldItems change.FieldID = 2
	changeFieldQty   change.FieldID = 3
)

func changePath(nodes ...change.PathNode) change.Path {
	var path change.Path
	path.Depth = uint8(len(nodes))
	copy(path.Nodes[:], nodes)
	return path
}

func fieldNode(field change.FieldID) change.PathNode {
	return change.PathNode{Field: field}
}

func intKeyNode(field change.FieldID, key int64) change.PathNode {
	return change.PathNode{
		Field: field,
		Key:   change.PathKey{Kind: change.KeyInt, Int: key},
	}
}

type recordedChange struct {
	Path change.Path
	Op   change.Op
}

type recordedChanges []recordedChange

func (changes recordedChanges) clone() recordedChanges {
	return append(recordedChanges(nil), changes...)
}

func (changes recordedChanges) VisitChanges(visitor change.Visitor) error {
	if visitor == nil {
		return change.ErrNilVisitor
	}
	for _, item := range changes {
		if err := visitor.VisitChange(item.Path, item.Op); err != nil {
			return err
		}
	}
	return nil
}

func (changes recordedChanges) HasChanges() bool {
	return len(changes) > 0
}

type emptyDirtySource struct{}

func (emptyDirtySource) VisitChanges(change.Visitor) error { return nil }

func (emptyDirtySource) HasChanges() bool { return true }

type typedNilSource struct{}

func (*typedNilSource) VisitChanges(change.Visitor) error {
	panic("typed nil source must be rejected before VisitChanges")
}

func (*typedNilSource) HasChanges() bool {
	panic("typed nil source must be rejected before HasChanges")
}

type failingSource struct {
	err error
}

func (source failingSource) VisitChanges(visitor change.Visitor) error {
	if visitor == nil {
		return change.ErrNilVisitor
	}
	if err := visitor.VisitChange(changePath(fieldNode(changeFieldName)), change.OpSet); err != nil {
		return err
	}
	return source.err
}

func (failingSource) HasChanges() bool { return true }

type recordedChangeCollector struct {
	Changes recordedChanges
}

func (collector *recordedChangeCollector) VisitChange(path change.Path, op change.Op) error {
	collector.Changes = append(collector.Changes, recordedChange{Path: path, Op: op})
	return nil
}

func collectRecordedChanges(source change.Source) (recordedChanges, error) {
	collector := recordedChangeCollector{}
	if err := source.VisitChanges(&collector); err != nil {
		return nil, err
	}
	return collector.Changes, nil
}
