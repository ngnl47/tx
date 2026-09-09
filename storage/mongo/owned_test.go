package mongo

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// 条件丢失或未同次推进序号，会让超时迟到的旧写入覆盖新值。
func TestOwnedUpdateCarriesAtomicOwnerAndSequenceConditions(t *testing.T) {
	client := &ownedClient{result: UpdateResult{MatchedCount: 1}}
	adapter, _ := NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{IDField: "Id", Collection: "units"}, Type: meta.Type{Fields: []meta.Field{{Name: "Id", BSONName: "Id"}, {Name: "Value", BSONName: "Value"}}}}
	source := ownedSource{}
	resolver := change.FieldNameResolverFunc(func(id change.FieldID, _ change.FieldNameKind) string {
		if id == 2 {
			return "Value"
		}
		return "Id"
	})
	err := adapter.ApplyOwnedChanges(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source, WritePosition{Owner: 3, Sequence: 9}, storage.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []Equality{{Field: "_tx.owner", Value: int64(3)}, {Field: "_tx.sequence", Value: map[string]any{"$lt": int64(9)}}}
	if !reflect.DeepEqual(client.filter.Conditions, want) {
		t.Fatalf("conditions = %#v", client.filter.Conditions)
	}
	set := client.update["$set"].(map[string]any)
	if set["Value"] != int64(100) || set["_tx.sequence"] != int64(9) {
		t.Fatalf("update = %#v", client.update)
	}
	client.result = UpdateResult{}
	if err := adapter.ApplyOwnedChanges(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source, WritePosition{Owner: 3, Sequence: 8}, storage.ApplyOptions{}); !errors.Is(err, ErrFenceRejected) {
		t.Fatalf("rejected write = %v", err)
	}
}

type ownedClient struct {
	Client
	result   UpdateResult
	filter   Filter
	update   map[string]any
	document map[string]any
}

func TestClaimOwnershipDoesNotResetSameOwnerSequence(t *testing.T) {
	client := &ownedClient{document: map[string]any{"_tx": map[string]any{"owner": int64(3), "sequence": int64(9)}}}
	adapter, _ := NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{IDField: "Id", Collection: "units"}}
	sequence, err := adapter.ClaimOwnership(context.Background(), spec, storage.RecordKey{ID: int64(7)}, 3)
	if err != nil || sequence != 9 {
		t.Fatalf("claim = %d, %v", sequence, err)
	}
	if !reflect.DeepEqual(client.filter.Conditions, []Equality{{Field: "_tx.owner", Value: map[string]any{"$not": map[string]any{"$gte": int64(3)}}}}) {
		t.Fatalf("claim condition = %#v", client.filter.Conditions)
	}
	if _, err := adapter.ClaimOwnership(context.Background(), spec, storage.RecordKey{ID: int64(7)}, 2); !errors.Is(err, ErrFenceRejected) {
		t.Fatalf("old owner = %v", err)
	}
}

func (client *ownedClient) FindOne(context.Context, string, Filter, ...string) (map[string]any, bool, error) {
	return client.document, client.document != nil, nil
}

func (client *ownedClient) UpdateOneDurable(_ context.Context, _ string, filter Filter, update map[string]any, _ storage.ApplyOptions) (UpdateResult, error) {
	client.filter, client.update = filter, update
	return client.result, nil
}

type ownedSource struct{}

func (ownedSource) HasChanges() bool { return true }
func (ownedSource) VisitChanges(visitor change.Visitor) error {
	return visitor.VisitChange(change.Path{Depth: 1, Nodes: [change.MaxPathDepth]change.PathNode{{Field: 2}}}, change.OpSet)
}
func (ownedSource) ResolveValue(change.Path) (any, bool) { return int64(100), true }

func (client *ownedClient) UpdateOne(ctx context.Context, collection string, filter Filter, update map[string]any, options storage.ApplyOptions) (UpdateResult, error) {
	return client.UpdateOneDurable(ctx, collection, filter, update, options)
}

// 比较同一变更文档在普通写入与带所有权条件的直接写入中的编码开销，不包含网络 I/O。
func BenchmarkDirectWriteConditions(b *testing.B) {
	for _, owned := range []bool{false, true} {
		name := "plain"
		if owned {
			name = "owned"
		}
		b.Run(name, func(b *testing.B) {
			client := &ownedClient{result: UpdateResult{MatchedCount: 1}}
			adapter, _ := NewAdapter(client)
			spec := meta.TypeSpec{Mongo: &meta.MongoSpec{IDField: "Id", Collection: "units"}}
			source := ownedSource{}
			resolver := change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "value" })
			b.ReportAllocs()
			for b.Loop() {
				var err error
				if owned {
					err = adapter.ApplyOwnedChanges(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source, WritePosition{Owner: 1, Sequence: 1}, storage.ApplyOptions{})
				} else {
					err = adapter.ApplyChangesWithOptions(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source, storage.ApplyOptions{})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
