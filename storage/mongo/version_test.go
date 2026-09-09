package mongo

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// 冲突后必须重新读版本；保留字段级变更，不能退化为全文替换。
func TestVersionedChangesRefreshBaselineAfterConflict(t *testing.T) {
	client := &versionClient{document: map[string]any{"_tx_version": int64(5)}}
	adapter, _ := NewAdapter(client)
	spec := meta.TypeSpec{Mongo: &meta.MongoSpec{IDField: "Id", Collection: "units"}}
	resolver := change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "value" })
	source := versionSource{}
	err := adapter.ApplyVersionedChanges(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source)
	if !errors.Is(err, ErrFenceRejected) {
		t.Fatalf("conflict = %v", err)
	}
	client.document["_tx_version"] = int64(6)
	client.result = UpdateResult{MatchedCount: 1}
	if err := adapter.ApplyVersionedChanges(context.Background(), spec, storage.RecordKey{ID: int64(7)}, source, resolver, source); err != nil {
		t.Fatal(err)
	}
	if len(client.filter.Conditions) != 1 || client.filter.Conditions[0].Value != int64(6) {
		t.Fatalf("stale baseline: %+v", client.filter)
	}
	set := client.update["$set"].(map[string]any)
	if len(set) != 2 || set["value"] != int64(100) || set["_tx_version"] != int64(7) {
		t.Fatalf("update = %+v", client.update)
	}
}

type versionClient struct {
	Client
	result   UpdateResult
	filter   Filter
	update   map[string]any
	document map[string]any
}

func (client *versionClient) FindOne(context.Context, string, Filter, ...string) (map[string]any, bool, error) {
	return client.document, client.document != nil, nil
}

func (client *versionClient) UpdateOneDurable(_ context.Context, _ string, filter Filter, update map[string]any, _ storage.ApplyOptions) (UpdateResult, error) {
	client.filter, client.update = filter, update
	return client.result, nil
}

type versionSource struct{}

func (versionSource) HasChanges() bool { return true }
func (versionSource) VisitChanges(visitor change.Visitor) error {
	return visitor.VisitChange(change.Path{Depth: 1, Nodes: [change.MaxPathDepth]change.PathNode{{Field: 2}}}, change.OpSet)
}
func (versionSource) ResolveValue(change.Path) (any, bool) { return int64(100), true }

func (client *versionClient) UpdateOne(ctx context.Context, collection string, filter Filter, update map[string]any, options storage.ApplyOptions) (UpdateResult, error) {
	return client.UpdateOneDurable(ctx, collection, filter, update, options)
}
