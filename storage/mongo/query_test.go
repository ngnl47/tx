package mongo

import (
	"context"
	"encoding/json"
	"github.com/ngnl47/tx/meta"
	"reflect"
	"testing"
)

type queryClient struct {
	Client
	filter Filter
}

func (client *queryClient) FindOne(_ context.Context, collection string, filter Filter, _ ...string) (map[string]any, bool, error) {
	client.filter = filter
	return map[string]any{"accountId": int64(42)}, true, nil
}

func TestFindDocumentPreservesCompoundIdentityFilter(t *testing.T) {
	client := &queryClient{}
	adapter, err := NewAdapter(client)
	if err != nil {
		t.Fatal(err)
	}
	spec := meta.TypeSpec{Type: meta.Type{Fields: []meta.Field{{Name: "AccountId", JSONName: "accountId", BSONName: "accountId"}}}, Mongo: &meta.MongoSpec{IDField: "AccountId", Collection: "users"}}
	spec.DocumentFactory = func() any {
		return &struct {
			AccountID int64 `bson:"accountId" json:"accountId"`
		}{}
	}
	filter := Filter{Field: "platform", Value: "wechat", Conditions: []Equality{{Field: "platformUId", Value: "alice"}}}
	document, exists, err := adapter.FindDocument(context.Background(), spec, filter)
	if err != nil || !exists || document["accountId"] != json.Number("42") || !reflect.DeepEqual(client.filter, filter) {
		t.Fatalf("document=%v exists=%v filter=%v err=%v", document, exists, client.filter, err)
	}
}
