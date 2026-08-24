package tests

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/meta"
)

func TestNormalizeMongoIndexesPreservesRepeatedDeclarations(t *testing.T) {
	spec := normalizeMongoFieldTag(t, "PlatformUId", `mongodb:"index:p_id,index"`)
	indexes := spec.Fields["PlatformUId"].Mongo.Indexes
	if len(indexes) != 2 {
		t.Fatalf("indexes = %#v, want named and anonymous indexes", indexes)
	}
}

func TestNormalizeMongoIndexesPreservesExplicitCompoundKeys(t *testing.T) {
	spec := normalizeMongoFieldTag(t, "AreaId", `mongodb:"index::areaId:1:deleted:1:roleId:1"`)
	indexes := spec.Fields["AreaId"].Mongo.Indexes
	if len(indexes) != 1 {
		t.Fatalf("indexes = %#v, want one compound index", indexes)
	}

	encoded, err := json.Marshal(indexes[0])
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var got struct {
		Name string `json:"name"`
		Keys []struct {
			Path  string `json:"path"`
			Order string `json:"order"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got.Name != "" || !reflect.DeepEqual(got.Keys, []struct {
		Path  string `json:"path"`
		Order string `json:"order"`
	}{
		{Path: "areaId", Order: "1"},
		{Path: "deleted", Order: "1"},
		{Path: "roleId", Order: "1"},
	}) {
		t.Fatalf("index = %s, want unnamed areaId/deleted/roleId compound index", encoded)
	}
}

func TestNormalizeMongoIndexesPreservesRepeatedExplicitCompounds(t *testing.T) {
	spec := normalizeMongoFieldTag(
		t,
		"RoleId",
		`mongodb:"index::roleId:1:state:1:expireAt:1,index::roleId:1:id:1:category:1:state:1:expireAt:1"`,
	)
	indexes := spec.Fields["RoleId"].Mongo.Indexes
	if len(indexes) != 2 {
		t.Fatalf("indexes = %#v, want two explicit compound indexes", indexes)
	}
	if len(indexes[0].Keys) != 3 || len(indexes[1].Keys) != 5 {
		t.Fatalf("indexes = %#v, want 3-key and 5-key compounds", indexes)
	}
}

func TestNormalizeMongoIndexesRejectsModifiersWithoutPrecedingIndex(t *testing.T) {
	for _, tag := range []reflect.StructTag{
		`mongodb:"unique,index"`,
		`mongodb:"order:-1"`,
		`mongodb:"weight:10"`,
		`mongodb:"expireAfter:0"`,
		`mongodb:"index:false,unique"`,
	} {
		t.Run(string(tag), func(t *testing.T) {
			directives, err := anno.ParseStructTag(tag, anno.ScopeField)
			if err != nil {
				t.Fatalf("ParseStructTag returned error: %v", err)
			}
			for i := range directives {
				directives[i].Field = "ExpireAt"
			}
			_, err = meta.Normalize(directives)
			if err == nil || !strings.Contains(err.Error(), "requires a preceding index") {
				t.Fatalf("Normalize error = %v, want preceding index error", err)
			}
		})
	}
}

func TestNormalizeMongoIndexesRejectsUnknownIndexTypeParameter(t *testing.T) {
	directives, err := anno.ParseStructTag(`mongodb:"index,indexType:-1"`, anno.ScopeField)
	if err != nil {
		t.Fatalf("ParseStructTag returned error: %v", err)
	}
	_, err = meta.Normalize(directives)
	if err == nil || !strings.Contains(err.Error(), "unknown param: mongodb.indexType") {
		t.Fatalf("Normalize error = %v, want unknown indexType rejection", err)
	}
}

func normalizeMongoFieldTag(t *testing.T, field string, tag reflect.StructTag) meta.TypeSpec {
	t.Helper()
	directives, err := anno.ParseStructTag(tag, anno.ScopeField)
	if err != nil {
		t.Fatalf("ParseStructTag returned error: %v", err)
	}
	for i := range directives {
		directives[i].Field = field
	}
	spec, err := meta.Normalize(directives)
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	return spec
}
