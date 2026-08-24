package mongo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/meta"
)

func TestBuildIndexModelsGroupsNamedAndAnonymousIndexes(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{
			{Name: "RoleId", BSONName: "_id"},
			{Name: "Platform", BSONName: "platform"},
			{Name: "PlatformUId", BSONName: "platformUId"},
		}},
		Fields: map[string]meta.FieldSpec{
			"RoleId":   {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Keys: []meta.IndexKeySpec{{Order: "1"}}, Unique: true}}}},
			"Platform": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Name: "p_id", Keys: []meta.IndexKeySpec{{Order: "1"}}}}}},
			"PlatformUId": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{
				{Name: "p_id", Keys: []meta.IndexKeySpec{{Order: "1"}}},
				{Keys: []meta.IndexKeySpec{{Order: "-1"}}},
			}}},
		},
	}

	models, err := BuildIndexModels(spec)
	if err != nil {
		t.Fatalf("BuildIndexModels returned error: %v", err)
	}
	want := []IndexModel{
		{Name: "p_id", Keys: []IndexKey{{Path: "platform", Order: "1"}, {Path: "platformUId", Order: "1"}}},
		{Name: "platformUId_-1", Keys: []IndexKey{{Path: "platformUId", Order: "-1"}}},
	}
	if !reflect.DeepEqual(models, want) {
		t.Fatalf("models = %#v, want %#v", models, want)
	}
}

func TestBuildIndexModelsResolvesExplicitAndRelativePaths(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{Name: "Profile", BSONName: "profile"}}},
		Fields: map[string]meta.FieldSpec{
			"Profile": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{
				Keys: []meta.IndexKeySpec{
					{Path: ".level", Order: "-1"},
					{Path: "areaId", Order: "1"},
				},
			}}}},
		},
	}

	models, err := BuildIndexModels(spec)
	if err != nil {
		t.Fatalf("BuildIndexModels returned error: %v", err)
	}
	want := []IndexModel{{
		Name: "profile.level_-1_areaId_1",
		Keys: []IndexKey{{Path: "profile.level", Order: "-1"}, {Path: "areaId", Order: "1"}},
	}}
	if !reflect.DeepEqual(models, want) {
		t.Fatalf("models = %#v, want %#v", models, want)
	}
}

func TestBuildIndexModelsRejectsNamedIndexOptionConflicts(t *testing.T) {
	expire60 := int64(60)
	expire120 := int64(120)
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{Name: "First"}, {Name: "Second"}}},
		Fields: map[string]meta.FieldSpec{
			"First":  {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Name: "same", Keys: []meta.IndexKeySpec{{Order: "1"}}, ExpireAfter: &expire60}}}},
			"Second": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{Name: "same", Keys: []meta.IndexKeySpec{{Order: "1"}}, ExpireAfter: &expire120}}}},
		},
	}

	_, err := BuildIndexModels(spec)
	if err == nil || !strings.Contains(err.Error(), "same") {
		t.Fatalf("BuildIndexModels error = %v, want named index conflict", err)
	}
}
