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

func TestBuildIndexModelsRejectsInvalidTTLAndTextWeightOptions(t *testing.T) {
	expireAfter := int64(0)
	tests := []struct {
		name string
		spec meta.TypeSpec
		want string
	}{
		{
			name: "compound TTL",
			spec: meta.TypeSpec{
				Type: meta.Type{Fields: []meta.Field{{Name: "ExpireAt", BSONName: "expireAt"}}},
				Fields: map[string]meta.FieldSpec{
					"ExpireAt": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{
						Keys:        []meta.IndexKeySpec{{Path: "expireAt", Order: "1"}, {Path: "roleId", Order: "1"}},
						ExpireAfter: &expireAfter,
					}}}},
				},
			},
			want: "TTL index",
		},
		{
			name: "weight without text key",
			spec: meta.TypeSpec{
				Type: meta.Type{Fields: []meta.Field{{Name: "Content", BSONName: "content"}}},
				Fields: map[string]meta.FieldSpec{
					"Content": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{
						Keys:    []meta.IndexKeySpec{{Order: "1"}},
						Weights: []meta.IndexWeightSpec{{Weight: 10}},
					}}}},
				},
			},
			want: "text index",
		},
		{
			name: "weight outside Mongo range",
			spec: meta.TypeSpec{
				Type: meta.Type{Fields: []meta.Field{{Name: "Content", BSONName: "content"}}},
				Fields: map[string]meta.FieldSpec{
					"Content": {Mongo: &meta.MongoFieldSpec{Indexes: []meta.IndexSpec{{
						Keys:    []meta.IndexKeySpec{{Order: "text"}},
						Weights: []meta.IndexWeightSpec{{Weight: 100000}},
					}}}},
				},
			},
			want: "weight",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildIndexModels(tt.spec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BuildIndexModels error = %v, want %q", err, tt.want)
			}
		})
	}
}
