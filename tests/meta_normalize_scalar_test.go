package tests

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/anno"
	"github.com/ngnl47/tx/meta"
)

func TestNormalizeRejectsInvalidTypeScalarParameters(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{name: "delay save duration", line: `// @storage:` + "`mode:\"fusion\" delaySaveTime:\"10mm\"`", want: "storage.delaySaveTime"},
		{name: "overflowing seconds", line: `// @storage:` + "`mode:\"fusion\" delaySaveTime:\"9223372036854775807\"`", want: "storage.delaySaveTime"},
		{name: "delay delete duration", line: `// @storage:` + "`mode:\"fusion\" delayDelTime:\"tomorrow\"`", want: "storage.delayDelTime"},
		{name: "memory cache duration", line: `// @storage:` + "`mode:\"fusion\" memoryCache:\"later\"`", want: "storage.memoryCache"},
		{name: "redis omit empty", line: `// @redis:` + "`omitempty:\"ture\"`", want: "redis.omitempty"},
		{name: "mongo sharding", line: `// @mongodb:` + "`enableSharding:\"yes-please\"`", want: "mongodb.enableSharding"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directives, err := anno.ParseTypeDoc([]string{tt.line})
			if err != nil {
				t.Fatalf("ParseTypeDoc returned error: %v", err)
			}
			_, err = meta.Normalize(directives)
			if err == nil {
				t.Fatal("Normalize returned nil error, want invalid scalar error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Normalize error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestNormalizeRejectsInvalidFieldScalarParameters(t *testing.T) {
	tests := []struct {
		name string
		tag  reflect.StructTag
		want string
	}{
		{name: "redis ignore", tag: `redis:"ignore:ture"`, want: "redis.ignore"},
		{name: "redis full update", tag: `redis:"fullUpdate:ture"`, want: "redis.fullUpdate"},
		{name: "mongo ignore", tag: `mongodb:"ignore:ture"`, want: "mongodb.ignore"},
		{name: "mongo unique", tag: `mongodb:"index,unique:ture"`, want: "mongodb.unique"},
		{name: "mongo weight", tag: `mongodb:"index,weight:heavy"`, want: "mongodb.weight"},
		{name: "mongo expire after", tag: `mongodb:"index,expireAfter:later"`, want: "mongodb.expireAfter"},
		{name: "mongo negative expire after", tag: `mongodb:"index,expireAfter:-1"`, want: "mongodb.expireAfter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directives, err := anno.ParseStructTag(tt.tag, anno.ScopeField)
			if err != nil {
				t.Fatalf("ParseStructTag returned error: %v", err)
			}
			for i := range directives {
				directives[i].Field = "Value"
			}
			_, err = meta.Normalize(directives)
			if err == nil {
				t.Fatal("Normalize returned nil error, want invalid scalar error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Normalize error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}
