package tests

import (
	"github.com/ngnl47/tx/modules/actor"
	"testing"
)

func TestActorKeyPreservesExistingEncoding(t *testing.T) {
	if key := actor.Int64ID(-47); key != "i:-47" {
		t.Fatal(key)
	}
	if key := actor.Uint64ID(47); key != "u:47" {
		t.Fatal(key)
	}
	if key := actor.StringID("room:47"); key != "s:room:47" {
		t.Fatal(key)
	}
	if value, err := actor.ParseInt64ID("i:47"); err != nil || value != 47 {
		t.Fatalf("%d %v", value, err)
	}
	if value, err := actor.ParseUint64ID("u:18446744073709551615"); err != nil || value != ^uint64(0) {
		t.Fatalf("%d %v", value, err)
	}
	if value, err := actor.ParseStringID("s:room:47"); err != nil || value != "room:47" {
		t.Fatalf("%q %v", value, err)
	}
	if _, err := actor.ParseInt64ID("s:47"); err == nil {
		t.Fatal("错误类型被解释为整数")
	}
	if _, err := actor.ParseStringID("room:47"); err == nil {
		t.Fatal("未编码键被解释为标准字符串键")
	}

}
