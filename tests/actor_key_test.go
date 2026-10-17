package tests

import (
	"github.com/ngnl47/tx/modules/actor"
	"testing"
)

func TestActorKeyPreservesExistingEncoding(t *testing.T) {
	if key := actor.Int64Key(-47); key != "i:-47" {
		t.Fatal(key)
	}
	if key := actor.Uint64Key(47); key != "u:47" {
		t.Fatal(key)
	}
	if key := actor.StringKey("room:47"); key != "s:room:47" {
		t.Fatal(key)
	}
	if value, err := actor.ParseInt64Key("i:47"); err != nil || value != 47 {
		t.Fatalf("%d %v", value, err)
	}
	if value, err := actor.ParseUint64Key("u:18446744073709551615"); err != nil || value != ^uint64(0) {
		t.Fatalf("%d %v", value, err)
	}
	if value, err := actor.ParseStringKey("s:room:47"); err != nil || value != "room:47" {
		t.Fatalf("%q %v", value, err)
	}
	if _, err := actor.ParseInt64Key("s:47"); err == nil {
		t.Fatal("错误类型被解释为整数")
	}
	if _, err := actor.ParseStringKey("room:47"); err == nil {
		t.Fatal("未编码键被解释为标准字符串键")
	}

}
