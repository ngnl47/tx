package tests

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/protocol/bon"
)

type bonProtocolItem struct {
	ID       int32     `json:"id" msgpack:"itemId"`
	Quantity int64     `json:"quantity" msgpack:"qty"`
	ExpireAt time.Time `json:"expireAt" msgpack:"expireAt"`
}

type bonProtocolRequest struct {
	RoleID int64            `json:"roleId" msgpack:"rid"`
	Item   *bonProtocolItem `json:"item" msgpack:"item"`
	Labels map[string]int32 `json:"labels" msgpack:"labels"`
	Scores map[int32]int64  `json:"scores" msgpack:"scores"`
	IDs    []uint64         `json:"ids" msgpack:"ids"`
	Raw    map[string]any   `json:"raw" msgpack:"raw"`
}

func TestBONBodyDecodesTypedMsgpackTaggedBody(t *testing.T) {
	want := bonProtocolRequest{
		RoleID: 4_700_001,
		Item:   &bonProtocolItem{ID: 1001, Quantity: 9_007_199_254_740_991, ExpireAt: time.UnixMilli(1_725_000_000_123)},
		Labels: map[string]int32{"daily.reward": 7},
		Scores: map[int32]int64{1001: 9_007_199_254_740_991},
		IDs:    []uint64{1, 9_007_199_254_740_991},
		Raw:    map[string]any{"enabled": true, "rates": []any{int32(3), "normal"}},
	}

	payload, err := bon.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var got bonProtocolRequest
	if err := bon.Unmarshal(payload, &got); err != nil {
		t.Fatalf("DecodeValue returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded body = %#v, want %#v", got, want)
	}
}

func TestBONBodyMatchesClientPrimitiveLayout(t *testing.T) {
	payload, err := bon.Marshal(struct {
		ID int32 `json:"id" msgpack:"pid"`
	}{ID: 1})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	want := []byte{
		8, 1,
		5, 3, 112, 105, 100,
		1, 1, 0, 0, 0,
	}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %v, want %v", payload, want)
	}
}

func TestBONUint32PreservesUpperHalf(t *testing.T) {
	for _, want := range []uint32{0, 2_147_483_647, 2_147_483_648, 4_294_967_295} {
		payload, err := bon.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got uint32
		if err := bon.Unmarshal(payload, &got); err != nil {
			t.Fatalf("uint32 %d 解码失败: %v", want, err)
		}
		if got != want {
			t.Fatalf("uint32 = %d, want %d", got, want)
		}
	}
}

func TestBONRejectsMalformedBody(t *testing.T) {
	var target any
	if err := bon.Unmarshal([]byte{8, 1, 5, 1, 99}, &target); err == nil {
		t.Fatal("Unmarshal returned nil error for truncated payload")
	}
}

func TestBONRoundTripsMultiByteLengths(t *testing.T) {
	type value struct {
		Text   string  `msgpack:"text"`
		Binary []byte  `msgpack:"binary"`
		Values []int32 `msgpack:"values"`
	}
	want := value{
		Text:   strings.Repeat("x", 128),
		Binary: make([]byte, 128),
		Values: make([]int32, 128),
	}
	encodedText, err := bon.Marshal(want.Text)
	if err != nil {
		t.Fatalf("Marshal text returned error: %v", err)
	}
	if len(encodedText) < 3 || !reflect.DeepEqual(encodedText[:3], []byte{5, 0x80, 0x01}) {
		t.Fatalf("128-byte string prefix = %v, want [5 128 1]", encodedText[:min(len(encodedText), 3)])
	}
	for index := range want.Binary {
		want.Binary[index] = byte(index)
		want.Values[index] = int32(index)
	}

	payload, err := bon.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var got value
	if err := bon.Unmarshal(payload, &got); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded value does not match input")
	}
}

func TestBONRejectsImpossibleContainerLengthBeforeAllocation(t *testing.T) {
	payload := append([]byte{9}, encodeBONNatural(65_536)...)
	var target any
	err := bon.Unmarshal(payload, &target)
	if err == nil || !strings.Contains(err.Error(), "array length 65536 exceeds remaining payload") {
		t.Fatalf("Unmarshal error = %v, want impossible array length error", err)
	}
}

func TestBONRejectsExcessiveContainerDepth(t *testing.T) {
	payload := make([]byte, 0, 2*65+1)
	for range 65 {
		payload = append(payload, 9, 1)
	}
	payload = append(payload, 0)

	var target any
	err := bon.Unmarshal(payload, &target)
	if err == nil || !strings.Contains(err.Error(), "container depth exceeds 64") {
		t.Fatalf("Unmarshal error = %v, want container depth error", err)
	}
}

func encodeBONNatural(value int) []byte {
	encoded := make([]byte, 0, 5)
	next := uint32(value)
	for next >= 0x80 {
		encoded = append(encoded, byte(next)|0x80)
		next >>= 7
	}
	return append(encoded, byte(next))
}
