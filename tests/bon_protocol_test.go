package tests

import (
	"reflect"
	"testing"
	"time"

	"github.com/ngnl47/tx/protocol/bon"
)

type bonProtocolItem struct {
	ID       int32     `json:"id"`
	Quantity int64     `json:"quantity"`
	ExpireAt time.Time `json:"expireAt"`
}

type bonProtocolRequest struct {
	RoleID int64            `json:"roleId"`
	Item   *bonProtocolItem `json:"item"`
	Labels map[string]int32 `json:"labels"`
	Scores map[int32]int64  `json:"scores"`
	IDs    []uint64         `json:"ids"`
	Raw    map[string]any   `json:"raw"`
}

func TestBONEnvelopeDecodesTypedJSONTaggedBody(t *testing.T) {
	const command = "Role_AddItem"
	want := bonProtocolRequest{
		RoleID: 4_700_001,
		Item:   &bonProtocolItem{ID: 1001, Quantity: 9_007_199_254_740_991, ExpireAt: time.UnixMilli(1_725_000_000_123)},
		Labels: map[string]int32{"daily.reward": 7},
		Scores: map[int32]int64{1001: 9_007_199_254_740_991},
		IDs:    []uint64{1, 9_007_199_254_740_991},
		Raw:    map[string]any{"enabled": true, "rates": []any{int32(3), "normal"}},
	}

	payload, err := bon.MarshalEnvelope(command, want)
	if err != nil {
		t.Fatalf("MarshalEnvelope returned error: %v", err)
	}
	actualCommand, body, err := bon.UnmarshalEnvelope(payload)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope returned error: %v", err)
	}
	if actualCommand != command {
		t.Fatalf("command = %q, want %q", actualCommand, command)
	}
	var got bonProtocolRequest
	if err := bon.DecodeValue(body, &got); err != nil {
		t.Fatalf("DecodeValue returned error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded body = %#v, want %#v", got, want)
	}
}

func TestBONEnvelopeMatchesClientPrimitiveLayout(t *testing.T) {
	payload, err := bon.MarshalEnvelope("Ping", struct {
		ID int32 `json:"id"`
	}{ID: 1})
	if err != nil {
		t.Fatalf("MarshalEnvelope returned error: %v", err)
	}
	want := []byte{
		8, 2,
		5, 1, 99,
		5, 4, 80, 105, 110, 103,
		5, 1, 98,
		8, 1,
		5, 2, 105, 100,
		1, 1, 0, 0, 0,
	}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %v, want %v", payload, want)
	}
}

func TestBONRejectsMalformedEnvelope(t *testing.T) {
	if _, _, err := bon.UnmarshalEnvelope([]byte{8, 1, 5, 1, 99}); err == nil {
		t.Fatal("UnmarshalEnvelope returned nil error for truncated payload")
	}
}
