package tests

import (
	"encoding/json"
	"testing"

	"github.com/ngnl47/tx/protocol/bon"
)

func BenchmarkBONBodyEncode(b *testing.B) {
	value := bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		payload, err := bon.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBONPayload = payload
	}
}

func BenchmarkBONBodyDecode(b *testing.B) {
	payload, err := bon.Marshal(bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var target bonProtocolRequest
		if err := bon.Unmarshal(payload, &target); err != nil {
			b.Fatal(err)
		}
		benchmarkBONRequest = target
	}
}

func BenchmarkBONRejectImpossibleArray(b *testing.B) {
	payload := append([]byte{9}, encodeBONNatural(65_536)...)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var target any
		benchmarkBONErr = bon.Unmarshal(payload, &target)
	}
}

func BenchmarkJSONBodyEncode(b *testing.B) {
	value := bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		payload, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkJSONPayload = payload
	}
}

func BenchmarkJSONBodyDecode(b *testing.B) {
	payload, err := json.Marshal(bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var target bonProtocolRequest
		if err := json.Unmarshal(payload, &target); err != nil {
			b.Fatal(err)
		}
		benchmarkJSONRequest = target
	}
}

var (
	benchmarkBONPayload  []byte
	benchmarkBONRequest  bonProtocolRequest
	benchmarkBONErr      error
	benchmarkJSONPayload []byte
	benchmarkJSONRequest bonProtocolRequest
)
