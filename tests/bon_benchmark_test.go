package tests

import (
	"encoding/json"
	"testing"

	"github.com/ngnl47/tx/protocol/bon"
)

func BenchmarkBONEnvelopeEncode(b *testing.B) {
	value := bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		payload, err := bon.MarshalEnvelope("Role_AddItem", value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBONPayload = payload
	}
}

func BenchmarkBONEnvelopeDecode(b *testing.B) {
	payload, err := bon.MarshalEnvelope("Role_AddItem", bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, body, err := bon.UnmarshalEnvelope(payload)
		if err != nil {
			b.Fatal(err)
		}
		var target bonProtocolRequest
		if err := bon.DecodeValue(body, &target); err != nil {
			b.Fatal(err)
		}
		benchmarkBONRequest = target
	}
}

func BenchmarkJSONEnvelopeEncode(b *testing.B) {
	value := bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		payload, err := json.Marshal(struct {
			Command string             `json:"c"`
			Body    bonProtocolRequest `json:"b"`
		}{Command: "Role_AddItem", Body: value})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkJSONPayload = payload
	}
}

func BenchmarkJSONEnvelopeDecode(b *testing.B) {
	payload, err := json.Marshal(struct {
		Command string             `json:"c"`
		Body    bonProtocolRequest `json:"b"`
	}{Command: "Role_AddItem", Body: bonProtocolRequest{RoleID: 470001, Item: &bonProtocolItem{ID: 1001, Quantity: 3}, Labels: map[string]int32{"daily.reward": 7}}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var target struct {
			Command string             `json:"c"`
			Body    bonProtocolRequest `json:"b"`
		}
		if err := json.Unmarshal(payload, &target); err != nil {
			b.Fatal(err)
		}
		benchmarkJSONRequest = target.Body
	}
}

var (
	benchmarkBONPayload  []byte
	benchmarkBONRequest  bonProtocolRequest
	benchmarkJSONPayload []byte
	benchmarkJSONRequest bonProtocolRequest
)
