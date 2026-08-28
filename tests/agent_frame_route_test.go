package tests

import (
	"testing"

	txagent "github.com/ngnl47/tx/modules/agent"
)

func TestAgentFrameRoundTripsCommandRouteWithoutParsingPayload(t *testing.T) {
	original := txagent.Frame{
		Kind:      txagent.FrameUpMsg,
		Seq:       1,
		CommandID: 30001,
		RouteKey:  txagent.Int64RouteKey(470001),
		Payload:   []byte{0x91, 0x01},
	}
	encoded, err := txagent.EncodeFrame(original, 1024)
	if err != nil {
		t.Fatalf("EncodeFrame returned error: %v", err)
	}
	decoded, err := txagent.DecodeFrame(encoded, 1024)
	if err != nil {
		t.Fatalf("DecodeFrame returned error: %v", err)
	}
	key, ok := decoded.RouteKey.Int64()
	if !ok || key != 470001 {
		t.Fatalf("decoded route key = %#v, want int64(470001)", decoded.RouteKey)
	}
	if decoded.CommandID != original.CommandID || string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("decoded frame = %#v, want command route and original payload", decoded)
	}
}

func TestAgentFrameRejectsUpMsgWithoutCommandID(t *testing.T) {
	_, err := txagent.EncodeFrame(txagent.Frame{
		Kind:    txagent.FrameUpMsg,
		Seq:     1,
		Payload: []byte{1},
	}, 1024)
	if err == nil {
		t.Fatal("EncodeFrame accepted upMsg frame without CommandID")
	}
}
