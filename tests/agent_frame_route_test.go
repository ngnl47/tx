package tests

import (
	"testing"

	txagent "github.com/ngnl47/tx/modules/agent"
)

func TestAgentFrameRoundTripsCommandRouteWithoutParsingPayload(t *testing.T) {
	original := txagent.Frame{
		Kind:        txagent.FrameUpMsg,
		Seq:         1,
		CommandHash: 30001,
		RouteKey:    txagent.Int64RouteKey(470001),
		Payload:     []byte{0x91, 0x01},
	}
	encoded, err := txagent.EncodeFrame(original, 1024)
	if err != nil {
		t.Fatalf("EncodeFrame returned error: %v", err)
	}
	if encoded[0] != 4 {
		t.Fatalf("encoded frame version = %d, want 4", encoded[0])
	}
	decoded, err := txagent.DecodeFrame(encoded, 1024)
	if err != nil {
		t.Fatalf("DecodeFrame returned error: %v", err)
	}
	key, ok := decoded.RouteKey.Int64()
	if !ok || key != 470001 {
		t.Fatalf("decoded route key = %#v, want int64(470001)", decoded.RouteKey)
	}
	if decoded.CommandHash != original.CommandHash || string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("decoded frame = %#v, want command route and original payload", decoded)
	}
}

func TestAgentFrameRejectsUpMsgWithoutCommandHash(t *testing.T) {
	_, err := txagent.EncodeFrame(txagent.Frame{
		Kind:    txagent.FrameUpMsg,
		Seq:     1,
		Payload: []byte{1},
	}, 1024)
	if err == nil {
		t.Fatal("EncodeFrame accepted upMsg frame without CommandHash")
	}
}

func TestAgentFrameRoundTripsReadyControlFrame(t *testing.T) {
	original := txagent.Frame{Kind: txagent.FrameReady}
	encoded, err := txagent.EncodeFrame(original, 1024)
	if err != nil {
		t.Fatalf("EncodeFrame returned error: %v", err)
	}
	decoded, err := txagent.DecodeFrame(encoded, 1024)
	if err != nil {
		t.Fatalf("DecodeFrame returned error: %v", err)
	}
	if decoded.Kind != txagent.FrameReady || decoded.Seq != 0 || decoded.Ack != 0 || len(decoded.Payload) != 0 {
		t.Fatalf("decoded ready frame = %#v", decoded)
	}
}

func TestAgentDownFrameCarriesFullHashAndRejectsOldVersion(t *testing.T) {
	frame := txagent.Frame{Kind: txagent.FrameDownMsg, Seq: 1, CommandHash: 0xf1234567, Payload: []byte{8, 0}}
	encoded, err := txagent.EncodeFrame(frame, 1024)
	if err != nil {
		t.Fatal(err)
	}
	got, err := txagent.DecodeFrame(encoded, 1024)
	if err != nil || got.CommandHash != frame.CommandHash || string(got.Payload) != string(frame.Payload) {
		t.Fatalf("decoded = %+v, err=%v", got, err)
	}
	encoded[0] = 3
	if _, err := txagent.DecodeFrame(encoded, 1024); err == nil {
		t.Fatal("accepted obsolete frame version")
	}
	frame.CommandHash = 0
	if _, err := txagent.EncodeFrame(frame, 1024); err == nil {
		t.Fatal("accepted downlink without command hash")
	}
}
