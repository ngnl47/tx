package agent

import (
	"errors"
	"testing"
)

func TestLogoutFramesHaveDedicatedControlSemantics(t *testing.T) {
	for _, frame := range []Frame{{Kind: FrameKind(7)}, {Kind: FrameKind(7), Payload: []byte("signed-ticket")}, {Kind: FrameKind(8)}} {
		encoded, err := EncodeFrame(frame, 1024)
		if err != nil {
			t.Fatalf("encode logout control: %v", err)
		}
		got, err := DecodeFrame(encoded, 1024)
		if err != nil || got.Kind != frame.Kind || string(got.Payload) != string(frame.Payload) {
			t.Fatalf("decode = %+v, %v", got, err)
		}
	}
	for _, frame := range []Frame{{Kind: FrameKind(7), Ack: 1}, {Kind: FrameKind(8), Payload: []byte("ticket")}, {Kind: FrameKind(7), Restore: true}} {
		if _, err := EncodeFrame(frame, 1024); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("invalid control %+v = %v", frame, err)
		}
	}
}
