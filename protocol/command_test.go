package protocol_test

import (
	"bytes"
	"github.com/ngnl47/tx/protocol"
	"testing"
)

func TestCommandPacketKeepsHashAndOpaqueBody(t *testing.T) {
	body := []byte{8, 0}
	packet, err := protocol.PackCommand(0xf1234567, body)
	if err != nil {
		t.Fatal(err)
	}
	hash, got, err := protocol.UnpackCommand(packet)
	if err != nil || hash != 0xf1234567 || !bytes.Equal(got, body) {
		t.Fatalf("%x %x %v", hash, got, err)
	}
	for _, invalid := range [][]byte{nil, {1, 2, 3}, {0, 0, 0, 0}} {
		if _, _, err := protocol.UnpackCommand(invalid); err == nil {
			t.Fatal("accepted invalid packet")
		}
	}
}
