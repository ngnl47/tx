package tests

import "github.com/ngnl47/tx/protocol"

func testDownPacket(body string) []byte {
	data, err := protocol.PackCommand(1, []byte(body))
	if err != nil {
		panic(err)
	}
	return data
}
