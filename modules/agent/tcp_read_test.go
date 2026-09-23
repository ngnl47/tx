package agent

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestTCPTransportReadKeepsEarlierPayload(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	frames := []Frame{
		{Kind: FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("first-payload")},
		{Kind: FrameUpMsg, Seq: 2, CommandID: 1, Payload: []byte("second-payload")},
	}
	writeErr := make(chan error, 1)
	go func() {
		for _, frame := range frames {
			encoded, err := EncodeFrame(frame, 1024)
			if err != nil {
				writeErr <- err
				return
			}
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(len(encoded)))
			if _, err := client.Write(append(length[:], encoded...)); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()
	transport := &tcpTransport{connection: server, maxFrameBytes: 1024}
	first, err := transport.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	if string(first.Payload) != "first-payload" || string(second.Payload) != "second-payload" {
		t.Fatalf("decoded payloads changed: %q, %q", first.Payload, second.Payload)
	}
}

func BenchmarkTCPTransportRead(b *testing.B) {
	encoded, err := EncodeFrame(Frame{Kind: FrameUpMsg, Seq: 1, CommandID: 1, Payload: []byte("ping")}, 1024)
	if err != nil {
		b.Fatal(err)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(encoded)))
	connection := &repeatingFrameConn{frame: append(length[:], encoded...)}
	transport := &tcpTransport{connection: connection, maxFrameBytes: 1024}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		frame, err := transport.Read(context.Background())
		if err != nil || frame.Seq != 1 {
			b.Fatalf("read frame = %#v, %v", frame, err)
		}
	}
}

type repeatingFrameConn struct {
	frame  []byte
	offset int
}

func (c *repeatingFrameConn) Read(buffer []byte) (int, error) {
	if c.offset == len(c.frame) {
		c.offset = 0
	}
	n := copy(buffer, c.frame[c.offset:])
	c.offset += n
	return n, nil
}

func (*repeatingFrameConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (*repeatingFrameConn) Close() error                     { return nil }
func (*repeatingFrameConn) LocalAddr() net.Addr              { return nil }
func (*repeatingFrameConn) RemoteAddr() net.Addr             { return nil }
func (*repeatingFrameConn) SetDeadline(time.Time) error      { return nil }
func (*repeatingFrameConn) SetReadDeadline(time.Time) error  { return nil }
func (*repeatingFrameConn) SetWriteDeadline(time.Time) error { return nil }
