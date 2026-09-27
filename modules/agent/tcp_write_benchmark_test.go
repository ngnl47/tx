package agent

import (
	"context"
	"io"
	"net"
	"testing"
)

func TestTCPTransportWriteDownBatchPreservesFrames(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	frames := []Frame{
		{Kind: FrameDownMsg, Seq: 10, Payload: []byte("first")},
		{Kind: FrameDownMsg, Seq: 11, Payload: []byte("second")},
		{Kind: FrameDownMsg, Seq: 12, Payload: []byte("third")},
	}
	result := make(chan error, 1)
	go func() {
		result <- (&tcpTransport{connection: server, maxFrameBytes: 1024}).writeDownBatch(context.Background(), frames)
	}()
	transport := &tcpTransport{connection: client, maxFrameBytes: 1024}
	for _, want := range frames {
		got, err := transport.Read(context.Background())
		if err != nil || got.Kind != want.Kind || got.Seq != want.Seq || string(got.Payload) != string(want.Payload) {
			t.Fatalf("frame = %+v, %v; want %+v", got, err, want)
		}
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// BenchmarkTCPDownlinkWrites 测量同一连接连续写出三帧时的传输成本上限。
func BenchmarkTCPDownlinkWrites(b *testing.B) {
	frames := []Frame{
		{Kind: FrameDownMsg, Seq: 1, Payload: make([]byte, 96)},
		{Kind: FrameDownMsg, Seq: 2, Payload: make([]byte, 96)},
		{Kind: FrameDownMsg, Seq: 3, Payload: make([]byte, 96)},
	}
	for _, variant := range []struct {
		name  string
		write func(context.Context, net.Conn, []Frame) error
	}{
		{name: "separate", write: func(ctx context.Context, conn net.Conn, frames []Frame) error {
			transport := tcpTransport{connection: conn, maxFrameBytes: 1024}
			for _, frame := range frames {
				if err := transport.Write(ctx, frame); err != nil {
					return err
				}
			}
			return nil
		}},
		{name: "one-writev", write: func(ctx context.Context, conn net.Conn, frames []Frame) error {
			return (&tcpTransport{connection: conn, maxFrameBytes: 1024}).writeDownBatch(ctx, frames)
		}},
	} {
		b.Run(variant.name, func(b *testing.B) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			defer listener.Close()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				conn, err := listener.Accept()
				if err == nil {
					defer conn.Close()
					_, _ = io.Copy(io.Discard, conn)
				}
			}()
			conn, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = conn.Close(); <-serverDone }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.SetBytes(int64(len(frames) * (4 + frameHeaderBytes + len(frames[0].Payload))))
			b.ResetTimer()
			for b.Loop() {
				if err := variant.write(ctx, conn, frames); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
