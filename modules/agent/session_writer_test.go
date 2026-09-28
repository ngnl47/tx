package agent

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
)

func TestSessionFrameWriterDoesNotSendUpMsgAcknowledgements(t *testing.T) {
	transport := newInitializingSessionTransport()
	var written atomic.Int64
	writer := sessionFrameWriter{transport: transport}
	if err := writer.Write(context.Background(), Frame{Kind: FrameReady, Ack: 77}); err != nil {
		t.Fatal(err)
	}
	if frame := <-transport.writes; frame.Kind != FrameReady || frame.Ack != 0 {
		t.Fatalf("ready = %+v, want zero ack", frame)
	}
	if err := writer.writeDownMsg(context.Background(), Frame{Kind: FrameDownMsg, Seq: 1, Ack: 77}, &written); err != nil {
		t.Fatal(err)
	}
	if frame := <-transport.writes; frame.Kind != FrameDownMsg || frame.Ack != 0 || written.Load() != 1 {
		t.Fatalf("down = %+v, written = %d", frame, written.Load())
	}
}

func TestWriteDownMsgsBatchesAvailableTCPFramesInOrder(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream := &queuedMailboxStream{ctx: ctx, items: make(chan msgq.FetchResult, 5)}
	for seq := int64(1); seq <= 5; seq++ {
		stream.items <- msgq.FetchResult{Seq: seq, Payload: []byte{byte(seq)}}
	}
	writer := sessionFrameWriter{transport: &tcpTransport{connection: server, maxFrameBytes: 1024}}
	var written atomic.Int64
	result := make(chan error, 1)
	go func() {
		result <- (&Server{}).writeDownMsgs(ctx, stream, &writer, &written, 42)
	}()
	reader := &tcpTransport{connection: client, maxFrameBytes: 1024}
	for seq := int64(1); seq <= 5; seq++ {
		frame, err := reader.Read(ctx)
		if err != nil || frame.Seq != seq || frame.Ack != 0 || len(frame.Payload) != 1 || frame.Payload[0] != byte(seq) {
			t.Fatalf("frame %d = %+v, %v", seq, frame, err)
		}
	}
	if err := <-result; err != io.EOF {
		t.Fatalf("stream result = %v, want EOF", err)
	}
	if got := written.Load(); got != 5 {
		t.Fatalf("written seq = %d, want 5", got)
	}
}

func TestSessionFrameWriterBatchClearsAck(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	tcp := &tcpTransport{connection: server, maxFrameBytes: 1024}
	writer := sessionFrameWriter{transport: tcp}
	frames := []Frame{{Kind: FrameDownMsg, Seq: 1, Ack: 77}, {Kind: FrameDownMsg, Seq: 2, Ack: 77}}
	var written atomic.Int64
	result := make(chan error, 1)
	go func() {
		result <- writer.writeTCPDownBatch(context.Background(), tcp, frames, &written)
	}()
	reader := &tcpTransport{connection: client, maxFrameBytes: 1024}
	for seq := int64(1); seq <= 2; seq++ {
		frame, err := reader.Read(context.Background())
		if err != nil || frame.Seq != seq || frame.Ack != 0 {
			t.Fatalf("frame %d = %+v, %v", seq, frame, err)
		}
	}
	if err := <-result; err != nil || written.Load() != 2 {
		t.Fatalf("batch write = %v, written = %d", err, written.Load())
	}
}

type queuedMailboxStream struct {
	ctx   context.Context
	items chan msgq.FetchResult
}

func (stream *queuedMailboxStream) Context() context.Context { return stream.ctx }
func (stream *queuedMailboxStream) Recv() (msgq.FetchResult, error) {
	select {
	case item := <-stream.items:
		return item, nil
	default:
		return msgq.FetchResult{}, io.EOF
	}
}
func (stream *queuedMailboxStream) TryRecv() (msgq.FetchResult, bool, error) {
	select {
	case item := <-stream.items:
		return item, true, nil
	default:
		return msgq.FetchResult{}, false, nil
	}
}
func (*queuedMailboxStream) Ack(context.Context, int64, int64) error { return nil }
func (*queuedMailboxStream) Renew(context.Context) error             { return nil }
func (*queuedMailboxStream) Close() error                            { return nil }
