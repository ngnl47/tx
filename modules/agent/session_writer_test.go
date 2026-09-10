package agent

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestSessionFrameWriterSamplesAckAtWrite(t *testing.T) {
	transport := newInitializingSessionTransport()
	var processed, written atomic.Int64
	writer := sessionFrameWriter{transport: transport, processedUpSeq: &processed}
	processed.Store(6769)
	// 模拟下行先组帧、上行随后完成；旧帧不能把确认号带回过去。
	down := Frame{Kind: FrameDownMsg, Seq: 1, Ack: processed.Load()}
	processed.Store(6770)
	if err := writer.Write(context.Background(), Frame{Kind: FrameAck, Ack: processed.Load()}); err != nil {
		t.Fatal(err)
	}
	if frame := <-transport.writes; frame.Ack != 6770 {
		t.Fatalf("ack = %d, want 6770", frame.Ack)
	}
	if err := writer.writeDownMsg(context.Background(), down, &written); err != nil {
		t.Fatal(err)
	}
	if frame := <-transport.writes; frame.Ack != 6770 || written.Load() != 1 {
		t.Fatalf("down = %+v, written = %d", frame, written.Load())
	}
}
