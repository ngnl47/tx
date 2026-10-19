package tests

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	txlog "github.com/ngnl47/tx/log"
	txagent "github.com/ngnl47/tx/modules/agent"
)

const agentTransportMaxFrameBytes = 256

func TestTCPAgentLogsConnectionLifecycle(t *testing.T) {
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	t.Cleanup(func() { txlog.SetDefault(nil) })

	agent, _, mailbox, _ := newAgentSessionFixture(t, nil)
	server, err := txagent.ListenTCP("127.0.0.1:0", agent, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("listen TCP agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()

	connection, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial TCP agent: %v", err)
	}
	writeTCPAgentFrame(t, connection, txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")})
	if ready := readTCPAgentFrame(t, connection); ready.Kind != txagent.FrameReady {
		t.Fatalf("handshake response = %#v, want Ready", ready)
	}
	waitForMailboxSession(t, mailbox)
	if err := connection.Close(); err != nil {
		t.Fatalf("close TCP agent: %v", err)
	}

	records := waitForAgentLifecycleLogs(t, handler)
	opened := assertAgentLifecycleLog(t, records, "agent connection opened", slog.LevelInfo)
	if !recordHasInt64Attr(opened, "connectionID", 7) {
		t.Fatalf("connection opened attrs = %#v", recordAttrs(opened))
	}
	established := assertAgentLifecycleLog(t, records, "agent session established", slog.LevelInfo)
	if !recordHasInt64Attr(established, "roleID", 470001) || !recordHasInt64Attr(established, "sessionID", 99) || !recordHasInt64Attr(established, "connectionID", 7) {
		t.Fatalf("session established attrs = %#v", recordAttrs(established))
	}
	closed := assertAgentLifecycleLog(t, records, "agent connection closed", slog.LevelInfo)
	if !recordHasStringAttr(closed, "transport", "tcp") || !recordHasInt64Attr(closed, "roleID", 470001) || !recordHasInt64Attr(closed, "sessionID", 99) || !recordHasInt64Attr(closed, "connectionID", 7) {
		t.Fatalf("connection closed attrs = %#v", recordAttrs(closed))
	}
}

func TestTCPAgentRoundTrip(t *testing.T) {
	agent, _, mailbox, dispatched := newAgentSessionFixture(t, nil)
	server, err := txagent.ListenTCP("127.0.0.1:0", agent, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("listen TCP agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()

	connection, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial TCP agent: %v", err)
	}
	defer connection.Close()
	writeTCPAgentFrame(t, connection, txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")})
	writeTCPAgentFrame(t, connection, txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandHash: 1, Payload: []byte("bon-request")})
	request := receiveDispatch(t, dispatched)
	if string(request.Payload) != "bon-request" {
		t.Fatalf("dispatch payload = %q", request.Payload)
	}
	if _, err := mailbox.PubSession(context.Background(), 99, testDownPacket("bon-response")); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	assertAgentTransportFrames(t, readTCPAgentFrame(t, connection), readTCPAgentFrame(t, connection))
}

func TestWebSocketAgentRoundTrip(t *testing.T) {
	agent, _, mailbox, dispatched := newAgentSessionFixture(t, nil)
	handler, err := txagent.NewWebSocketHandler(agent, agentTransportMaxFrameBytes, txagent.WebSocketOptions{})
	if err != nil {
		t.Fatalf("new WebSocket handler: %v", err)
	}
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, "ws"+httpServer.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial WebSocket agent: %v", err)
	}
	defer connection.CloseNow()
	writeWebSocketAgentFrame(t, ctx, connection, txagent.Frame{Kind: txagent.FrameHandshake, Payload: []byte("token")})
	writeWebSocketAgentFrame(t, ctx, connection, txagent.Frame{Kind: txagent.FrameUpMsg, Seq: 1, CommandHash: 1, Payload: []byte("bon-request")})
	request := receiveDispatch(t, dispatched)
	if string(request.Payload) != "bon-request" {
		t.Fatalf("dispatch payload = %q", request.Payload)
	}
	if _, err := mailbox.PubSession(context.Background(), 99, testDownPacket("bon-response")); err != nil {
		t.Fatalf("publish downMsg: %v", err)
	}
	assertAgentTransportFrames(t, readWebSocketAgentFrame(t, ctx, connection), readWebSocketAgentFrame(t, ctx, connection))
}

func assertAgentTransportFrames(t *testing.T, first, second txagent.Frame) {
	t.Helper()
	var ready, downMsg txagent.Frame
	for _, frame := range [...]txagent.Frame{first, second} {
		switch frame.Kind {
		case txagent.FrameReady:
			ready = frame
		case txagent.FrameDownMsg:
			downMsg = frame
		default:
			t.Fatalf("unexpected agent frame = %#v", frame)
		}
	}
	if ready.Kind != txagent.FrameReady || ready.Ack != 0 {
		t.Fatalf("ready frame = %#v, want ack=0", ready)
	}
	if downMsg.Seq != 1 || downMsg.Ack != 0 || string(downMsg.Payload) != "bon-response" {
		t.Fatalf("downMsg frame = %#v", downMsg)
	}
}

func TestAgentTransportRejectsOversizedFrame(t *testing.T) {
	agent, _, _, _ := newAgentSessionFixture(t, nil)
	server, err := txagent.ListenTCP("127.0.0.1:0", agent, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("listen TCP agent: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve(context.Background()) }()
	connection, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("dial TCP agent: %v", err)
	}
	defer connection.Close()
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], agentTransportMaxFrameBytes+1)
	if _, err := connection.Write(length[:]); err != nil {
		t.Fatalf("write oversized length: %v", err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	_, err = connection.Read(make([]byte, 1))
	if err == nil || (!errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed)) {
		t.Fatalf("read after oversized frame error = %v, want closed connection", err)
	}
}

func writeTCPAgentFrame(t *testing.T, connection net.Conn, frame txagent.Frame) {
	t.Helper()
	encoded, err := txagent.EncodeFrame(frame, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("encode TCP frame: %v", err)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(encoded)))
	if _, err := connection.Write(append(length[:], encoded...)); err != nil {
		t.Fatalf("write TCP frame: %v", err)
	}
}

func readTCPAgentFrame(t *testing.T, connection net.Conn) txagent.Frame {
	t.Helper()
	var length [4]byte
	if _, err := io.ReadFull(connection, length[:]); err != nil {
		t.Fatalf("read TCP frame length: %v", err)
	}
	encoded := make([]byte, binary.BigEndian.Uint32(length[:]))
	if _, err := io.ReadFull(connection, encoded); err != nil {
		t.Fatalf("read TCP frame body: %v", err)
	}
	frame, err := txagent.DecodeFrame(encoded, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("decode TCP frame: %v", err)
	}
	return frame
}

func writeWebSocketAgentFrame(t *testing.T, ctx context.Context, connection *websocket.Conn, frame txagent.Frame) {
	t.Helper()
	encoded, err := txagent.EncodeFrame(frame, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("encode WebSocket frame: %v", err)
	}
	if err := connection.Write(ctx, websocket.MessageBinary, encoded); err != nil {
		t.Fatalf("write WebSocket frame: %v", err)
	}
}

func readWebSocketAgentFrame(t *testing.T, ctx context.Context, connection *websocket.Conn) txagent.Frame {
	t.Helper()
	kind, reader, err := connection.Reader(ctx)
	if err != nil {
		t.Fatalf("read WebSocket frame: %v", err)
	}
	if kind != websocket.MessageBinary {
		t.Fatalf("WebSocket message kind = %v, want binary", kind)
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, agentTransportMaxFrameBytes+1))
	if err != nil {
		t.Fatalf("read WebSocket frame payload: %v", err)
	}
	frame, err := txagent.DecodeFrame(encoded, agentTransportMaxFrameBytes)
	if err != nil {
		t.Fatalf("decode WebSocket frame: %v", err)
	}
	return frame
}

func waitForAgentLifecycleLogs(t *testing.T, handler *recordingSlogHandler) []slog.Record {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		records := handler.Records()
		foundClosed := false
		for _, record := range records {
			if record.Message == "agent connection closed" {
				foundClosed = true
				break
			}
		}
		if foundClosed {
			return records
		}
		select {
		case <-deadline:
			t.Fatalf("agent lifecycle logs = %#v, want connection closed", records)
		case <-time.After(time.Millisecond):
		}
	}
}

func assertAgentLifecycleLog(t *testing.T, records []slog.Record, message string, level slog.Level) slog.Record {
	t.Helper()
	for _, record := range records {
		if record.Message == message {
			if record.Level != level {
				t.Fatalf("%q level = %s, want %s", message, record.Level, level)
			}
			return record
		}
	}
	t.Fatalf("agent lifecycle logs = %#v, want %q", records, message)
	return slog.Record{}
}
