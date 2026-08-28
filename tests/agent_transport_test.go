package tests

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	txagent "github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/msgq"
)

const agentTransportMaxFrameBytes = 256

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
	writeTCPAgentFrame(t, connection, txagent.Frame{Kind: txagent.FrameBusiness, Seq: 1, CommandID: 1, Payload: []byte("bon-request")})
	request := receiveDispatch(t, dispatched)
	if string(request.Payload) != "bon-request" {
		t.Fatalf("dispatch payload = %q", request.Payload)
	}
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish outbound: %v", err)
	}
	outbound := readTCPAgentFrame(t, connection)
	if outbound.Kind != txagent.FrameOutbound || outbound.Seq != 1 || string(outbound.Payload) != "bon-response" {
		t.Fatalf("outbound frame = %#v", outbound)
	}
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
	writeWebSocketAgentFrame(t, ctx, connection, txagent.Frame{Kind: txagent.FrameBusiness, Seq: 1, CommandID: 1, Payload: []byte("bon-request")})
	request := receiveDispatch(t, dispatched)
	if string(request.Payload) != "bon-request" {
		t.Fatalf("dispatch payload = %q", request.Payload)
	}
	if _, err := mailbox.Pub(context.Background(), msgq.PubRequest{Header: msgq.SessionHeader{RoleID: 470001}, Payload: []byte("bon-response")}); err != nil {
		t.Fatalf("publish outbound: %v", err)
	}
	outbound := readWebSocketAgentFrame(t, ctx, connection)
	if outbound.Kind != txagent.FrameOutbound || outbound.Seq != 1 || string(outbound.Payload) != "bon-response" {
		t.Fatalf("outbound frame = %#v", outbound)
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
