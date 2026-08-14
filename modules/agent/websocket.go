package agent

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// NewWebSocketHandler 创建接收 binary message 的默认 Agent WebSocket Handler。
func NewWebSocketHandler(agent *Server, maxFrameBytes int) (http.Handler, error) {
	if agent == nil || maxFrameBytes < frameHeaderBytes {
		return nil, ErrInvalidServer
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		_ = agent.Serve(request.Context(), &webSocketTransport{connection: connection, maxFrameBytes: maxFrameBytes})
	}), nil
}

type webSocketTransport struct {
	connection    *websocket.Conn
	maxFrameBytes int
	closeOnce     sync.Once
	closeErr      error
}

func (t *webSocketTransport) Read(ctx context.Context) (Frame, error) {
	if t == nil || t.connection == nil {
		return Frame{}, ErrTransportNil
	}
	kind, reader, err := t.connection.Reader(ctx)
	if err != nil {
		return Frame{}, err
	}
	if kind != websocket.MessageBinary {
		return Frame{}, ErrInvalidFrame
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, int64(t.maxFrameBytes)+1))
	if err != nil {
		return Frame{}, err
	}
	if len(encoded) > t.maxFrameBytes {
		return Frame{}, ErrFrameTooLarge
	}
	return DecodeFrame(encoded, t.maxFrameBytes)
}

func (t *webSocketTransport) Write(ctx context.Context, frame Frame) error {
	if t == nil || t.connection == nil {
		return ErrTransportNil
	}
	encoded, err := EncodeFrame(frame, t.maxFrameBytes)
	if err != nil {
		return err
	}
	return t.connection.Write(ctx, websocket.MessageBinary, encoded)
}

func (t *webSocketTransport) Close() error {
	if t == nil || t.connection == nil {
		return nil
	}
	t.closeOnce.Do(func() { t.closeErr = t.connection.CloseNow() })
	return t.closeErr
}
