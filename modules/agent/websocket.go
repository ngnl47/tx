package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

// WebSocketOptions 描述浏览器 Agent 入口允许的精确 Origin。
// 空列表沿用同源校验；跨域调试必须显式配置完整的 http/https Origin。
type WebSocketOptions struct {
	// AllowedOrigins 是允许跨域连接的完整 http/https Origin 白名单。
	AllowedOrigins []string
}

// Validate 校验 Origin 白名单只包含完整且无通配符的 http/https Origin。
func (options WebSocketOptions) Validate() error {
	return validateWebSocketOptions(options)
}

// NewWebSocketHandler 创建接收 binary message 的默认 Agent WebSocket Handler。
func NewWebSocketHandler(agent *Server, maxFrameBytes int, options WebSocketOptions) (http.Handler, error) {
	if agent == nil || maxFrameBytes < frameHeaderBytes {
		return nil, ErrInvalidServer
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	acceptOptions := &websocket.AcceptOptions{OriginPatterns: slices.Clone(options.AllowedOrigins)}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, acceptOptions)
		if err != nil {
			return
		}
		serveConnection(request.Context(), "websocket", request.RemoteAddr, agent, &webSocketTransport{connection: connection, maxFrameBytes: maxFrameBytes})
	}), nil
}

func validateWebSocketOptions(options WebSocketOptions) error {
	for _, origin := range options.AllowedOrigins {
		if origin != strings.TrimSpace(origin) || origin == "" || strings.Contains(origin, "*") {
			return fmt.Errorf("agent: WebSocket origin %q must be an exact URL", origin)
		}
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("agent: WebSocket origin %q must be an exact http or https origin", origin)
		}
	}
	return nil
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
	prepared, err := prepareFrame(frame, t.maxFrameBytes)
	if err != nil {
		return err
	}
	writer, err := t.connection.Writer(ctx, websocket.MessageBinary)
	if err != nil {
		return err
	}
	var header [frameHeaderBytes]byte
	encodeFrameHeader(header[:], frame, len(prepared.routeKey))
	if err := writeFull(writer, header[:]); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writeFull(writer, prepared.routeKey); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writeFull(writer, frame.Payload); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (t *webSocketTransport) Close() error {
	if t == nil || t.connection == nil {
		return nil
	}
	t.closeOnce.Do(func() { t.closeErr = t.connection.CloseNow() })
	return t.closeErr
}
