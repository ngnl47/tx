package agent

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// TCPServer 是使用长度前缀二进制帧的默认 Agent TCP Server。
type TCPServer struct {
	listener      net.Listener
	agent         *Server
	maxFrameBytes int
	closeOnce     sync.Once
	closeErr      error
}

// ListenTCP 创建尚未开始接受连接的 TCP Agent Server。
func ListenTCP(address string, agent *Server, maxFrameBytes int) (*TCPServer, error) {
	if agent == nil || maxFrameBytes < frameHeaderBytes {
		return nil, ErrInvalidServer
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &TCPServer{listener: listener, agent: agent, maxFrameBytes: maxFrameBytes}, nil
}

// Addr 返回实际监听地址，适合动态端口和集成测试。
func (s *TCPServer) Addr() net.Addr {
	if s == nil || s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Serve 接受连接，并为每条连接运行独立 Agent Session。
func (s *TCPServer) Serve(ctx context.Context) error {
	if s == nil || s.listener == nil || s.agent == nil {
		return ErrServerNil
	}
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() {
			serveConnection(ctx, "tcp", connection.RemoteAddr().String(), s.agent, &tcpTransport{connection: connection, maxFrameBytes: s.maxFrameBytes})
		}()
	}
}

// Close 停止接受新 TCP 连接。
func (s *TCPServer) Close() error {
	if s == nil || s.listener == nil {
		return nil
	}
	s.closeOnce.Do(func() { s.closeErr = s.listener.Close() })
	return s.closeErr
}

type tcpTransport struct {
	connection    net.Conn
	maxFrameBytes int
	closeOnce     sync.Once
	closeErr      error
}

func (t *tcpTransport) Read(ctx context.Context) (Frame, error) {
	if t == nil || t.connection == nil {
		return Frame{}, ErrTransportNil
	}
	stop := context.AfterFunc(ctx, func() { _ = t.connection.SetReadDeadline(time.Now()) })
	defer stop()
	defer func() { _ = t.connection.SetReadDeadline(time.Time{}) }()

	var length [4]byte
	if _, err := io.ReadFull(t.connection, length[:]); err != nil {
		return Frame{}, err
	}
	size := int(binary.BigEndian.Uint32(length[:]))
	if size < frameHeaderBytes {
		return Frame{}, ErrInvalidFrame
	}
	if size > t.maxFrameBytes {
		return Frame{}, ErrFrameTooLarge
	}
	encoded := make([]byte, size)
	if _, err := io.ReadFull(t.connection, encoded); err != nil {
		return Frame{}, err
	}
	return DecodeFrame(encoded, t.maxFrameBytes)
}

func (t *tcpTransport) Write(ctx context.Context, frame Frame) error {
	if t == nil || t.connection == nil {
		return ErrTransportNil
	}
	prepared, err := prepareFrame(frame, t.maxFrameBytes)
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = t.connection.SetWriteDeadline(time.Now()) })
	defer stop()
	defer func() { _ = t.connection.SetWriteDeadline(time.Time{}) }()
	var prefix [4 + frameHeaderBytes]byte
	binary.BigEndian.PutUint32(prefix[:4], uint32(prepared.size))
	encodeFrameHeader(prefix[4:], frame, len(prepared.routeKey))
	return writeBuffersFull(t.connection, net.Buffers{prefix[:], prepared.routeKey, frame.Payload})
}

func (t *tcpTransport) Close() error {
	if t == nil || t.connection == nil {
		return nil
	}
	t.closeOnce.Do(func() { t.closeErr = t.connection.Close() })
	return t.closeErr
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func writeBuffersFull(writer io.Writer, buffers net.Buffers) error {
	for len(buffers) > 0 {
		written, err := buffers.WriteTo(writer)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
