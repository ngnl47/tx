package agent

import (
	"encoding/binary"
	"unicode/utf8"

	txprotocol "github.com/ngnl47/tx/protocol"
)

// FrameKind 区分握手帧、上行业务帧、下行帧和确认帧。
type FrameKind uint8

const (
	// FrameHandshake 建立或恢复逻辑会话。
	FrameHandshake FrameKind = iota + 1
	// FrameBusiness 携带客户端上行业务命令。
	FrameBusiness
	// FrameOutbound 携带服务端下行消息。
	FrameOutbound
	// FrameAck 在没有业务载荷可捎带时，单独确认对端已经连续处理的 Seq。
	FrameAck
)

const (
	frameVersion     byte = 2
	frameHeaderBytes      = 32
	frameRestoreFlag byte = 1
)

type RouteKeyKind = txprotocol.RouteKeyKind

const (
	// RouteKeyNone 表示命令不携带 Actor 路由键。
	RouteKeyNone = txprotocol.RouteKeyNone
	// RouteKeyInt64 表示有符号整数路由键。
	RouteKeyInt64 = txprotocol.RouteKeyInt64
	// RouteKeyUint64 表示无符号整数路由键。
	RouteKeyUint64 = txprotocol.RouteKeyUint64
	// RouteKeyString 表示 UTF-8 字符串路由键。
	RouteKeyString = txprotocol.RouteKeyString
)

type RouteKey = txprotocol.RouteKey

func Int64RouteKey(value int64) RouteKey {
	return txprotocol.Int64RouteKey(value)
}

func Uint64RouteKey(value uint64) RouteKey {
	return txprotocol.Uint64RouteKey(value)
}

func StringRouteKey(value string) RouteKey {
	return txprotocol.StringRouteKey(value)
}

// Frame 是与传输协议无关的协议帧。握手帧的 Payload 是 token 字节，业务帧使用 BON 字节，
// 下行帧则携带后端消息。CommandID 和 RouteKey 只属于上行业务帧。
type Frame struct {
	// Kind 决定其余头字段的校验规则。
	Kind FrameKind
	// Seq 是当前方向从 1 开始递增的消息序号。
	Seq int64
	// Ack 是发送方已经连续处理完成的对端消息序号。
	Ack int64
	// Restore 只用于握手帧，表示恢复既有逻辑会话。
	Restore bool
	// CommandID 是上行业务帧的稳定命令号。
	CommandID uint32
	// RouteKey 是上行业务帧用于直达目标服务或 Actor 的路由键。
	RouteKey RouteKey
	// Payload 是握手 token、BON 业务载荷或下行消息正文。
	Payload []byte
}

func (f Frame) validHandshake() bool {
	return f.Kind == FrameHandshake && f.Seq == 0 && f.Ack >= 0 && (f.Restore || f.Ack == 0) && !f.hasCommandRoute()
}

func (f Frame) validBusiness() bool {
	return f.Kind == FrameBusiness && f.Seq > 0 && f.Ack >= 0 && f.CommandID > 0 && f.validRouteKey()
}

func (f Frame) validOutbound() bool {
	return f.Kind == FrameOutbound && f.Seq > 0 && f.Ack >= 0 && !f.Restore && !f.hasCommandRoute()
}

func (f Frame) validAck() bool {
	return f.Kind == FrameAck && f.Seq == 0 && f.Ack > 0 && !f.Restore && len(f.Payload) == 0 && !f.hasCommandRoute()
}

func (f Frame) hasCommandRoute() bool {
	return f.CommandID != 0 || f.RouteKey.Kind() != RouteKeyNone
}

func (f Frame) validRouteKey() bool {
	switch f.RouteKey.Kind() {
	case RouteKeyNone, RouteKeyInt64, RouteKeyUint64:
		return true
	case RouteKeyString:
		value, _ := f.RouteKey.String()
		return value != "" && utf8.ValidString(value)
	default:
		return false
	}
}

type encodedFrame struct {
	routeKey []byte
	size     int
}

// EncodeFrame 将 Frame 编码为带版本的二进制信封。
func EncodeFrame(frame Frame, maxFrameBytes int) ([]byte, error) {
	prepared, err := prepareFrame(frame, maxFrameBytes)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, prepared.size)
	encodeFrameHeader(encoded[:frameHeaderBytes], frame, len(prepared.routeKey))
	copy(encoded[frameHeaderBytes:], prepared.routeKey)
	copy(encoded[frameHeaderBytes+len(prepared.routeKey):], frame.Payload)
	return encoded, nil
}

func prepareFrame(frame Frame, maxFrameBytes int) (encodedFrame, error) {
	if maxFrameBytes < frameHeaderBytes || (!frame.validHandshake() && !frame.validBusiness() && !frame.validOutbound() && !frame.validAck()) {
		return encodedFrame{}, ErrInvalidFrame
	}
	routeKey, err := encodeRouteKey(frame.RouteKey)
	if err != nil {
		return encodedFrame{}, err
	}
	size := frameHeaderBytes + len(routeKey) + len(frame.Payload)
	if size > maxFrameBytes {
		return encodedFrame{}, ErrFrameTooLarge
	}
	return encodedFrame{routeKey: routeKey, size: size}, nil
}

func encodeRouteKey(key RouteKey) ([]byte, error) {
	switch key.Kind() {
	case RouteKeyNone:
		return nil, nil
	case RouteKeyInt64:
		numeric, _ := key.Int64()
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(numeric))
		return encoded, nil
	case RouteKeyUint64:
		numeric, _ := key.Uint64()
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, numeric)
		return encoded, nil
	case RouteKeyString:
		text, _ := key.String()
		if text == "" || !utf8.ValidString(text) {
			return nil, ErrInvalidFrame
		}
		return []byte(text), nil
	default:
		return nil, ErrInvalidFrame
	}
}

func encodeFrameHeader(header []byte, frame Frame, routeKeySize int) {
	header[0] = frameVersion
	header[1] = byte(frame.Kind)
	if frame.Restore {
		header[2] = frameRestoreFlag
	}
	header[3] = byte(frame.RouteKey.Kind())
	binary.BigEndian.PutUint64(header[4:12], uint64(frame.Seq))
	binary.BigEndian.PutUint64(header[12:20], uint64(frame.Ack))
	binary.BigEndian.PutUint32(header[20:24], frame.CommandID)
	binary.BigEndian.PutUint32(header[24:28], uint32(routeKeySize))
	binary.BigEndian.PutUint32(header[28:32], uint32(len(frame.Payload)))
}

// DecodeFrame 解码并校验带版本的二进制信封。
func DecodeFrame(encoded []byte, maxFrameBytes int) (Frame, error) {
	if maxFrameBytes < frameHeaderBytes || len(encoded) < frameHeaderBytes {
		return Frame{}, ErrInvalidFrame
	}
	if len(encoded) > maxFrameBytes {
		return Frame{}, ErrFrameTooLarge
	}
	if encoded[0] != frameVersion || encoded[2]&^frameRestoreFlag != 0 {
		return Frame{}, ErrInvalidFrame
	}
	routeKeySize := int(binary.BigEndian.Uint32(encoded[24:28]))
	payloadSize := int(binary.BigEndian.Uint32(encoded[28:32]))
	if routeKeySize > len(encoded)-frameHeaderBytes || payloadSize != len(encoded)-frameHeaderBytes-routeKeySize {
		return Frame{}, ErrInvalidFrame
	}
	routeKey, err := decodeRouteKey(RouteKeyKind(encoded[3]), encoded[frameHeaderBytes:frameHeaderBytes+routeKeySize])
	if err != nil {
		return Frame{}, err
	}
	frame := Frame{
		Kind:      FrameKind(encoded[1]),
		Seq:       int64(binary.BigEndian.Uint64(encoded[4:12])),
		Ack:       int64(binary.BigEndian.Uint64(encoded[12:20])),
		Restore:   encoded[2]&frameRestoreFlag != 0,
		CommandID: binary.BigEndian.Uint32(encoded[20:24]),
		RouteKey:  routeKey,
		Payload:   cloneBytes(encoded[frameHeaderBytes+routeKeySize:]),
	}
	if (!frame.validHandshake() && !frame.validBusiness() && !frame.validOutbound() && !frame.validAck()) || (frame.Restore && frame.Kind != FrameHandshake) {
		return Frame{}, ErrInvalidFrame
	}
	return frame, nil
}

func decodeRouteKey(kind RouteKeyKind, encoded []byte) (RouteKey, error) {
	switch kind {
	case RouteKeyNone:
		if len(encoded) != 0 {
			return RouteKey{}, ErrInvalidFrame
		}
		return RouteKey{}, nil
	case RouteKeyInt64:
		if len(encoded) != 8 {
			return RouteKey{}, ErrInvalidFrame
		}
		return Int64RouteKey(int64(binary.BigEndian.Uint64(encoded))), nil
	case RouteKeyUint64:
		if len(encoded) != 8 {
			return RouteKey{}, ErrInvalidFrame
		}
		return Uint64RouteKey(binary.BigEndian.Uint64(encoded)), nil
	case RouteKeyString:
		value := string(encoded)
		if value == "" || !utf8.ValidString(value) {
			return RouteKey{}, ErrInvalidFrame
		}
		return StringRouteKey(value), nil
	default:
		return RouteKey{}, ErrInvalidFrame
	}
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
