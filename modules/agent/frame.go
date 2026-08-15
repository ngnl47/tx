package agent

import (
	"encoding/binary"
)

// FrameKind 区分 Agent 统一信封中的握手、上行和下行帧。
type FrameKind uint8

const (
	FrameHandshake FrameKind = iota + 1
	FrameBusiness
	FrameOutbound
	FrameAck
)

const (
	frameVersion     byte = 1
	frameHeaderBytes      = 24
	frameRestoreFlag byte = 1
)

// Frame 是传输无关的协议帧。Payload 对握手是 token，对业务帧是 BON bytes。
type Frame struct {
	Kind    FrameKind
	Seq     int64
	Ack     int64
	Restore bool
	Payload []byte
}

func (f Frame) validHandshake() bool {
	return f.Kind == FrameHandshake && f.Seq == 0 && f.Ack >= 0 && (f.Restore || f.Ack == 0)
}

func (f Frame) validBusiness() bool {
	return f.Kind == FrameBusiness && f.Seq > 0 && f.Ack >= 0
}

func (f Frame) validOutbound() bool {
	return f.Kind == FrameOutbound && f.Seq > 0 && f.Ack >= 0 && !f.Restore
}

func (f Frame) validAck() bool {
	return f.Kind == FrameAck && f.Seq == 0 && f.Ack > 0 && !f.Restore && len(f.Payload) == 0
}

// EncodeFrame 将统一 Agent Frame 编码为版本化二进制信封。
func EncodeFrame(frame Frame, maxFrameBytes int) ([]byte, error) {
	size, err := validateFrame(frame, maxFrameBytes)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, size)
	encodeFrameHeader(encoded[:frameHeaderBytes], frame)
	copy(encoded[frameHeaderBytes:], frame.Payload)
	return encoded, nil
}

func validateFrame(frame Frame, maxFrameBytes int) (int, error) {
	if maxFrameBytes < frameHeaderBytes || (!frame.validHandshake() && !frame.validBusiness() && !frame.validOutbound() && !frame.validAck()) {
		return 0, ErrInvalidFrame
	}
	size := frameHeaderBytes + len(frame.Payload)
	if size > maxFrameBytes {
		return 0, ErrFrameTooLarge
	}
	return size, nil
}

func encodeFrameHeader(header []byte, frame Frame) {
	header[0] = frameVersion
	header[1] = byte(frame.Kind)
	if frame.Restore {
		header[2] = frameRestoreFlag
	}
	binary.BigEndian.PutUint64(header[4:12], uint64(frame.Seq))
	binary.BigEndian.PutUint64(header[12:20], uint64(frame.Ack))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(frame.Payload)))
}

// DecodeFrame 解码并校验版本化二进制信封。
func DecodeFrame(encoded []byte, maxFrameBytes int) (Frame, error) {
	if maxFrameBytes < frameHeaderBytes || len(encoded) < frameHeaderBytes {
		return Frame{}, ErrInvalidFrame
	}
	if len(encoded) > maxFrameBytes {
		return Frame{}, ErrFrameTooLarge
	}
	if encoded[0] != frameVersion || encoded[3] != 0 || encoded[2]&^frameRestoreFlag != 0 {
		return Frame{}, ErrInvalidFrame
	}
	payloadSize := int(binary.BigEndian.Uint32(encoded[20:24]))
	if payloadSize != len(encoded)-frameHeaderBytes {
		return Frame{}, ErrInvalidFrame
	}
	frame := Frame{
		Kind:    FrameKind(encoded[1]),
		Seq:     int64(binary.BigEndian.Uint64(encoded[4:12])),
		Ack:     int64(binary.BigEndian.Uint64(encoded[12:20])),
		Restore: encoded[2]&frameRestoreFlag != 0,
		Payload: cloneBytes(encoded[frameHeaderBytes:]),
	}
	if (!frame.validHandshake() && !frame.validBusiness() && !frame.validOutbound() && !frame.validAck()) || (frame.Restore && frame.Kind != FrameHandshake) {
		return Frame{}, ErrInvalidFrame
	}
	return frame, nil
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
