package protocol

import (
	"encoding/binary"
	"errors"
)

// PackCommand 将命令哈希与不透明正文组合为 MsgQ 可保存的消息；线上帧由 Agent 拆分写入。
func PackCommand(hash uint32, body []byte) ([]byte, error) {
	if hash == 0 {
		return nil, errors.New("protocol: zero command hash")
	}
	data := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(data, hash)
	copy(data[4:], body)
	return data, nil
}

// UnpackCommand 只解析消息头，返回的正文引用原始数据且不得修改。
func UnpackCommand(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, errors.New("protocol: truncated command header")
	}
	hash := binary.BigEndian.Uint32(data)
	if hash == 0 {
		return 0, nil, errors.New("protocol: zero command hash")
	}
	return hash, data[4:], nil
}
