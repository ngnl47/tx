package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

type taskScalarKind string

const (
	taskScalarString taskScalarKind = "string"
	taskScalarInt    taskScalarKind = "int"
	taskScalarInt8   taskScalarKind = "int8"
	taskScalarInt16  taskScalarKind = "int16"
	taskScalarInt32  taskScalarKind = "int32"
	taskScalarInt64  taskScalarKind = "int64"
	taskScalarUint   taskScalarKind = "uint"
	taskScalarUint8  taskScalarKind = "uint8"
	taskScalarUint16 taskScalarKind = "uint16"
	taskScalarUint32 taskScalarKind = "uint32"
	taskScalarUint64 taskScalarKind = "uint64"
)

type taskScalarJSON struct {
	Kind  taskScalarKind `json:"kind"`
	Value string         `json:"value"`
}

type delayTaskJSON struct {
	Type      string          `json:"type"`
	ID        *taskScalarJSON `json:"id,omitempty"`
	ShardKey  *taskScalarJSON `json:"shardKey,omitempty"`
	KeySuffix string          `json:"keySuffix,omitempty"`
	Action    string          `json:"action"`
	DueAt     time.Time       `json:"dueAt"`
}

// MarshalJSON 为持久化任务记录整数的精确 Go 类型，避免 any 经 JSON
// 往返后统一退化为 float64 或 int64。
func (t DelayTask) MarshalJSON() ([]byte, error) {
	id, err := encodeTaskScalar(t.ID)
	if err != nil {
		return nil, fmt.Errorf("task: encode id: %w", err)
	}
	shardKey, err := encodeTaskScalar(t.ShardKey)
	if err != nil {
		return nil, fmt.Errorf("task: encode shard key: %w", err)
	}
	return json.Marshal(delayTaskJSON{
		Type:      t.Type,
		ID:        id,
		ShardKey:  shardKey,
		KeySuffix: t.KeySuffix,
		Action:    t.Action,
		DueAt:     t.DueAt,
	})
}

// UnmarshalJSON 恢复持久化任务中记录的精确 string/整数类型。
func (t *DelayTask) UnmarshalJSON(data []byte) error {
	if t == nil {
		return fmt.Errorf("task: decode into nil DelayTask")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire delayTaskJSON
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("task: multiple JSON values")
		}
		return fmt.Errorf("task: invalid trailing JSON: %w", err)
	}
	id, err := decodeTaskScalar(wire.ID)
	if err != nil {
		return fmt.Errorf("task: decode id: %w", err)
	}
	shardKey, err := decodeTaskScalar(wire.ShardKey)
	if err != nil {
		return fmt.Errorf("task: decode shard key: %w", err)
	}
	*t = DelayTask{
		Type:      wire.Type,
		ID:        id,
		ShardKey:  shardKey,
		KeySuffix: wire.KeySuffix,
		Action:    wire.Action,
		DueAt:     wire.DueAt,
	}
	return nil
}

func validateTaskScalar(value any) error {
	switch value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return nil
	default:
		return fmt.Errorf("unsupported scalar type %T", value)
	}
}

func taskScalarIdentity(value any) string {
	encoded, err := encodeTaskScalar(value)
	if err != nil {
		return fmt.Sprintf("%T:%v", value, value)
	}
	if encoded == nil {
		return "nil"
	}
	return string(encoded.Kind) + ":" + encoded.Value
}

func encodeTaskScalar(value any) (*taskScalarJSON, error) {
	if isNilTaskID(value) {
		return nil, nil
	}
	encoded := &taskScalarJSON{}
	switch value := value.(type) {
	case string:
		encoded.Kind, encoded.Value = taskScalarString, value
	case int:
		encoded.Kind, encoded.Value = taskScalarInt, strconv.FormatInt(int64(value), 10)
	case int8:
		encoded.Kind, encoded.Value = taskScalarInt8, strconv.FormatInt(int64(value), 10)
	case int16:
		encoded.Kind, encoded.Value = taskScalarInt16, strconv.FormatInt(int64(value), 10)
	case int32:
		encoded.Kind, encoded.Value = taskScalarInt32, strconv.FormatInt(int64(value), 10)
	case int64:
		encoded.Kind, encoded.Value = taskScalarInt64, strconv.FormatInt(value, 10)
	case uint:
		encoded.Kind, encoded.Value = taskScalarUint, strconv.FormatUint(uint64(value), 10)
	case uint8:
		encoded.Kind, encoded.Value = taskScalarUint8, strconv.FormatUint(uint64(value), 10)
	case uint16:
		encoded.Kind, encoded.Value = taskScalarUint16, strconv.FormatUint(uint64(value), 10)
	case uint32:
		encoded.Kind, encoded.Value = taskScalarUint32, strconv.FormatUint(uint64(value), 10)
	case uint64:
		encoded.Kind, encoded.Value = taskScalarUint64, strconv.FormatUint(value, 10)
	default:
		return nil, fmt.Errorf("unsupported scalar type %T", value)
	}
	return encoded, nil
}

func decodeTaskScalar(encoded *taskScalarJSON) (any, error) {
	if encoded == nil {
		return nil, nil
	}
	switch encoded.Kind {
	case taskScalarString:
		return encoded.Value, nil
	case taskScalarInt:
		value, err := strconv.ParseInt(encoded.Value, 10, strconv.IntSize)
		return int(value), err
	case taskScalarInt8:
		value, err := strconv.ParseInt(encoded.Value, 10, 8)
		return int8(value), err
	case taskScalarInt16:
		value, err := strconv.ParseInt(encoded.Value, 10, 16)
		return int16(value), err
	case taskScalarInt32:
		value, err := strconv.ParseInt(encoded.Value, 10, 32)
		return int32(value), err
	case taskScalarInt64:
		value, err := strconv.ParseInt(encoded.Value, 10, 64)
		return value, err
	case taskScalarUint:
		value, err := strconv.ParseUint(encoded.Value, 10, strconv.IntSize)
		return uint(value), err
	case taskScalarUint8:
		value, err := strconv.ParseUint(encoded.Value, 10, 8)
		return uint8(value), err
	case taskScalarUint16:
		value, err := strconv.ParseUint(encoded.Value, 10, 16)
		return uint16(value), err
	case taskScalarUint32:
		value, err := strconv.ParseUint(encoded.Value, 10, 32)
		return uint32(value), err
	case taskScalarUint64:
		value, err := strconv.ParseUint(encoded.Value, 10, 64)
		return value, err
	default:
		return nil, fmt.Errorf("unsupported scalar kind %q", encoded.Kind)
	}
}
