// Package wal 定义后端无关的预写日志契约。
package wal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidRecord = errors.New("wal: invalid record")
	ErrConflict      = errors.New("wal: stream position conflict")
	ErrSequenceGap   = errors.New("wal: sequence gap")
	// ErrCheckpointRegression 表示重叠 checkpoint 试图把同一流的位置回退到更早序号。
	ErrCheckpointRegression = errors.New("wal: checkpoint sequence regression")
)

const (
	// MaxPayloadBytes 是单条 WAL 记录允许的最大正文大小。
	MaxPayloadBytes = 4 << 20
	// MaxReadBatch 限制单次恢复读取的记录数，调用方应使用 Seq 分批推进。
	MaxReadBatch = 4096
	// MaxReadBatchBytes 限制一次恢复结果携带的正文总量。
	MaxReadBatchBytes  = 64 << 20
	defaultReplayBatch = 256
)

// Stream 标识一条独立、有序的恢复日志流。后端路由由 Namespace 和 StreamKey 稳定推导，
// Generation 变化不能让同一状态流漂移到其他分片。
type Stream struct {
	// Namespace 区分不同业务日志类型，并参与稳定后端路由。
	Namespace string
	// StreamKey 标识同一 Namespace 下必须保持顺序的业务状态流。
	StreamKey string
	// Generation 区分同一状态流的不同 owner 世代，阻止旧 owner 继续推进新位置。
	Generation uint64
}

// Record 是一次已经编码完成、可独立校验的状态变更。
//
// Payload 在 Append 返回前必须保持只读。WAL 的同步追加边界返回后，调用方可自行复用原缓冲区。
type Record struct {
	// Stream 指定记录所属的有序恢复流。
	Stream Stream
	// Seq 是当前 Generation 内从 1 开始连续递增的序号。
	Seq uint64
	// PayloadVersion 标识业务载荷编码格式，供恢复器选择对应解码逻辑。
	PayloadVersion uint32
	// PayloadHash 是 Payload 的 SHA-256，用于幂等重试和冲突检测。
	PayloadHash [sha256.Size]byte
	// Payload 是追加前已经编码完成的只读业务变更正文。
	Payload []byte
	// CreatedAt 记录业务生成该 WAL 记录的时间，不参与流内排序。
	CreatedAt time.Time
}

// Store 是服务使用的安全 WAL 门面。Replay 负责连续分页恢复；Checkpoint 只会在回调成功后
// 清理已覆盖日志，并在当前进程内串行同一 Stream 的重叠 checkpoint、拒绝并发位置回退。
type Store interface {
	Append(context.Context, Record) error
	Read(context.Context, Stream, uint64, int) ([]Record, error)
	Replay(context.Context, Stream, uint64, func(context.Context, Record) error) (uint64, error)
	Checkpoint(context.Context, Stream, uint64, func(context.Context) error) error
}

// Backend 是扩展 WAL 存储实现的底层契约。普通服务应使用 Store，不直接调用 Trim。
type Backend interface {
	Append(context.Context, Record) error
	Read(context.Context, Stream, uint64, int) ([]Record, error)
	Trim(context.Context, Stream, uint64) error
}

type store struct {
	backend     Backend
	checkpoints checkpointRegistry
}

var _ Store = (*store)(nil)

type streamID struct {
	namespace  string
	streamKey  string
	generation uint64
}

type checkpointRegistry struct {
	mu      sync.Mutex
	entries map[streamID]*checkpointEntry
}

type checkpointEntry struct {
	signal     chan struct{}
	references int
	maximum    uint64
}

// NewStore 用 backend 创建只在 checkpoint 成功后执行 Trim 的安全门面。
func NewStore(backend Backend) (Store, error) {
	if isNil(backend) {
		return nil, fmt.Errorf("wal: backend is not configured")
	}
	return &store{backend: backend}, nil
}

func (current *store) Append(ctx context.Context, record Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	return current.backend.Append(ctx, record)
}

func (current *store) Read(ctx context.Context, stream Stream, afterSeq uint64, limit int) ([]Record, error) {
	if err := validateRead(stream, afterSeq, limit); err != nil {
		return nil, err
	}
	records, err := current.backend.Read(ctx, stream, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	if len(records) > limit {
		return nil, fmt.Errorf("%w: backend returned %d records for limit %d", ErrInvalidRecord, len(records), limit)
	}
	if err := ValidateSequence(stream, afterSeq, records); err != nil {
		return nil, err
	}
	return records, nil
}

// Replay 从 afterSeq 后连续分批读取记录，并按 Seq 顺序交给 apply。
// 返回值只推进到最后一条成功处理的记录；返回错误时，调用方不应开放尚未完整恢复的状态。
func (current *store) Replay(ctx context.Context, stream Stream, afterSeq uint64, apply func(context.Context, Record) error) (uint64, error) {
	if apply == nil {
		return afterSeq, fmt.Errorf("%w: replay callback is nil", ErrInvalidRecord)
	}
	if err := validateRead(stream, afterSeq, defaultReplayBatch); err != nil {
		return afterSeq, err
	}
	lastSeq := afterSeq
	for {
		records, err := current.Read(ctx, stream, lastSeq, defaultReplayBatch)
		if err != nil {
			return lastSeq, err
		}
		if len(records) == 0 {
			return lastSeq, nil
		}
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return lastSeq, err
			}
			if err := apply(ctx, record); err != nil {
				return lastSeq, fmt.Errorf("wal: replay sequence %d: %w", record.Seq, err)
			}
			lastSeq = record.Seq
		}
	}
}

func (current *store) Checkpoint(ctx context.Context, stream Stream, throughSeq uint64, persist func(context.Context) error) error {
	if persist == nil {
		return fmt.Errorf("%w: checkpoint callback is nil", ErrInvalidRecord)
	}
	if err := validateTrim(stream, throughSeq); err != nil {
		return err
	}
	entry, err := current.checkpoints.acquire(ctx, stream)
	if err != nil {
		return err
	}
	defer current.checkpoints.release(stream, entry)
	if throughSeq < entry.maximum {
		return fmt.Errorf("%w: got %d after %d", ErrCheckpointRegression, throughSeq, entry.maximum)
	}
	if err := persist(ctx); err != nil {
		return err
	}
	entry.maximum = throughSeq
	return current.backend.Trim(ctx, stream, throughSeq)
}

// NewRecord 创建一条记录并计算稳定的内容哈希。它不会复制编码载荷。
func NewRecord(stream Stream, seq uint64, payloadVersion uint32, payload []byte) (Record, error) {
	record := Record{
		Stream:         stream,
		Seq:            seq,
		PayloadVersion: payloadVersion,
		Payload:        payload,
		CreatedAt:      time.Now().UTC(),
	}
	if err := record.validateFields(); err != nil {
		return Record{}, err
	}
	record.PayloadHash = sha256.Sum256(payload)
	return record, nil
}

// Validate 校验记录能否被所有当前 WAL backend 稳定排序与恢复。
func (record Record) Validate() error {
	if err := record.validateFields(); err != nil {
		return err
	}
	hash := sha256.Sum256(record.Payload)
	if hash != record.PayloadHash {
		return fmt.Errorf("%w: payload hash mismatch", ErrInvalidRecord)
	}
	return nil
}

func (record Record) validateFields() error {
	if err := record.Stream.Validate(); err != nil {
		return err
	}
	if record.Seq == 0 || record.Seq > math.MaxInt64 {
		return fmt.Errorf("%w: sequence must be between 1 and %d", ErrInvalidRecord, int64(math.MaxInt64))
	}
	if record.PayloadVersion == 0 {
		return fmt.Errorf("%w: payload version must be greater than 0", ErrInvalidRecord)
	}
	if len(record.Payload) == 0 {
		return fmt.Errorf("%w: payload is empty", ErrInvalidRecord)
	}
	if len(record.Payload) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload size %d exceeds %d bytes", ErrInvalidRecord, len(record.Payload), MaxPayloadBytes)
	}
	if record.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created time is zero", ErrInvalidRecord)
	}
	return nil
}

// Validate 校验恢复流身份。
func (stream Stream) Validate() error {
	if strings.TrimSpace(stream.Namespace) == "" {
		return fmt.Errorf("%w: namespace is empty", ErrInvalidRecord)
	}
	if strings.TrimSpace(stream.StreamKey) == "" {
		return fmt.Errorf("%w: stream key is empty", ErrInvalidRecord)
	}
	if stream.Generation == 0 || stream.Generation > math.MaxInt64 {
		return fmt.Errorf("%w: generation must be between 1 and %d", ErrInvalidRecord, int64(math.MaxInt64))
	}
	return nil
}

// ValidateSequence 确认恢复结果属于同一条流，并从 afterSeq 后严格连续。
func ValidateSequence(stream Stream, afterSeq uint64, records []Record) error {
	if err := validateRead(stream, afterSeq, 1); err != nil {
		return err
	}
	if len(records) > MaxReadBatch {
		return fmt.Errorf("%w: record count %d exceeds %d", ErrInvalidRecord, len(records), MaxReadBatch)
	}
	want := afterSeq + 1
	payloadBytes := 0
	for i, record := range records {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("wal: validate record at index %d: %w", i, err)
		}
		if record.Stream.Namespace != stream.Namespace || record.Stream.StreamKey != stream.StreamKey || record.Stream.Generation != stream.Generation {
			return fmt.Errorf("%w: record at index %d belongs to another stream", ErrInvalidRecord, i)
		}
		if record.Seq != want {
			return fmt.Errorf("%w: got sequence %d, want %d", ErrSequenceGap, record.Seq, want)
		}
		if len(record.Payload) > MaxReadBatchBytes-payloadBytes {
			return fmt.Errorf("%w: recovery payload exceeds %d bytes", ErrInvalidRecord, MaxReadBatchBytes)
		}
		payloadBytes += len(record.Payload)
		want++
	}
	return nil
}

func (registry *checkpointRegistry) acquire(ctx context.Context, stream Stream) (*checkpointEntry, error) {
	id := streamID{namespace: stream.Namespace, streamKey: stream.StreamKey, generation: stream.Generation}
	registry.mu.Lock()
	if registry.entries == nil {
		registry.entries = make(map[streamID]*checkpointEntry)
	}
	entry := registry.entries[id]
	if entry == nil {
		entry = &checkpointEntry{signal: make(chan struct{}, 1)}
		entry.signal <- struct{}{}
		registry.entries[id] = entry
	}
	entry.references++
	registry.mu.Unlock()
	select {
	case <-ctx.Done():
		registry.releaseReference(id, entry)
		return nil, ctx.Err()
	case <-entry.signal:
		return entry, nil
	}
}

func (registry *checkpointRegistry) release(stream Stream, entry *checkpointEntry) {
	entry.signal <- struct{}{}
	registry.releaseReference(streamID{namespace: stream.Namespace, streamKey: stream.StreamKey, generation: stream.Generation}, entry)
}

func (registry *checkpointRegistry) releaseReference(id streamID, entry *checkpointEntry) {
	registry.mu.Lock()
	entry.references--
	if entry.references == 0 && registry.entries[id] == entry {
		delete(registry.entries, id)
	}
	registry.mu.Unlock()
}

func validateRead(stream Stream, afterSeq uint64, limit int) error {
	if err := stream.Validate(); err != nil {
		return err
	}
	if afterSeq > math.MaxInt64 {
		return fmt.Errorf("%w: sequence %d exceeds the supported range", ErrInvalidRecord, afterSeq)
	}
	if limit <= 0 || limit > MaxReadBatch {
		return fmt.Errorf("%w: read limit must be between 1 and %d", ErrInvalidRecord, MaxReadBatch)
	}
	return nil
}

func validateTrim(stream Stream, throughSeq uint64) error {
	if err := stream.Validate(); err != nil {
		return err
	}
	if throughSeq == 0 || throughSeq > math.MaxInt64 {
		return fmt.Errorf("%w: trim sequence must be between 1 and %d", ErrInvalidRecord, int64(math.MaxInt64))
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
