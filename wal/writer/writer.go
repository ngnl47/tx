// Package writer 提供 WAL 记录与持久化检查点之间的显式写入适配器。
package writer

import (
	"context"
	"errors"
	"reflect"

	"github.com/ngnl47/tx/wal"
	walcheckpoint "github.com/ngnl47/tx/wal/checkpoint"
)

// CheckpointSubmitter 接收不携带正文的异步持久化检查点请求。
type CheckpointSubmitter interface {
	Submit(walcheckpoint.Request) error
}

// Sync 在 WAL 追加成功后同步执行目标持久化，并在成功后推进 WAL checkpoint。
type Sync struct {
	log wal.Store
}

// NewSync 创建同步 WAL Writer。
func NewSync(log wal.Store) (*Sync, error) {
	if isNil(log) {
		return nil, errors.New("wal writer: store is not configured")
	}
	return &Sync{log: log}, nil
}

// Write 执行 Append -> persist -> WAL checkpoint。persist 失败时保留 WAL 记录。
func (writer *Sync) Write(ctx context.Context, record wal.Record, persist func(context.Context) error) error {
	if writer == nil || isNil(writer.log) {
		return errors.New("wal writer: store is not configured")
	}
	if persist == nil {
		return errors.New("wal writer: persist callback is nil")
	}
	if err := writer.log.Append(ctx, record); err != nil {
		return err
	}
	return writer.log.Checkpoint(ctx, record.Stream, record.Seq, persist)
}

// Async 在 WAL 追加成功后只登记异步持久化检查点，不复制记录正文。
type Async struct {
	log         wal.Store
	checkpoints CheckpointSubmitter
}

// NewAsync 创建异步 WAL Writer。
func NewAsync(log wal.Store, checkpoints CheckpointSubmitter) (*Async, error) {
	if isNil(log) {
		return nil, errors.New("wal writer: store is not configured")
	}
	if isNil(checkpoints) {
		return nil, errors.New("wal writer: checkpoint submitter is not configured")
	}
	return &Async{log: log, checkpoints: checkpoints}, nil
}

// Write 执行 Append -> Submit。Submit 失败时返回错误，但已经追加的 WAL 记录仍保留，
// 调用方不得把对应 Store 变更清理为成功状态。
func (writer *Async) Write(ctx context.Context, record wal.Record) error {
	if writer == nil || isNil(writer.log) || isNil(writer.checkpoints) {
		return errors.New("wal writer: dependencies are not configured")
	}
	if err := writer.log.Append(ctx, record); err != nil {
		return err
	}
	return writer.checkpoints.Submit(walcheckpoint.Request{
		Stream:     record.Stream,
		ThroughSeq: record.Seq,
	})
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
