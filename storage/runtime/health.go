package runtime

import (
	"context"
	"errors"
	"fmt"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
	txfusion "github.com/ngnl47/tx/storage/fusion"
	txmongo "github.com/ngnl47/tx/storage/mongo"
	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/wal"
)

// WriteAdmission 是有状态缓存路径使用的写入准入适配器。
// 它按事务目标声明的存储模式检查健康状态，不向普通业务存储 API 暴露闸门细节。
type WriteAdmission struct {
	storage *Runtime
}

// NewWriteAdmission 创建按事务目标存储模式检查健康状态的写入闸门。
func NewWriteAdmission(storage *Runtime) WriteAdmission {
	return WriteAdmission{storage: storage}
}

// RequireWritable 检查本次事务涉及的全部持久化模式。
// 混合 Redis/Mongo 目标与 Fusion 目标都要求两个后端同时可写。
func (admission WriteAdmission) RequireWritable(targets []txn.LockTarget) error {
	if admission.storage == nil {
		return ErrNotConfigured
	}
	var redisRequired bool
	var mongoRequired bool
	for _, target := range targets {
		store, ok := target.Store.(txstorage.ChangeStore)
		if !ok || isNil(store) {
			return fmt.Errorf("storage runtime: write admission target %s is not a persistent Store", target.Key.String())
		}
		switch mode := store.TypeSpec().Storage.Mode; mode {
		case meta.StorageModeRedis:
			redisRequired = true
		case meta.StorageModeMongo:
			mongoRequired = true
		case meta.StorageModeFusion:
			redisRequired = true
			mongoRequired = true
		default:
			return fmt.Errorf("storage runtime: write admission target %s has unsupported storage mode %q", target.Key.String(), mode)
		}
	}
	switch {
	case redisRequired && mongoRequired:
		var redisErr error
		var mongoErr error
		if admission.storage.redis == nil {
			redisErr = unavailableBackendError(txstorage.BackendRedis, ErrNotConfigured)
		}
		if admission.storage.mongo == nil {
			mongoErr = unavailableBackendError(txstorage.BackendMongo, ErrNotConfigured)
		}
		if err := errors.Join(redisErr, mongoErr); err != nil {
			return err
		}
		return admission.storage.requireStorageMode(meta.StorageModeFusion)
	case redisRequired:
		if admission.storage.redis == nil {
			return unavailableBackendError(txstorage.BackendRedis, ErrNotConfigured)
		}
		return admission.storage.requireStorageMode(meta.StorageModeRedis)
	case mongoRequired:
		if admission.storage.mongo == nil {
			return unavailableBackendError(txstorage.BackendMongo, ErrNotConfigured)
		}
		return admission.storage.requireStorageMode(meta.StorageModeMongo)
	default:
		return nil
	}
}

type healthAwareWAL struct {
	storage *Runtime
	next    wal.Store
}

func (store healthAwareWAL) Append(ctx context.Context, record wal.Record) error {
	if err := store.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return err
	}
	err := store.next.Append(ctx, record)
	store.storage.observeStorageError(err)
	return err
}

func (store healthAwareWAL) Read(ctx context.Context, stream wal.Stream, afterSeq uint64, limit int) ([]wal.Record, error) {
	if err := store.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return nil, err
	}
	records, err := store.next.Read(ctx, stream, afterSeq, limit)
	store.storage.observeStorageError(err)
	return records, err
}

func (store healthAwareWAL) Replay(ctx context.Context, stream wal.Stream, afterSeq uint64, apply func(context.Context, wal.Record) error) (uint64, error) {
	if err := store.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return afterSeq, err
	}
	lastSeq, err := store.next.Replay(ctx, stream, afterSeq, apply)
	store.storage.observeStorageError(err)
	return lastSeq, err
}

func (store healthAwareWAL) Checkpoint(ctx context.Context, stream wal.Stream, throughSeq uint64, persist func(context.Context) error) error {
	if err := store.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return err
	}
	err := store.next.Checkpoint(ctx, stream, throughSeq, persist)
	store.storage.observeStorageError(err)
	return err
}

type healthAwareDelayHandler struct {
	storage *Runtime
	next    txfusion.DelayTaskHandler
}

func (handler healthAwareDelayHandler) HandleDelayTask(ctx context.Context, item task.DelayTask) error {
	if handler.next == nil {
		return ErrWorkerNotConfigured
	}
	err := handler.next.HandleDelayTask(ctx, item)
	handler.storage.observeStorageError(err)
	return err
}

type guardedReader struct {
	storage *Runtime
	mode    meta.StorageMode
	next    txstorage.DocumentLoader
}

func (reader guardedReader) LoadDocument(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if err := reader.storage.requireStorageMode(reader.mode); err != nil {
		return nil, false, err
	}
	document, exists, err := reader.next.LoadDocument(ctx, spec, key, fields...)
	reader.storage.observeStorageError(err)
	return document, exists, err
}

type guardedBatchReader struct {
	guardedReader
	batch txstorage.BatchDocumentLoader
}

type guardedMongoBackfiller struct {
	storage *Runtime
	next    txstorage.DocumentBackfiller
}

func (backfiller guardedMongoBackfiller) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key txstorage.RecordKey, doc map[string]any, options txstorage.ApplyOptions) (bool, error) {
	if err := backfiller.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return false, err
	}
	inserted, err := backfiller.next.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
	backfiller.storage.observeStorageError(err)
	return inserted, err
}

type guardedMongoSequences struct {
	storage *Runtime
	next    txmongo.SequenceAllocator
}

func (sequences guardedMongoSequences) NextSequence(ctx context.Context, routeType string, shardKey any, name string) (int64, error) {
	if err := sequences.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return 0, err
	}
	value, err := sequences.next.NextSequence(ctx, routeType, shardKey, name)
	sequences.storage.observeStorageError(err)
	return value, err
}

func (reader guardedBatchReader) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []txstorage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	if err := reader.storage.requireStorageMode(reader.mode); err != nil {
		return nil, nil, err
	}
	documents, exists, err := reader.batch.LoadDocuments(ctx, spec, keys, fields...)
	reader.storage.observeStorageError(err)
	return documents, exists, err
}

func (s *Runtime) guardReader(mode meta.StorageMode, next txstorage.DocumentLoader) txstorage.DocumentLoader {
	if s == nil || next == nil {
		return nil
	}
	reader := guardedReader{storage: s, mode: mode, next: next}
	if batch, ok := next.(txstorage.BatchDocumentLoader); ok {
		return guardedBatchReader{guardedReader: reader, batch: batch}
	}
	return reader
}

func (s *Runtime) requireStorageMode(mode meta.StorageMode) error {
	if s == nil {
		return ErrNotConfigured
	}
	s.healthMu.RLock()
	redisErr := s.redisHealthErr
	mongoErr := s.mongoHealthErr
	s.healthMu.RUnlock()

	switch mode {
	case meta.StorageModeRedis:
		return unavailableBackendError(txstorage.BackendRedis, redisErr)
	case meta.StorageModeMongo:
		return unavailableBackendError(txstorage.BackendMongo, mongoErr)
	case meta.StorageModeFusion:
		return errors.Join(
			unavailableBackendError(txstorage.BackendRedis, redisErr),
			unavailableBackendError(txstorage.BackendMongo, mongoErr),
		)
	default:
		return fmt.Errorf("storage runtime: unsupported storage mode %q", mode)
	}
}

func unavailableBackendError(backend txstorage.Backend, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %v", ErrStorageUnavailable, backend, err)
}

func (s *Runtime) observeStorageError(err error) {
	if s == nil || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	backend, ok := txstorage.BackendFromError(err)
	if !ok {
		return
	}
	s.markBackendUnavailable(backend, err)
}

func (s *Runtime) markBackendUnavailable(backend txstorage.Backend, err error) {
	if s == nil || err == nil {
		return
	}
	s.healthMu.Lock()
	var previous error
	switch backend {
	case txstorage.BackendRedis:
		previous = s.redisHealthErr
		s.redisHealthErr = err
		s.redisHealthGen++
	case txstorage.BackendMongo:
		previous = s.mongoHealthErr
		s.mongoHealthErr = err
		s.mongoHealthGen++
	default:
		s.healthMu.Unlock()
		return
	}
	s.healthMu.Unlock()

	if previous == nil {
		txlog.Warn("storage backend became unavailable", "backend", backend.String(), "err", err)
	}
}

func (s *Runtime) backendHealthGeneration(backend txstorage.Backend) uint64 {
	if s == nil {
		return 0
	}
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	switch backend {
	case txstorage.BackendRedis:
		return s.redisHealthGen
	case txstorage.BackendMongo:
		return s.mongoHealthGen
	default:
		return 0
	}
}

func (s *Runtime) recoverBackendHealth(backend txstorage.Backend, observedGeneration uint64) {
	if s == nil {
		return
	}
	s.healthMu.Lock()
	var previous error
	switch backend {
	case txstorage.BackendRedis:
		if s.redisHealthGen != observedGeneration {
			s.healthMu.Unlock()
			return
		}
		previous = s.redisHealthErr
		s.redisHealthErr = nil
	case txstorage.BackendMongo:
		if s.mongoHealthGen != observedGeneration {
			s.healthMu.Unlock()
			return
		}
		previous = s.mongoHealthErr
		s.mongoHealthErr = nil
	default:
		s.healthMu.Unlock()
		return
	}
	s.healthMu.Unlock()
	if previous != nil {
		txlog.Info("storage backend recovered", "backend", backend.String())
	}
}
