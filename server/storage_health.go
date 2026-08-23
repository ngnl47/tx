package server

import (
	"context"
	"errors"
	"fmt"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	txstorage "github.com/ngnl47/tx/storage"
	txfusion "github.com/ngnl47/tx/storage/fusion"
	"github.com/ngnl47/tx/storage/task"
	"github.com/ngnl47/tx/wal"
)

type healthAwareWAL struct {
	storage *storageRuntime
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

func (store healthAwareWAL) Checkpoint(ctx context.Context, stream wal.Stream, throughSeq uint64, persist func(context.Context) error) error {
	if err := store.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return err
	}
	err := store.next.Checkpoint(ctx, stream, throughSeq, persist)
	store.storage.observeStorageError(err)
	return err
}

type healthAwareDelayHandler struct {
	storage *storageRuntime
	next    txfusion.DelayTaskHandler
}

func (handler healthAwareDelayHandler) HandleDelayTask(ctx context.Context, item task.DelayTask) error {
	if handler.next == nil {
		return errWorkerNotConfigured
	}
	err := handler.next.HandleDelayTask(ctx, item)
	handler.storage.observeStorageError(err)
	return err
}

type guardedReader struct {
	storage *storageRuntime
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

func (reader guardedBatchReader) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []txstorage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	if err := reader.storage.requireStorageMode(reader.mode); err != nil {
		return nil, nil, err
	}
	documents, exists, err := reader.batch.LoadDocuments(ctx, spec, keys, fields...)
	reader.storage.observeStorageError(err)
	return documents, exists, err
}

func (s *storageRuntime) guardReader(mode meta.StorageMode, next txstorage.DocumentLoader) txstorage.DocumentLoader {
	if s == nil || next == nil {
		return nil
	}
	reader := guardedReader{storage: s, mode: mode, next: next}
	if batch, ok := next.(txstorage.BatchDocumentLoader); ok {
		return guardedBatchReader{guardedReader: reader, batch: batch}
	}
	return reader
}

func (s *storageRuntime) requireStorageMode(mode meta.StorageMode) error {
	if s == nil {
		return errStorageNotConfigured
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
		return fmt.Errorf("server: unsupported storage mode %q", mode)
	}
}

func unavailableBackendError(backend txstorage.Backend, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %v", ErrStorageUnavailable, backend, err)
}

func (s *storageRuntime) observeStorageError(err error) {
	if s == nil || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	backend, ok := txstorage.BackendFromError(err)
	if !ok {
		return
	}
	s.markBackendUnavailable(backend, err)
}

func (s *storageRuntime) markBackendUnavailable(backend txstorage.Backend, err error) {
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

func (s *storageRuntime) backendHealthGeneration(backend txstorage.Backend) uint64 {
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

func (s *storageRuntime) recoverBackendHealth(backend txstorage.Backend, observedGeneration uint64) {
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
