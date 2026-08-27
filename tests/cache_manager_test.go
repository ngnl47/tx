package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cachemodule "github.com/ngnl47/tx/modules/cache"
	"github.com/ngnl47/tx/modules/txn"
)

func TestCacheManagerConfigValidateRequiresLifecycleDurations(t *testing.T) {
	tests := []struct {
		name   string
		config cachemodule.Config
		want   string
	}{
		{name: "flush interval", config: cachemodule.Config{ShutdownFlushTimeout: time.Second}, want: "flush interval"},
		{name: "shutdown timeout", config: cachemodule.Config{FlushInterval: time.Second}, want: "shutdown flush timeout"},
		{name: "pending roots", config: cachemodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, MaxPendingRoots: -1}, want: "pending root"},
		{name: "root changes", config: cachemodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, MaxRootChanges: -1}, want: "root change"},
		{name: "heap interval", config: cachemodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, HeapSampleInterval: -1}, want: "heap sample interval"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want %q", err, test.want)
			}
		})
	}
	valid := cachemodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid Config.Validate returned error: %v", err)
	}
}

func TestCacheManagerRegistersCommittedRootAndFlushesChanges(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: writer}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)

	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if manager.Pending() != 1 || writer.hasOptions {
		t.Fatalf("commit state = pending:%d wrote:%t, want one deferred root", manager.Pending(), writer.hasOptions)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if manager.Pending() != 0 || !writer.hasOptions || store.HasChanges() {
		t.Fatalf("flush state = pending:%d wrote:%t changes:%t", manager.Pending(), writer.hasOptions, store.HasChanges())
	}
}

func TestCacheManagerRejectsDifferentStoreForRetainedIdentity(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	key := txn.NewLockKey("role", 1001)
	firstLock := txn.NewLockTarget(key, first)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: firstLock, store: first}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	called := false
	secondLock := txn.NewLockTarget(key, second)
	err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: secondLock, store: second},
		called:               &called,
	})
	if !errors.Is(err, cachemodule.ErrIdentityConflict) {
		t.Fatalf("second Call error = %v, want ErrIdentityConflict", err)
	}
	if called || second.HasChanges() {
		t.Fatalf("conflicting Procedure executed: called=%t changes=%t", called, second.HasChanges())
	}
}

func TestCacheManagerAbortReleasesUncommittedIdentity(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	key := txn.NewLockKey("role", 1001)
	err := runtime.Call(context.Background(), commitParticipantProcedure{
		lock:       txn.NewLockTarget(key, first),
		store:      first,
		processErr: errors.New("rollback"),
	})
	if err == nil {
		t.Fatal("first Call returned nil error")
	}

	if err := runtime.Call(context.Background(), runtimeTestProcedure{
		lock:  txn.NewLockTarget(key, second),
		store: second,
	}); err != nil {
		t.Fatalf("second Call after rollback returned error: %v", err)
	}
}

func TestCacheManagerReleaseRequiresCleanRoot(t *testing.T) {
	store := &runtimeTestStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if err := manager.Release(lock); !errors.Is(err, cachemodule.ErrPendingRoot) {
		t.Fatalf("Release dirty root error = %v, want ErrPendingRoot", err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if err := manager.Release(lock); err != nil {
		t.Fatalf("Release clean root returned error: %v", err)
	}
	second := &runtimeTestStore{}
	secondLock := txn.NewLockTarget(lock.Key, second)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: secondLock, store: second}); err != nil {
		t.Fatalf("Call with reloaded Store after Release returned error: %v", err)
	}
}

func TestCacheManagerReleaseWaitsForActiveProcedure(t *testing.T) {
	store := &runtimeTestStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("initial Call returned error: %v", err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("initial Flush returned error: %v", err)
	}

	started := make(chan struct{})
	releaseProcedure := make(chan struct{})
	procedureDone := make(chan error, 1)
	go func() {
		procedureDone <- runtime.Call(context.Background(), blockingCacheProcedure{
			lock:    lock,
			store:   store,
			started: started,
			release: releaseProcedure,
		})
	}()
	<-started

	releaseDone := make(chan error, 1)
	go func() { releaseDone <- manager.Release(lock) }()
	select {
	case err := <-releaseDone:
		t.Fatalf("Release returned while Procedure still held entity lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseProcedure)
	if err := <-procedureDone; err != nil {
		t.Fatalf("blocking Procedure returned error: %v", err)
	}
	if err := <-releaseDone; !errors.Is(err, cachemodule.ErrPendingRoot) {
		t.Fatalf("Release after Procedure error = %v, want ErrPendingRoot", err)
	}
}

type blockingCacheProcedure struct {
	lock    txn.LockTarget
	store   *runtimeTestStore
	started chan<- struct{}
	release <-chan struct{}
}

func (procedure blockingCacheProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure blockingCacheProcedure) Process(*txn.Tx) error {
	procedure.store.Set()
	close(procedure.started)
	<-procedure.release
	return nil
}

func TestCacheManagerReleaseRejectsInvalidTarget(t *testing.T) {
	manager := cachemodule.NewManager(cachemodule.Dependencies{}, cachemodule.Config{})
	if err := manager.Release(txn.LockTarget{}); !errors.Is(err, cachemodule.ErrInvalidTarget) {
		t.Fatalf("Release invalid target error = %v, want ErrInvalidTarget", err)
	}
}

func TestCacheManagerKeepsFailedFlushPending(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{err: errors.New("storage unavailable")}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: writer}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}

	if err := manager.Flush(context.Background()); err == nil {
		t.Fatal("Flush returned nil error")
	}
	if manager.Pending() != 1 || !store.HasChanges() {
		t.Fatalf("failed flush state = pending:%d changes:%t, want retained", manager.Pending(), store.HasChanges())
	}
}

func TestCacheManagerIgnoresPureMemoryTransactionTarget(t *testing.T) {
	store := &runtimeMemoryStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("memory", 1001), store)
	if err := runtime.Call(context.Background(), commitParticipantProcedure{
		lock:  lock,
		store: &store.runtimeTestStore,
	}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if manager.Pending() != 0 {
		t.Fatalf("Pending = %d, want pure memory target ignored", manager.Pending())
	}
}

func BenchmarkTxnRuntimeCommitToCacheManager(b *testing.B) {
	store := &runtimeTestStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		b.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	procedure := runtimeTestProcedure{
		lock:  txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
		store: store,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := runtime.Call(ctx, procedure); err != nil {
			b.Fatal(err)
		}
		// 模拟同一常驻根对象在两次刷盘周期之间仅保留当前一轮变更，
		// 避免测试 Store 的历史切片增长掩盖事务与缓存提交边界的成本。
		store.ClearChanges()
	}
}

func BenchmarkTxnRuntimeWithoutCommitParticipant(b *testing.B) {
	store := &runtimeTestStore{}
	runtime := txn.NewRuntime()
	procedure := runtimeTestProcedure{
		lock:  txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
		store: store,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := runtime.Call(ctx, procedure); err != nil {
			b.Fatal(err)
		}
		store.ClearChanges()
	}
}
