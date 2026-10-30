package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/txn"
	writebackmodule "github.com/ngnl47/tx/modules/writeback"
)

func TestWriteBackManagerConfigValidateRequiresLifecycleDurations(t *testing.T) {
	tests := []struct {
		name   string
		config writebackmodule.Config
		want   string
	}{
		{name: "flush interval", config: writebackmodule.Config{ShutdownFlushTimeout: time.Second}, want: "flush interval"},
		{name: "shutdown timeout", config: writebackmodule.Config{FlushInterval: time.Second}, want: "shutdown flush timeout"},
		{name: "pending roots", config: writebackmodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, MaxPendingRoots: -1}, want: "pending root"},
		{name: "root changes", config: writebackmodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, MaxRootChanges: -1}, want: "root change"},
		{name: "heap interval", config: writebackmodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second, HeapSampleInterval: -1}, want: "heap sample interval"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want %q", err, test.want)
			}
		})
	}
	valid := writebackmodule.Config{FlushInterval: time.Second, ShutdownFlushTimeout: time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid Config.Validate returned error: %v", err)
	}
}

func TestWriteBackManagerRegistersCommittedRootAndFlushesChanges(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: writer}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)

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

func TestWriteBackManagerRejectsDifferentStoreForRetainedIdentity(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	key := txn.NewLockey("role", 0, 1001)
	firstLock := txn.NewStoreTarget(key, first)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: firstLock, store: first}); err != nil {
		t.Fatalf("first Call returned error: %v", err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	called := false
	secondLock := txn.NewStoreTarget(key, second)
	err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: secondLock, store: second},
		called:               &called,
	})
	if !errors.Is(err, writebackmodule.ErrIdentityConflict) {
		t.Fatalf("second Call error = %v, want ErrIdentityConflict", err)
	}
	if called || second.HasChanges() {
		t.Fatalf("conflicting Procedure executed: called=%t changes=%t", called, second.HasChanges())
	}
}

func TestWriteBackManagerAbortReleasesUncommittedIdentity(t *testing.T) {
	first := &runtimeTestStore{}
	second := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	key := txn.NewLockey("role", 0, 1001)
	err := runtime.Call(context.Background(), commitParticipantProcedure{
		lock:       txn.NewStoreTarget(key, first),
		store:      first,
		processErr: errors.New("rollback"),
	})
	if err == nil {
		t.Fatal("first Call returned nil error")
	}

	if err := runtime.Call(context.Background(), runtimeTestProcedure{
		lock:  txn.NewStoreTarget(key, second),
		store: second,
	}); err != nil {
		t.Fatalf("second Call after rollback returned error: %v", err)
	}
}

func TestWriteBackManagerTryReleaseRequiresCleanRoot(t *testing.T) {
	store := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: lock, store: store}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if released, err := manager.TryRelease(context.Background(), lock); err != nil || released {
		t.Fatalf("TryRelease pending root = (%t, %v), want (false, nil)", released, err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	store.Set()
	if released, err := manager.TryRelease(context.Background(), lock); err != nil || released {
		t.Fatalf("TryRelease changed Store = (%t, %v), want (false, nil)", released, err)
	}
	store.ClearChanges()
	if released, err := manager.TryRelease(context.Background(), lock); err != nil || !released {
		t.Fatalf("TryRelease clean root = (%t, %v), want (true, nil)", released, err)
	}
	second := &runtimeTestStore{}
	secondLock := txn.NewStoreTarget(lock.Lock, second)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: secondLock, store: second}); err != nil {
		t.Fatalf("Call with reloaded Store after TryRelease returned error: %v", err)
	}
}

func TestWriteBackManagerTryReleaseWaitsForActiveProcedure(t *testing.T) {
	store := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
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
		procedureDone <- runtime.Call(context.Background(), blockingWriteBackProcedure{
			lock:    lock,
			store:   store,
			started: started,
			release: releaseProcedure,
		})
	}()
	<-started

	type releaseResult struct {
		released bool
		err      error
	}
	releaseDone := make(chan releaseResult, 1)
	releaseCtx, cancelRelease := context.WithCancel(context.Background())
	defer cancelRelease()
	go func() {
		released, err := manager.TryRelease(releaseCtx, lock)
		releaseDone <- releaseResult{released: released, err: err}
	}()
	select {
	case result := <-releaseDone:
		t.Fatalf("TryRelease returned while Procedure still held entity lock: (%t, %v)", result.released, result.err)
	case <-time.After(20 * time.Millisecond):
	}
	cancelRelease()
	select {
	case result := <-releaseDone:
		if result.released || !errors.Is(result.err, context.Canceled) {
			t.Fatalf("取消等待应保留对象: %+v", result)
		}
	case <-time.After(time.Second):
		close(releaseProcedure)
		<-procedureDone
		t.Fatal("取消后实体锁等待未退出")
	}
	close(releaseProcedure)
	if err := <-procedureDone; err != nil {
		t.Fatalf("blocking Procedure returned error: %v", err)
	}
	if released, err := manager.TryRelease(context.Background(), lock); err != nil || released {
		t.Fatalf("TryRelease after Procedure = (%t, %v), want (false, nil)", released, err)
	}
}

type blockingWriteBackProcedure struct {
	lock    txn.StoreTarget
	store   *runtimeTestStore
	started chan<- struct{}
	release <-chan struct{}
}

func (procedure blockingWriteBackProcedure) Locks(context.Context) (txn.Lockeys, error) {
	return txn.Lockeys{procedure.lock.Lock}, nil
}

func (procedure blockingWriteBackProcedure) Process(*txn.Tx) error {
	procedure.store.Set()
	close(procedure.started)
	<-procedure.release
	return nil
}

func TestWriteBackManagerTryReleaseRejectsInvalidTarget(t *testing.T) {
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{}, writebackmodule.Config{})
	if released, err := manager.TryRelease(context.Background(), txn.StoreTarget{}); released || !errors.Is(err, writebackmodule.ErrInvalidTarget) {
		t.Fatalf("TryRelease invalid target = (%t, %v), want (false, ErrInvalidTarget)", released, err)
	}
}

func TestWriteBackManagerTryReleaseReturnsTrueForUnknownTarget(t *testing.T) {
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{}, writebackmodule.Config{})
	target := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), &runtimeTestStore{})
	if released, err := manager.TryRelease(context.Background(), target); err != nil || !released {
		t.Fatalf("TryRelease unknown target = (%t, %v), want (true, nil)", released, err)
	}
}

func TestWriteBackManagerTryReleaseReturnsFalseForReservedTarget(t *testing.T) {
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{}, writebackmodule.Config{})
	target := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), &runtimeTestStore{})
	reservation, err := manager.Prepare(context.Background(), []txn.StoreTarget{target})
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	t.Cleanup(reservation.Abort)
	if released, err := manager.TryRelease(context.Background(), target); err != nil || released {
		t.Fatalf("TryRelease reserved target = (%t, %v), want (false, nil)", released, err)
	}
}

func TestWriteBackManagerTryReleaseRejectsIdentityConflict(t *testing.T) {
	first := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	key := txn.NewLockey("role", 0, 1001)
	firstTarget := txn.NewStoreTarget(key, first)
	if err := runtime.Call(context.Background(), runtimeTestProcedure{lock: firstTarget, store: first}); err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if err := manager.Flush(context.Background()); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	secondTarget := txn.NewStoreTarget(key, &runtimeTestStore{})
	if released, err := manager.TryRelease(context.Background(), secondTarget); released || !errors.Is(err, writebackmodule.ErrIdentityConflict) {
		t.Fatalf("TryRelease conflicting target = (%t, %v), want (false, ErrIdentityConflict)", released, err)
	}
}

func TestWriteBackManagerPrepareRejectsTypedNilStore(t *testing.T) {
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{}, writebackmodule.Config{})
	var store *runtimeTestStore
	_, err := manager.Prepare(context.Background(), []txn.StoreTarget{
		txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store),
	})
	if !errors.Is(err, writebackmodule.ErrInvalidTarget) {
		t.Fatalf("Prepare typed nil Store error = %v, want ErrInvalidTarget", err)
	}
}

func TestWriteBackManagerKeepsFailedFlushPending(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{err: errors.New("storage unavailable")}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: writer}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
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

func TestWriteBackManagerIgnoresPureMemoryTransactionTarget(t *testing.T) {
	store := &runtimeMemoryStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("memory", 0, 1001), store)
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

func BenchmarkTxnRuntimeCommitToWriteBackManager(b *testing.B) {
	store := &runtimeTestStore{}
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{Writer: &recordingChangeStoreApplier{}}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		b.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	procedure := runtimeTestProcedure{
		lock:  txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store),
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
		lock:  txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store),
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
