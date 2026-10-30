package framework

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	txruntime "github.com/ngnl47/tx/runtime"
)

type stateWriteBackComponentFake struct{}

func (stateWriteBackComponentFake) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "test-writeback"}
}

type lifecycleProcedure func(*txn.Tx) error

func (p lifecycleProcedure) Locks(context.Context) (txn.Lockeys, error) { return nil, nil }
func (p lifecycleProcedure) Process(tx *txn.Tx) error                   { return p(tx) }

func TestProcedureDrainWaitsBeforeFinalWriteBack(t *testing.T) {
	events := []string{}
	mod, err := NewProcedureBuilder().WriteBack(&recordingProcedureWriteBack{events: &events}).Build()
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(t.Context())
	t.Cleanup(func() { root.Cancel(); root.Wait() })
	server := &Server{runtime: &runtime{lifecycle: root, selectedCMods: map[CModID]struct{}{CModProcedure: {}}}}
	if err := mod.Start(newCModContext(server)); err != nil {
		t.Fatal(err)
	}
	ctx := txruntime.Bind(t.Context(), newRuntimeAccess(server.runtime, Spec{Name: "game", Requires: []CModID{CModProcedure}}))
	entered, proceed, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- txruntime.Call(ctx, lifecycleProcedure(func(tx *txn.Tx) error {
			close(entered)
			<-proceed
			return tx.CallAfterCommit(lifecycleProcedure(func(*txn.Tx) error { return nil }))
		}))
	}()
	<-entered
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	drainErr := mod.(CModDrainer).Drain(canceled)
	before := append([]string(nil), events...)
	close(proceed)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(drainErr, context.Canceled) {
		t.Fatalf("Drain = %v", drainErr)
	}
	if !reflect.DeepEqual(before, []string{"start:writeback"}) {
		t.Fatalf("写回早于事务结束：%v", before)
	}
	if err := mod.(CModDrainer).Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"start:writeback", "drain:writeback"}) {
		t.Fatalf("events = %v", events)
	}
}

func (stateWriteBackComponentFake) Validate(ProcedureContext) error { return nil }
func (stateWriteBackComponentFake) Start(ProcedureContext) error    { return nil }
func (stateWriteBackComponentFake) Drain(context.Context) error     { return nil }
func (stateWriteBackComponentFake) Stop(context.Context) error      { return nil }

type invalidProcedureWriteBackFake struct {
	spec ComponentSpec
}

func (fake invalidProcedureWriteBackFake) ComponentSpec() ComponentSpec { return fake.spec }
func (invalidProcedureWriteBackFake) Validate(ProcedureContext) error   { return nil }
func (invalidProcedureWriteBackFake) Start(ProcedureContext) error      { return nil }
func (invalidProcedureWriteBackFake) Drain(context.Context) error       { return nil }
func (invalidProcedureWriteBackFake) Stop(context.Context) error        { return nil }

func TestProcedureBuilderDescribesSelectedComponents(t *testing.T) {
	mod, err := NewProcedureBuilder().
		WriteBack(stateWriteBackComponentFake{}).
		Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	spec := mod.Spec()
	if spec.ID != CModProcedure {
		t.Fatalf("CMod ID = %q, want %q", spec.ID, CModProcedure)
	}
	want := []ComponentSpec{
		{Role: "transaction", Name: "default"},
		{
			Role: "writeback",
			Name: "test-writeback",
		},
	}
	if !equalComponentSpecs(spec.Components, want) {
		t.Fatalf("components = %#v, want %#v", spec.Components, want)
	}
}

func TestProcedureBuilderRequiresWriteBack(t *testing.T) {
	mod, err := NewProcedureBuilder().Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want missing writeback error", mod, err)
	}
}

func TestProcedureBuilderRejectsTypedNilWriteBack(t *testing.T) {
	var writeBack *recordingProcedureWriteBack
	mod, err := NewProcedureBuilder().WriteBack(writeBack).Build()
	if mod != nil || err == nil {
		t.Fatalf("Build = %#v, %v, want typed nil writeback error", mod, err)
	}
}

func TestProcedureBuilderRejectsInvalidComponentTopology(t *testing.T) {
	tests := []struct {
		name      string
		writeBack ProcedureWriteBack
	}{
		{
			name:      "empty role",
			writeBack: invalidProcedureWriteBackFake{spec: ComponentSpec{Name: "writeback"}},
		},
		{
			name: "duplicate child role",
			writeBack: invalidProcedureWriteBackFake{spec: ComponentSpec{
				Role: "writeback",
				Name: "custom",
				Components: []ComponentSpec{
					{Role: "child", Name: "first"},
					{Role: "child", Name: "second"},
				},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewProcedureBuilder().WriteBack(test.writeBack)
			mod, err := builder.Build()
			if mod != nil || err == nil {
				t.Fatalf("Build = %#v, %v, want invalid component topology error", mod, err)
			}
		})
	}
}

type recordingProcedureWriteBack struct {
	events *[]string
}

func (*recordingProcedureWriteBack) ComponentSpec() ComponentSpec {
	return ComponentSpec{Role: "writeback", Name: "recording"}
}

func (writeBack *recordingProcedureWriteBack) Validate(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "validate:writeback")
	return nil
}

func (writeBack *recordingProcedureWriteBack) Start(runtime ProcedureContext) error {
	*writeBack.events = append(*writeBack.events, "start:writeback")
	if runtime.Transaction() == nil {
		return context.Canceled
	}
	return nil
}

func (writeBack *recordingProcedureWriteBack) Drain(context.Context) error {
	*writeBack.events = append(*writeBack.events, "drain:writeback")
	return nil
}

func (writeBack *recordingProcedureWriteBack) Stop(context.Context) error {
	*writeBack.events = append(*writeBack.events, "stop:writeback")
	return nil
}

func TestProcedureOwnsTransactionAndWriteBackLifecycle(t *testing.T) {
	events := []string{}
	writeBack := &recordingProcedureWriteBack{events: &events}
	mod, err := NewProcedureBuilder().WriteBack(writeBack).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	server := &Server{runtime: &runtime{
		lifecycle: root,
		instance:  testServiceInstance("slggame"),
		selectedCMods: map[CModID]struct{}{
			CModProcedure: {},
		},
	}}
	ctx := newCModContext(server)

	if err := mod.(CModValidator).Validate(ctx); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if server.runtime.transaction == nil {
		t.Fatal("transaction runtime was not installed")
	}
	if err := mod.(CModDrainer).Drain(context.Background()); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if err := mod.(CModStopper).Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	wantEvents := []string{"validate:writeback", "start:writeback", "drain:writeback", "stop:writeback"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func testServiceInstance(names ...string) discovery.Instance {
	return discovery.Instance{Services: names}
}

func equalComponentSpecs(left, right []ComponentSpec) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Role != right[index].Role ||
			left[index].Name != right[index].Name ||
			left[index].Optional != right[index].Optional ||
			!equalStrings(left[index].Services, right[index].Services) ||
			!equalComponentSpecs(left[index].Components, right[index].Components) {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestSubmitDrainWaitsBeforeFinalWriteBack(t *testing.T) {
	events := []string{}
	mod, err := NewProcedureBuilder().WriteBack(&recordingProcedureWriteBack{events: &events}).Build()
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(t.Context())
	t.Cleanup(func() { root.Cancel(); root.Wait() })
	server := &Server{runtime: &runtime{lifecycle: root, selectedCMods: map[CModID]struct{}{CModProcedure: {}}}}
	if err := mod.Start(newCModContext(server)); err != nil {
		t.Fatal(err)
	}
	if err := server.runtime.transaction.ConfigureAsync(txn.AsyncConfig{MaxConcurrent: 1, MaxPending: 2}); err != nil {
		t.Fatal(err)
	}
	ctx := txruntime.Bind(t.Context(), newRuntimeAccess(server.runtime, Spec{Name: "game", Requires: []CModID{CModProcedure}}))
	entered, proceed := make(chan struct{}), make(chan struct{})
	future, err := txruntime.Submit(ctx, lifecycleProcedure(func(tx *txn.Tx) error {
		return tx.CallAfterCommit(lifecycleProcedure(func(*txn.Tx) error {
			close(entered)
			<-proceed
			return nil
		}))
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	queued, err := txruntime.Submit(ctx, lifecycleProcedure(func(*txn.Tx) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	drainErr := mod.(CModDrainer).Drain(canceled)
	before := append([]string(nil), events...)
	close(proceed)
	if err := future.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(drainErr, context.Canceled) {
		t.Fatalf("Drain = %v", drainErr)
	}
	if !reflect.DeepEqual(before, []string{"start:writeback"}) {
		t.Fatalf("写回早于事务结束：%v", before)
	}
	if err := queued.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := mod.(CModDrainer).Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"start:writeback", "drain:writeback"}) {
		t.Fatalf("events = %v", events)
	}
}

func TestSubmitInitFailureRetainsResourcesUntilWorkExits(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	executor := txn.NewRuntime()
	rt := &runtime{lifecycle: root, transaction: executor}
	rt.config.Runtime.Lifecycle.DrainTimeout = 10 * time.Millisecond
	entered, finish := make(chan struct{}), make(chan struct{})
	stopped := false
	var future *txn.Future
	registry, err := newRegistry(
		testService{spec: Spec{Name: "game"}, start: func(Context) error {
			var err error
			future, err = executor.Submit(t.Context(), lifecycleProcedure(func(*txn.Tx) error { close(entered); <-finish; return nil }))
			<-entered
			return err
		}, stop: func(context.Context) error { stopped = true; return nil }},
		testService{spec: Spec{Name: "failure"}, start: func(Context) error { return errors.New("init failed") }},
	)
	if err != nil {
		t.Fatal(err)
	}
	host := newServiceHost(registry, rt)
	err = host.start([]string{"game", "failure"})
	if !errors.Is(err, errStopDeferred) || stopped {
		t.Errorf("init cleanup: err=%v stopped=%v", err, stopped)
	}
	close(finish)
	if err := future.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	server := &Server{runtime: rt, host: host}
	if err := server.finishWork(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if !stopped {
		t.Fatal("任务结束后没有重试初始化回滚")
	}
}

func TestSubmitStartupCleanupWithoutActorsRetainsDependencies(t *testing.T) {
	events := []string{}
	mod, err := NewProcedureBuilder().WriteBack(&recordingProcedureWriteBack{events: &events}).Build()
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(t.Context())
	defer func() { root.Cancel(); root.Wait() }()
	server := &Server{runtime: &runtime{lifecycle: root}}
	server.cmodHost = newCModHost(newCModContext(server), cmodGraph{ordered: []CMod{mod}})
	if err := server.cmodHost.start(); err != nil {
		t.Fatal(err)
	}
	finish := make(chan struct{})
	future, err := server.runtime.transaction.Submit(t.Context(), lifecycleProcedure(func(*txn.Tx) error { <-finish; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	err = server.shutdown(canceled, false)
	if !errors.Is(err, errStopDeferred) {
		t.Errorf("cleanup err=%v", err)
	}
	for _, event := range events {
		if event == "stop:writeback" {
			t.Error("异步任务结束前关闭了写回")
		}
	}
	close(finish)
	if err := future.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := server.shutdown(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1] != "stop:writeback" {
		t.Fatalf("events=%v", events)
	}
}
