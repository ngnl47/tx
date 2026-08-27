package framework

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestCModHostValidatesAllBeforeStartingAny(t *testing.T) {
	wantErr := errors.New("fusion config is invalid")
	events := []string{}
	host := newCModHost(nil, cmodGraph{ordered: []CMod{
		&cmodLifecycleFake{id: "redis", events: &events},
		&cmodLifecycleFake{id: "fusion", validateErr: wantErr, events: &events},
	}})

	err := host.start()
	if !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v, want %v", err, wantErr)
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want no start before validation completes", events)
	}
}

func TestCModHostRollsBackStartedModulesInReverseOrder(t *testing.T) {
	wantErr := errors.New("transaction start failed")
	events := []string{}
	host := newCModHost(nil, cmodGraph{ordered: []CMod{
		&cmodLifecycleFake{id: "redis", events: &events},
		&cmodLifecycleFake{id: "fusion", events: &events},
		&cmodLifecycleFake{id: "transaction", startErr: wantErr, events: &events},
	}})

	err := host.start()
	if !errors.Is(err, wantErr) {
		t.Fatalf("start error = %v, want %v", err, wantErr)
	}
	want := []string{"start:redis", "start:fusion", "start:transaction", "stop:transaction", "stop:fusion", "stop:redis"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestCModHostDrainsAndStopsInReverseOrder(t *testing.T) {
	events := []string{}
	host := newCModHost(nil, cmodGraph{ordered: []CMod{
		&cmodLifecycleFake{id: "redis", events: &events},
		&cmodLifecycleFake{id: "fusion", events: &events},
		&cmodLifecycleFake{id: "transaction", events: &events},
	}})
	if err := host.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := host.drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := host.stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	want := []string{
		"start:redis", "start:fusion", "start:transaction",
		"drain:transaction", "drain:fusion", "drain:redis",
		"stop:transaction", "stop:fusion", "stop:redis",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestCModHostStopsOwnedTasksBeforeRollbackDependencies(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	server := &Server{runtime: &runtime{lifecycle: root}}
	taskStarted := make(chan struct{})
	taskStopped := make(chan struct{})
	owner := &ownedTaskCMod{taskStarted: taskStarted, taskStopped: taskStopped}
	host := newCModHost(newCModContext(server), cmodGraph{ordered: []CMod{
		owner,
		&cmodLifecycleFake{id: "failure", startErr: errors.New("failed"), events: &[]string{}},
	}})

	err := host.start()
	if err == nil {
		t.Fatal("start error = nil, want failure")
	}
	select {
	case <-taskStopped:
	case <-time.After(time.Second):
		t.Fatal("owned task was not stopped during rollback")
	}
	if !owner.stopObservedTaskExit {
		t.Fatal("CMod Stop ran before its owned task exited")
	}
}

type cmodLifecycleFake struct {
	id          CModID
	validateErr error
	startErr    error
	events      *[]string
}

type ownedTaskCMod struct {
	taskStarted          chan struct{}
	taskStopped          chan struct{}
	stopObservedTaskExit bool
}

func (*ownedTaskCMod) Spec() CModSpec { return CModSpec{ID: "owner"} }

func (mod *ownedTaskCMod) Start(runtime CModContext) error {
	return runtime.Tasks().Go(func(ctx context.Context) {
		close(mod.taskStarted)
		<-ctx.Done()
		close(mod.taskStopped)
	})
}

func (mod *ownedTaskCMod) Stop(context.Context) error {
	select {
	case <-mod.taskStopped:
		mod.stopObservedTaskExit = true
	default:
	}
	return nil
}

func (fake *cmodLifecycleFake) Spec() CModSpec { return CModSpec{ID: fake.id} }

func (fake *cmodLifecycleFake) Validate(CModContext) error { return fake.validateErr }

func (fake *cmodLifecycleFake) Start(CModContext) error {
	*fake.events = append(*fake.events, "start:"+string(fake.id))
	return fake.startErr
}

func (fake *cmodLifecycleFake) Drain(context.Context) error {
	*fake.events = append(*fake.events, "drain:"+string(fake.id))
	return nil
}

func (fake *cmodLifecycleFake) Stop(context.Context) error {
	*fake.events = append(*fake.events, "stop:"+string(fake.id))
	return nil
}
