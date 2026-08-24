package server

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

func TestRegistrySelectsServicesInRegistrationOrder(t *testing.T) {
	registry, err := newRegistry(
		testService{spec: Spec{Name: "game"}},
		testService{spec: Spec{Name: "room"}},
		testService{spec: Spec{Name: "task"}},
	)
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	selected, err := registry.selectServices("*,!room")
	if err != nil {
		t.Fatalf("selectServices returned error: %v", err)
	}
	if names := selectedNames(selected); !reflect.DeepEqual(names, []string{"game", "task"}) {
		t.Fatalf("selected services = %#v, want game and task", names)
	}
}

func TestServiceSelectionReturnsStableNames(t *testing.T) {
	selection := newServiceSelection([]Service{
		testService{spec: Spec{Name: "msgq"}},
		testService{spec: Spec{Name: "agent"}},
	})
	if got, want := selection.Names(), []string{"agent", "msgq"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selection names = %v, want %v", got, want)
	}
}

func TestRegistryRejectsUnknownAndEmptySelection(t *testing.T) {
	registry, err := newRegistry(testService{spec: Spec{Name: "game"}})
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	if _, err := registry.selectServices("unknown"); err == nil {
		t.Fatal("unknown service error = nil, want error")
	}
	if _, err := registry.selectServices("!game"); err == nil {
		t.Fatal("empty selection error = nil, want error")
	}
}

func TestServiceHostRollsBackAndStopsInReverseOrder(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	runtime := &runtime{lifecycle: root}
	var calls []string
	registry, err := newRegistry(
		testService{
			spec:  Spec{Name: "game"},
			start: func(Context) error { calls = append(calls, "start:game"); return nil },
			stop:  func(context.Context) error { calls = append(calls, "stop:game"); return nil },
		},
		testService{
			spec:  Spec{Name: "task"},
			start: func(Context) error { calls = append(calls, "start:task"); return context.DeadlineExceeded },
		},
	)
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	host := newServiceHost(registry, runtime)
	if err := host.start("game,task"); err == nil {
		t.Fatal("start error = nil, want failure")
	}
	if want := []string{"start:game", "start:task", "stop:game"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestServiceHostDrainWaitsForAcceptedWork(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	runtime := &runtime{lifecycle: root}
	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := newRegistry(testService{
		spec: Spec{Name: "agent"},
		start: func(runtime Context) error {
			done, err := runtime.Admission().Enter()
			if err != nil {
				return err
			}
			close(started)
			go func() {
				<-release
				done()
			}()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	host := newServiceHost(registry, runtime)
	if err := host.start("agent"); err != nil {
		t.Fatalf("start returned error: %v", err)
	}
	<-started

	drained := make(chan error, 1)
	go func() { drained <- host.drain(context.Background()) }()
	select {
	case err := <-drained:
		t.Fatalf("drain returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not wait for accepted work")
	}
}

func TestServiceHostDrainTimeoutReportsServiceAndPendingWork(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	runtime := &runtime{lifecycle: root}
	var done func()
	registry, err := newRegistry(testService{
		spec: Spec{Name: "game"},
		start: func(runtime Context) error {
			var enterErr error
			done, enterErr = runtime.Admission().Enter()
			return enterErr
		},
	})
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	host := newServiceHost(registry, runtime)
	if err := host.start("game"); err != nil {
		t.Fatalf("start returned error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = host.drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain error = %v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), `service "game"`) || !strings.Contains(err.Error(), "pending 1") {
		t.Fatalf("drain error = %v, want service and pending count", err)
	}
	done()
}

func TestServiceHostDrainCanWaitAgainAfterTimeout(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	runtime := &runtime{lifecycle: root}
	var done func()
	drainCalls := 0
	registry, err := newRegistry(testService{
		spec: Spec{Name: "game"},
		start: func(runtime Context) error {
			var enterErr error
			done, enterErr = runtime.Admission().Enter()
			return enterErr
		},
		drain: func(context.Context) error {
			drainCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("newRegistry returned error: %v", err)
	}
	host := newServiceHost(registry, runtime)
	if err := host.start("game"); err != nil {
		t.Fatalf("start returned error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = host.drain(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first drain error = %v, want deadline exceeded", err)
	}

	done()
	if err := host.drain(context.Background()); err != nil {
		t.Fatalf("second drain returned error: %v", err)
	}
	if drainCalls != 1 {
		t.Fatalf("service drain calls = %d, want 1", drainCalls)
	}
}

type testService struct {
	spec  Spec
	start func(Context) error
	drain func(context.Context) error
	stop  func(context.Context) error
}

func (service testService) Spec() Spec { return service.spec }

func (service testService) Start(runtime Context) error {
	if service.start == nil {
		return nil
	}
	return service.start(runtime)
}

func (service testService) Drain(ctx context.Context) error {
	if service.drain == nil {
		return nil
	}
	return service.drain(ctx)
}

func (service testService) Stop(ctx context.Context) error {
	if service.stop == nil {
		return nil
	}
	return service.stop(ctx)
}

func selectedNames(services []Service) []string {
	names := make([]string, 0, len(services))
	for _, service := range services {
		names = append(names, service.Spec().Name)
	}
	return names
}
