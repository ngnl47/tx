package bee

import (
	"context"
	"strings"
	"testing"
)

// 构造阶段的能力声明错误必须变成启动错误，并清理已经启动的服务。
func TestCapabilityFailureRollsBackStartedService(t *testing.T) {
	stopped := false
	server, err := New(Config{AppID: "capability-test"}).Services(
		ServiceRegistration{Spec: ServiceSpec{Name: "first"}, Factory: func(Context) (Service, error) {
			return &capabilityTestService{stopped: &stopped}, nil
		}},
		ServiceRegistration{Spec: ServiceSpec{Name: "second"}, Factory: func(runtime Context) (Service, error) {
			_ = runtime.CModActor()
			return serviceFunc(func(context.Context) error { return nil }), nil
		}},
	).Build()
	if err != nil {
		t.Fatal(err)
	}
	err = server.Start(context.Background(), RunOptions{Services: "first,second"})
	if err == nil || !strings.Contains(err.Error(), "second") || !strings.Contains(err.Error(), "actor") {
		t.Fatalf("启动错误 = %v", err)
	}
	if !stopped {
		t.Fatal("已启动服务未回滚")
	}
}

type capabilityTestService struct{ stopped *bool }

func (*capabilityTestService) Start(context.Context) error  { return nil }
func (s *capabilityTestService) Stop(context.Context) error { *s.stopped = true; return nil }

// Setup/CMod 边界只转换能力错误，继续使用既有的逆序清理。
func TestCapabilityFailureAtAssemblyBoundaries(t *testing.T) {
	for _, phase := range []string{"setup", "start", "validate"} {
		t.Run(phase, func(t *testing.T) {
			stopped := false
			cleanup := false
			mod := &capabilityTestMod{phase: phase, stopped: &stopped}
			builder := New(Config{AppID: "capability-test"}).CMods(mod).Services(ServiceRegistration{
				Spec:    ServiceSpec{Name: "game", Requires: []CModID{"custom"}},
				Factory: func(Context) (Service, error) { return serviceFunc(func(context.Context) error { return nil }), nil },
			})
			if phase == "setup" {
				builder.Setup(func(Context) (func(), error) { return func() { cleanup = true }, nil })
				builder.Setup(func(c Context) (func(), error) { c.CModActor(); return nil, nil })
			}
			server, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			err = server.Start(context.Background(), RunOptions{Services: "game"})
			if err == nil || !strings.Contains(err.Error(), "actor") {
				t.Fatalf("启动错误 = %v", err)
			}
			if stopped != (phase != "validate") {
				t.Fatalf("模块清理 = %v，阶段 %s", stopped, phase)
			}
			if phase == "setup" && !cleanup {
				t.Fatal("Setup 清理未执行")
			}
			if phase == "validate" && !strings.Contains(err.Error(), "Validate") {
				t.Fatalf("缺少阶段诊断: %v", err)
			}
		})
	}
}

type capabilityTestMod struct {
	phase   string
	stopped *bool
}

func (*capabilityTestMod) Spec() CModSpec { return CModSpec{ID: "custom"} }
func (m *capabilityTestMod) Validate(c CModContext) error {
	if m.phase == "validate" {
		c.CModActor()
	}
	return nil
}
func (m *capabilityTestMod) Start(c CModContext) error {
	if m.phase == "start" {
		c.CModActor()
	}
	return nil
}
func (m *capabilityTestMod) Stop(context.Context) error { *m.stopped = true; return nil }

func TestServiceFactoryBusinessPanicIsNotConverted(t *testing.T) {
	marker := "business panic"
	defer func() {
		if got := recover(); got != marker {
			t.Fatalf("panic = %v", got)
		}
	}()
	adapter := serviceAdapter{registration: ServiceRegistration{Spec: ServiceSpec{Name: "game"}, Factory: func(Context) (Service, error) { panic(marker) }}}
	_ = adapter.Start(nil)
}
