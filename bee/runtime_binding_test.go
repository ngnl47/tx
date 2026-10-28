package bee

import (
	"context"
	txruntime "github.com/ngnl47/tx/runtime"
	"testing"
)

// 项目绑定在服务启动前可用，清理前仍可使用，停止后旧绑定拒绝访问。
func TestSetupRuntimeBindingLifecycle(t *testing.T) {
	var bound txruntime.Provider
	cleaned := false
	server, err := New(Config{AppID: "bound-runtime"}).Setup(func(c Context) (func(), error) {
		bound = c.Runtime()
		return func() {
			if bound.AppID() != "bound-runtime" {
				t.Error("清理时绑定失效")
			}
			cleaned = true
		}, nil
	}).Services(ServiceRegistration{Spec: ServiceSpec{Name: "game"}, Factory: func(c Context) (Service, error) {
		if bound == nil || c.Runtime().AppID() != bound.AppID() {
			t.Error("服务启动前未绑定进程资源")
		}
		return &boundRuntimeService{}, nil
	}}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(context.Background(), RunOptions{Services: []string{"game"}}); err != nil {
		t.Fatal(err)
	}
	if err = server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !cleaned {
		t.Fatal("项目绑定未清理")
	}
	defer func() {
		if recover() == nil {
			t.Error("停止后的旧绑定仍可访问")
		}
	}()
	bound.AppID()
}

type boundRuntimeService struct{}

func (*boundRuntimeService) Init(context.Context) error { return nil }
func (*boundRuntimeService) Stop(context.Context) error { return nil }
