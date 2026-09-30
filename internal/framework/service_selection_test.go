package framework

import (
	"reflect"
	"testing"
)

// 进程组合与启动顺序由项目决定，框架仅校验并执行明确清单。
func TestExplicitServiceSelectionKeepsProjectOrder(t *testing.T) {
	registry, err := newRegistry(testService{spec: Spec{Name: "agent"}}, testService{spec: Spec{Name: "msgq"}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := registry.selectServices([]string{"msgq", "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if got := selectedNames(selected); !reflect.DeepEqual(got, []string{"msgq", "agent"}) {
		t.Fatal(got)
	}
	for _, names := range [][]string{nil, {"unknown"}, {"*"}, {"agent", "agent"}} {
		if _, err := registry.selectServices(names); err == nil {
			t.Fatalf("accepted %v", names)
		}
	}
}
