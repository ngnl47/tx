package framework

import (
	"context"
	"strings"
	"testing"

	txactor "github.com/ngnl47/tx/modules/actor"
)

func TestActorCatalogIncludesUnselectedServicesWithoutStartingThem(t *testing.T) {
	factory := func(txactor.Context) (*txactor.Behavior, error) {
		t.Error("未选中服务却创建 Actor")
		return nil, txactor.ErrActivationDenied
	}
	app, err := New(Config{AppID: "catalog"}, WithService(
		testService{spec: Spec{Name: "agent"}},
		testService{spec: Spec{Name: "slgworld", ActorKinds: []txactor.Kind{txactor.MustKind("world", factory), txactor.MustKind("seasonWorld", factory)}}, start: func(Context) error { t.Error("启动了未选中服务"); return nil }},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(t.Context(), RunOptions{Services: "agent"}); err != nil {
		t.Fatal(err)
	}
	defer app.Stop(context.Background())
	if len(app.runtime.actorKinds) != 0 || app.runtime.hasCMod(CModActor) {
		t.Fatal("远端规格触发了本地 Actor 装配")
	}
	for _, name := range []string{"world", "seasonWorld"} {
		spec, ok := app.runtime.actorCatalog[name]
		if !ok || spec.service != "slgworld" || spec.kind.Name() != name {
			t.Fatalf("多对一规格缺失: %q %+v", name, spec)
		}
	}
}

func TestActorCatalogRejectsDuplicateInUnselectedServices(t *testing.T) {
	kind := txactor.MustKind("world", func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) { return nil, nil }}, nil
	})
	_, err := New(Config{AppID: "catalog"}, WithService(
		testService{spec: Spec{Name: "agent"}},
		testService{spec: Spec{Name: "slgworld", ActorKinds: []txactor.Kind{kind}}},
		testService{spec: Spec{Name: "season", ActorKinds: []txactor.Kind{kind}}},
	))
	if err == nil || !strings.Contains(err.Error(), "duplicate Actor kind") {
		t.Fatalf("未拒绝全局重名: %v", err)
	}
}
