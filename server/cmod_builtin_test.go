package server

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	txlog "github.com/ngnl47/tx/log"
)

func TestBuiltInCModsDeclareStableDependencies(t *testing.T) {
	app := &Server{}
	want := map[CModID][]CModID{
		CModRedisStorage:   nil,
		CModMongoStorage:   nil,
		CModFusionStorage:  {CModRedisStorage, CModMongoStorage},
		CModRPC:            nil,
		CModDiscovery:      nil,
		CModActor:          {CModRPC, CModDiscovery},
		CModTransaction:    {CModFusionStorage},
		CModDelayScheduler: {CModFusionStorage},
		CModWAL:            {CModMongoStorage},
	}
	for _, mod := range app.builtInCMods(RunOptions{}) {
		spec := mod.Spec()
		if !slices.Equal(spec.Requires, want[spec.ID]) {
			t.Fatalf("CMod %q requires = %v, want %v", spec.ID, spec.Requires, want[spec.ID])
		}
		delete(want, spec.ID)
	}
	if len(want) != 0 {
		t.Fatalf("missing built-in CMods: %v", want)
	}
}

func TestStartLogsResolvedServicesAndCModsOnce(t *testing.T) {
	var output bytes.Buffer
	service := testService{spec: Spec{Name: "game", Requires: []CModID{"custom-runtime"}}}
	mod := &builtInCMod{spec: CModSpec{ID: "custom-runtime"}}
	app, err := New(
		Config{AppID: "startup-log-test", Log: LogConfig{Output: &output}},
		WithCMod(mod),
		WithService(service),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { txlog.SetDefault(nil) })
	if err := app.Start(context.Background(), RunOptions{Services: "*"}); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Errorf("stop server: %v", err)
		}
	})

	text := output.String()
	if !strings.Contains(text, "services=game") {
		t.Fatalf("startup log = %q, want resolved services", text)
	}
	if !strings.Contains(text, "cmods=custom-runtime") {
		t.Fatalf("startup log = %q, want resolved CMods", text)
	}
	if strings.Count(text, "cmods=custom-runtime") != 1 {
		t.Fatalf("startup log = %q, want CMods once", text)
	}
}

func TestRPCOnlyServiceDoesNotRequireStorageOrGeneratedData(t *testing.T) {
	service := testService{spec: Spec{Name: "rpc-only", Requires: []CModID{CModRPC}}}
	app, err := New(Config{AppID: "rpc-only-test"}, WithService(service))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := app.Start(context.Background(), RunOptions{
		Services:   "rpc-only",
		InstanceID: "rpc-only-1",
		RPCListen:  "127.0.0.1:0",
	}); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		if err := app.Stop(context.Background()); err != nil {
			t.Errorf("stop server: %v", err)
		}
	})
	if app.Context().Storage() != nil {
		t.Fatal("RPC-only service unexpectedly assembled storage")
	}
	if !app.Context().HasCMod(CModRPC) {
		t.Fatal("RPC-only service did not start CModRPC")
	}
}

func TestRedisCModRequiresRoutesOnlyWhenSelected(t *testing.T) {
	service := testService{spec: Spec{Name: "redis-only", Requires: []CModID{CModRedisStorage}}}
	app, err := New(Config{AppID: "redis-only-test"}, WithService(service))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	err = app.Start(context.Background(), RunOptions{Services: "redis-only"})
	if err == nil || !strings.Contains(err.Error(), "redis routes are required") {
		t.Fatalf("start error = %v, want missing Redis routes", err)
	}
}
