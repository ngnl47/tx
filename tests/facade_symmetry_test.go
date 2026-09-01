package tests

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ngnl47/tx/ant"
	"github.com/ngnl47/tx/bee"
	txstorage "github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestAntPackageDocumentDescribesStatefulFacade(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "../ant/doc.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse Ant package document: %v", err)
	}
	if file.Doc == nil {
		t.Fatal("Ant package document is missing")
	}
	document := file.Doc.Text()
	if !strings.HasPrefix(document, "Package ant ") {
		t.Fatalf("Ant package document starts with %q", firstLine(document))
	}
	for _, keyword := range []string{"有状态", "Procedure", "逻辑槽位"} {
		if !strings.Contains(document, keyword) {
			t.Fatalf("Ant package document does not describe %q", keyword)
		}
	}
	if ast.IsExported(file.Name.Name) {
		t.Fatalf("unexpected exported package name %q", file.Name.Name)
	}
}

func TestBeeCommonFacadeMethodsAreCompatibleWithAnt(t *testing.T) {
	assertMethodSetSubset(t, reflect.TypeOf((*bee.Context)(nil)).Elem(), reflect.TypeOf((*ant.Context)(nil)).Elem())
	assertMethodSetSubset(t, reflect.TypeOf((*bee.Storage)(nil)).Elem(), reflect.TypeOf((*ant.Storage)(nil)).Elem())
	assertMethodSetSubset(t, reflect.TypeOf((*bee.RedisStorage)(nil)).Elem(), reflect.TypeOf((*ant.RedisStorage)(nil)).Elem())
	assertMethodSetSubset(t, reflect.TypeOf((*bee.MongoStorage)(nil)).Elem(), reflect.TypeOf((*ant.MongoStorage)(nil)).Elem())
	assertMethodSetSubset(t, reflect.TypeOf((*bee.FusionStorage)(nil)).Elem(), reflect.TypeOf((*ant.FusionStorage)(nil)).Elem())
	assertMethodSetSubset(t, reflect.TypeOf((*bee.Builder)(nil)), reflect.TypeOf((*ant.Builder)(nil)))
	assertMethodSetSubset(t, reflect.TypeOf((*bee.Server)(nil)), reflect.TypeOf((*ant.Server)(nil)))
}

func TestStorageCapabilitiesExposeOnlyTheirDeclaredReaders(t *testing.T) {
	assertStorageReaderMethods(t, reflect.TypeOf((*bee.Storage)(nil)).Elem())
	assertStorageReaderMethods(t, reflect.TypeOf((*bee.RedisStorage)(nil)).Elem(), "RedisReader")
	assertStorageReaderMethods(t, reflect.TypeOf((*bee.MongoStorage)(nil)).Elem(), "MongoReader")
	assertStorageReaderMethods(t, reflect.TypeOf((*bee.FusionStorage)(nil)).Elem(), "FusionReader")
	assertStorageReaderMethods(t, reflect.TypeOf((*ant.Storage)(nil)).Elem())
	assertStorageReaderMethods(t, reflect.TypeOf((*ant.RedisStorage)(nil)).Elem(), "RedisReader")
	assertStorageReaderMethods(t, reflect.TypeOf((*ant.MongoStorage)(nil)).Elem(), "MongoReader")
	assertStorageReaderMethods(t, reflect.TypeOf((*ant.FusionStorage)(nil)).Elem(), "FusionReader")
}

func TestMongoStorageExposesOnlyNarrowAtomicCapabilities(t *testing.T) {
	var beeMongo bee.MongoStorage
	var antMongo ant.MongoStorage
	var _ txstorage.DocumentBackfiller = beeMongo
	var _ txmongo.SequenceAllocator = beeMongo
	var _ txstorage.DocumentBackfiller = antMongo
	var _ txmongo.SequenceAllocator = antMongo
}

func assertStorageReaderMethods(t *testing.T, current reflect.Type, allowed ...string) {
	t.Helper()
	want := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		want[name] = struct{}{}
	}
	for _, name := range []string{"RedisReader", "MongoReader", "FusionReader"} {
		_, exists := current.MethodByName(name)
		_, expected := want[name]
		if exists != expected {
			t.Fatalf("%s method %s exists = %t, want %t", current, name, exists, expected)
		}
	}
}

func TestBeeCommonFacadeStructsAreCompatibleWithAnt(t *testing.T) {
	pairs := []struct {
		bee any
		ant any
	}{
		{bee.Config{}, ant.Config{}},
		{bee.StorageConfig{}, ant.StorageConfig{}},
		{bee.RuntimeConfig{}, ant.RuntimeConfig{}},
		{bee.LogConfig{}, ant.LogConfig{}},
		{bee.LifecycleConfig{}, ant.LifecycleConfig{}},
		{bee.DelayTaskConfig{}, ant.DelayTaskConfig{}},
		{bee.SchedulerConfig{}, ant.SchedulerConfig{}},
		{bee.HealthConfig{}, ant.HealthConfig{}},
		{bee.ActorConfig{}, ant.ActorConfig{}},
		{bee.DiscoveryConfig{}, ant.DiscoveryConfig{}},
		{bee.RunOptions{}, ant.RunOptions{}},
		{bee.CModSpec{}, ant.CModSpec{}},
		{bee.ComponentSpec{}, ant.ComponentSpec{}},
		{bee.ServiceSpec{}, ant.ServiceSpec{}},
		{bee.ServiceRegistration{}, ant.ServiceRegistration{}},
	}
	for _, pair := range pairs {
		assertStructFieldsSubset(t, reflect.TypeOf(pair.bee), reflect.TypeOf(pair.ant))
	}
}

func assertMethodSetSubset(t *testing.T, subset, superset reflect.Type) {
	t.Helper()
	for index := range subset.NumMethod() {
		method := subset.Method(index)
		other, ok := superset.MethodByName(method.Name)
		if !ok {
			t.Fatalf("Ant %s is missing Bee method %s", superset, method.Name)
		}
		if normalizeFacadeType(method.Type.String()) != normalizeFacadeType(other.Type.String()) {
			t.Fatalf("method %s differs: Bee=%s Ant=%s", method.Name, method.Type, other.Type)
		}
	}
}

func assertStructFieldsSubset(t *testing.T, subset, superset reflect.Type) {
	t.Helper()
	for index := range subset.NumField() {
		field := subset.Field(index)
		other, ok := superset.FieldByName(field.Name)
		if !ok {
			t.Fatalf("Ant %s is missing Bee field %s", superset, field.Name)
		}
		if normalizeFacadeType(field.Type.String()) != normalizeFacadeType(other.Type.String()) {
			t.Fatalf("field %s.%s differs: Bee=%s Ant=%s", subset, field.Name, field.Type, other.Type)
		}
	}
}

func normalizeFacadeType(value string) string {
	value = strings.ReplaceAll(value, "bee.", "facade.")
	return strings.ReplaceAll(value, "ant.", "facade.")
}

func firstLine(value string) string {
	if line, _, ok := strings.Cut(value, "\n"); ok {
		return line
	}
	return value
}

func TestBeeAndAntBuildersExposeTheSameCommonChain(t *testing.T) {
	beeBuilder := bee.New(bee.Config{}).
		CMods().
		Services().
		ConfigProvider(func(bee.AssemblySelection) (bee.Config, error) { return bee.Config{}, nil }).
		GeneratedData(nil, nil).
		Setup(func(bee.Context) (func(), error) { return nil, nil })
	antBuilder := ant.New(ant.Config{}).
		CMods().
		Services().
		ConfigProvider(func(ant.AssemblySelection) (ant.Config, error) { return ant.Config{}, nil }).
		GeneratedData(nil, nil).
		Setup(func(ant.Context) (func(), error) { return nil, nil })

	if beeBuilder == nil || antBuilder == nil {
		t.Fatal("framework builder is nil")
	}
	_, _ = beeBuilder.Build()
	_, _ = antBuilder.Build()
}

func TestBeeAndAntServersExposeTheSameLifecycle(t *testing.T) {
	beeMethods := []string{"Drain", "Run", "Start", "Stop"}
	antMethods := []string{"Drain", "Run", "Start", "Stop"}
	if !slices.Equal(beeMethods, antMethods) {
		t.Fatalf("server lifecycle differs: Bee=%v Ant=%v", beeMethods, antMethods)
	}

	var beeStart func(context.Context, bee.RunOptions) error = (*bee.Server)(nil).Start
	var beeRun func(context.Context, bee.RunOptions) error = (*bee.Server)(nil).Run
	var beeDrain func(context.Context) error = (*bee.Server)(nil).Drain
	var beeStop func(context.Context) error = (*bee.Server)(nil).Stop
	var antStart func(context.Context, ant.RunOptions) error = (*ant.Server)(nil).Start
	var antRun func(context.Context, ant.RunOptions) error = (*ant.Server)(nil).Run
	var antDrain func(context.Context) error = (*ant.Server)(nil).Drain
	var antStop func(context.Context) error = (*ant.Server)(nil).Stop
	_, _, _, _ = beeStart, beeRun, beeDrain, beeStop
	_, _, _, _ = antStart, antRun, antDrain, antStop
}

func TestBeeAndAntRunTheSameServiceLifecycle(t *testing.T) {
	beeEvents := &eventLog{}
	antEvents := &eventLog{}
	beeServer, err := bee.New(bee.Config{AppID: "facade-test"}).Services(bee.ServiceRegistration{
		Spec: bee.ServiceSpec{Name: "game"},
		Factory: func(bee.Context) (bee.Service, error) {
			beeEvents.add("factory")
			return beeServiceRecorder{events: beeEvents}, nil
		},
	}).Build()
	if err != nil {
		t.Fatalf("build Bee: %v", err)
	}
	antServer, err := ant.New(ant.Config{AppID: "facade-test"}).Services(ant.ServiceRegistration{
		Spec: ant.ServiceSpec{Name: "game"},
		Factory: func(ant.Context) (ant.Service, error) {
			antEvents.add("factory")
			return antServiceRecorder{events: antEvents}, nil
		},
	}).Build()
	if err != nil {
		t.Fatalf("build Ant: %v", err)
	}

	runLifecycle(t, beeServer.Start, beeServer.Drain, beeServer.Stop, bee.RunOptions{Services: "game"})
	runLifecycle(t, antServer.Start, antServer.Drain, antServer.Stop, ant.RunOptions{Services: "game"})
	if !slices.Equal(beeEvents.values(), antEvents.values()) {
		t.Fatalf("service lifecycle differs: Bee=%v Ant=%v", beeEvents.values(), antEvents.values())
	}
}

func runLifecycle[T any](
	t *testing.T,
	start func(context.Context, T) error,
	drain func(context.Context) error,
	stop func(context.Context) error,
	options T,
) {
	t.Helper()
	if err := start(context.Background(), options); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (log *eventLog) add(event string) {
	log.mu.Lock()
	log.events = append(log.events, event)
	log.mu.Unlock()
}

func (log *eventLog) values() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

type beeServiceRecorder struct{ events *eventLog }

func (service beeServiceRecorder) Start(context.Context) error {
	service.events.add("start")
	return nil
}

func (service beeServiceRecorder) Drain(context.Context) error {
	service.events.add("drain")
	return nil
}

func (service beeServiceRecorder) Stop(context.Context) error {
	service.events.add("stop")
	return nil
}

type antServiceRecorder struct{ events *eventLog }

func (service antServiceRecorder) Start(context.Context) error {
	service.events.add("start")
	return nil
}

func (service antServiceRecorder) Drain(context.Context) error {
	service.events.add("drain")
	return nil
}

func (service antServiceRecorder) Stop(context.Context) error {
	service.events.add("stop")
	return nil
}
