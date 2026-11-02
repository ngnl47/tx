package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/ant"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	txruntime "github.com/ngnl47/tx/runtime"
	"github.com/ngnl47/tx/storage"
)

type createWriteBackComponent struct{ manager *writeback.Manager }

func (createWriteBackComponent) ComponentSpec() ant.ComponentSpec {
	return ant.ComponentSpec{Role: "writeback", Name: "create-test"}
}
func (createWriteBackComponent) Validate(ant.CModProcedureContext) error { return nil }
func (c createWriteBackComponent) Start(ctx ant.CModProcedureContext) error {
	if err := ctx.BindCommitParticipant(c.manager); err != nil {
		return err
	}
	return c.manager.Start(ctx.Tasks())
}
func (c createWriteBackComponent) Drain(ctx context.Context) error { return c.manager.Shutdown(ctx) }
func (createWriteBackComponent) Stop(context.Context) error        { return nil }

type createService func(context.Context) error

func (s createService) Init(ctx context.Context) error { return s(ctx) }

// 首个共享 GD 操作直接创建，必须由正式 Provider 完成缓存与写回的惰性装配。
func TestRuntimeFirstCreateInitializesEntityCache(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[rollback], func(t *testing.T) {
			writer := &createWriter{}
			manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{
				FlushInterval: time.Hour, ShutdownFlushTimeout: time.Second,
			})
			mod, err := ant.NewCModProcedureBuilder().WriteBack(createWriteBackComponent{manager}).Build()
			if err != nil {
				t.Fatal(err)
			}
			var serviceCtx context.Context
			app, err := ant.New(ant.Config{AppID: "first-create"}).CMods(mod).Services(ant.ServiceRegistration{
				Spec: ant.ServiceSpec{Name: "game", Requires: []ant.CModID{ant.CModProcedure}},
				Factory: func(ant.Context) (ant.Service, error) {
					return createService(func(ctx context.Context) error { serviceCtx = ctx; return nil }), nil
				},
			}).Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := app.Start(t.Context(), ant.RunOptions{Services: []string{"game"}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := app.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			})
			descriptor := &entitycache.Descriptor{
				Load:  func(context.Context, storage.RecordKey) (any, bool, error) { return nil, false, nil },
				Store: func(v any) storage.ChangeStore { return v.(*managedRoot).Store() },
				Clone: func(v any) (any, error) { return &managedRoot{descriptor: v.(*managedRoot).descriptor}, nil },
			}
			root := &managedRoot{descriptor: descriptor}
			failure := errors.New("rollback")
			err = txruntime.Call(serviceCtx, entityProcedure(func(tx *txn.Tx) error {
				if err := txruntime.Create(tx.Context(), root); err != nil {
					return err
				}
				if rollback {
					return failure
				}
				return nil
			}))
			if rollback {
				if !errors.Is(err, failure) {
					t.Fatalf("首次创建未到达回滚: %v", err)
				}
			} else if err != nil {
				t.Fatalf("首次创建失败: %v", err)
			}
			cache, err := txruntime.EntityCache(serviceCtx)
			if err != nil {
				t.Fatal(err)
			}
			ctx, end := entitycache.Begin(serviceCtx)
			defer end()
			_, visible, err := cache.Snapshot(ctx, descriptor, root.RecordKey())
			wantPending := 1
			if rollback {
				wantPending = 0
			}
			if err != nil || visible == rollback || manager.Pending() != wantPending || writer.inserts != 0 {
				t.Fatalf("创建结束状态: visible=%v pending=%d inserts=%d err=%v", visible, manager.Pending(), writer.inserts, err)
			}
			if err := manager.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if writer.inserts != wantPending || manager.Pending() != 0 {
				t.Fatalf("写回状态: inserts=%d pending=%d", writer.inserts, manager.Pending())
			}
		})
	}
}
