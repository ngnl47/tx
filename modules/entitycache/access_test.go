package entitycache

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/storage"
)

// 能力检查失败时，冷缓存不得回源，热缓存不得绕过检查或回收对象。
func TestRuntimeAccessFailurePreservesCache(t *testing.T) {
	releases := 0
	r, descriptor, loads := managementFixture(t, func(context.Context, any) (bool, error) {
		releases++
		return true, nil
	})
	ref := firstRef(t, r)
	failure := errors.New("未绑定存储能力")
	descriptor.CheckAccess = func(context.Context) error { return failure }
	clones := 0
	clone := descriptor.Clone
	descriptor.Clone = func(value any) (any, error) {
		clones++
		return clone(value)
	}
	inProcedure := func(run func(context.Context) error) error {
		return txn.NewRuntime().Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
			return run(tx.Context())
		}))
	}
	for _, id := range []int64{1, 2} {
		key := storage.RecordKey{ID: id}
		if _, _, err := r.Snapshot(t.Context(), descriptor, key); !errors.Is(err, failure) {
			t.Fatalf("Snapshot(%d): %v", id, err)
		}
		if err := inProcedure(func(ctx context.Context) error {
			_, _, err := r.Find(ctx, descriptor, key)
			return err
		}); !errors.Is(err, failure) {
			t.Fatalf("Find(%d): %v", id, err)
		}
		if err := inProcedure(func(ctx context.Context) error {
			_, _, err := r.Snapshot(ctx, descriptor, key)
			return err
		}); !errors.Is(err, failure) {
			t.Fatalf("事务内 Snapshot(%d): %v", id, err)
		}
	}
	if _, err := r.SnapshotCached(t.Context(), ref); !errors.Is(err, failure) {
		t.Fatalf("SnapshotCached: %v", err)
	}
	if result, err := r.TryRelease(t.Context(), ref); result != Retained || !errors.Is(err, failure) {
		t.Fatalf("TryRelease: %s %v", result, err)
	}
	if err := inProcedure(func(ctx context.Context) error {
		_, err := r.ReserveCreate(ctx, describedValue{descriptor: descriptor})
		return err
	}); !errors.Is(err, failure) {
		t.Fatalf("ReserveCreate: %v", err)
	}
	if *loads != 1 || clones != 0 || releases != 0 {
		t.Fatalf("准入失败触发了数据操作：loads=%d clones=%d releases=%d", *loads, clones, releases)
	}
	page, err := r.List(t.Context(), ListOptions{})
	if err != nil || len(page.Items) != 1 || page.Items[0].UseCount != 0 || page.Items[0].Ref != ref {
		t.Fatalf("准入失败改变了缓存或泄漏使用权：%+v %v", page, err)
	}
}

type describedValue struct{ descriptor *Descriptor }

func (v describedValue) EntityDescriptor() *Descriptor { return v.descriptor }
