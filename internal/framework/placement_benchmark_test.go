package framework

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/internal/actorplacement"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
)

// 只计时实际运行时的缓存命中路径；冷查询与业务执行均在测量之外。
func BenchmarkActorPlacementHit(b *testing.B) {
	for _, size := range []int{2000, 10000} {
		for _, grouped := range []bool{false, true} {
			for _, parallel := range []bool{false, true} {
				b.Run(fmt.Sprintf("%d/grouped=%t/parallel=%t", size, grouped, parallel), func(b *testing.B) {
					tasks := lifecycle.NewWaitGroupContext(context.Background())
					defer func() { tasks.Cancel(); tasks.Wait() }()
					const slot = "fixed-slot"
					option := txactor.WithSlotMapping(func(txactor.Identity) string { return slot })
					if grouped {
						option = txactor.WithPlacement("server")
					}
					typ := txactor.MustType("role", func(txactor.Context) (*txactor.Behavior, error) { return &txactor.Behavior{}, nil }, option)
					p := &actorPlacement{catalog: map[string]actorTypeSpec{"role": {service: "game", actorType: typ}}}
					var reads atomic.Int64
					cache, err := actorplacement.NewCache(tasks, func(context.Context, txactor.Identity) (string, error) { reads.Add(1); return slot, nil })
					if err != nil {
						b.Fatal(err)
					}
					p.cache = cache
					p.ready.Store(true)
					ids := make([]txactor.Identity, size)
					for i := range ids {
						ids[i] = txactor.Identity{Type: "role", ID: txactor.Int64ID(int64(i + 1))}
						if _, err := p.resolve(context.Background(), ids[i]); err != nil {
							b.Fatal(err)
						}
					}
					before := reads.Load()
					run := func(i int) {
						if got, err := p.resolve(context.Background(), ids[i%size]); err != nil || got != slot {
							b.Fatalf("%s %v", got, err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					if parallel {
						b.RunParallel(func(pb *testing.PB) {
							i := 0
							for pb.Next() {
								run(i)
								i++
							}
						})
					} else {
						for i := 0; i < b.N; i++ {
							run(i)
						}
					}
					b.StopTimer()
					if reads.Load() != before {
						b.Fatal("缓存命中发生冷查询")
					}
				})
			}
		}
	}
}
