package actortrial

import (
	"context"
	"testing"
	"time"
)

func BenchmarkActivationProbe(b *testing.B) {
	release := make(chan struct{})
	r := New(4, time.Second, func(Context) (*Behavior, error) { <-release; return nil, ErrActivationDenied })
	defer func() { close(release); _ = r.Drain(context.Background()) }()
	id := Identity{"world", "51"}
	if _, err := r.EnsureActivation(id); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if status := r.Probe(id); status.State != Initializing {
			b.Fatal(status)
		}
	}
	b.StopTimer()
}

// 完整加载、业务卸载和收尾都计时；与无预算原型同批比较准入机制的增量成本。
func BenchmarkActivationCycle(b *testing.B) {
	for _, limited := range []bool{false, true} {
		name := "unlimited"
		if limited {
			name = "limited"
		}
		b.Run(name, func(b *testing.B) {
			r := New(4, time.Second, func(Context) (*Behavior, error) {
				return &Behavior{Receive: func(c Context, v any) (any, error) { c.Deactivate(); return v, nil }}, nil
			})
			if limited {
				r.SetActivationLimits(ActivationLimits{Bytes: 100, Initializations: 1, Estimate: func(Identity) int64 { return 60 }})
			}
			defer r.Drain(context.Background())
			id := Identity{"world", "51"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.Call(context.Background(), id, "finish"); err != nil {
					b.Fatal(err)
				}
				r.workers.Wait()
			}
			b.StopTimer()
		})
	}
}
