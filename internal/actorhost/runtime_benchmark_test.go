package actorhost

import (
	"context"
	"testing"
	"time"
)

func BenchmarkRuntimeCall(b *testing.B) {
	r := New(32, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) { return value, nil }}, nil
	})
	defer r.Drain(context.Background())
	id := Identity{"role", "1"}
	if _, err := r.Call(context.Background(), id, "move"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Call(context.Background(), id, "move"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
