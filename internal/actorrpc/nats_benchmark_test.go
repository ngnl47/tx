package actorrpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func BenchmarkNATSCall(b *testing.B) {
	url := os.Getenv("TX_ACTORTRIAL_NATS_URL")
	if url == "" {
		b.Skip("需要显式指定 TX_ACTORTRIAL_NATS_URL")
	}
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "remote"
		}
		b.Run(name, func(b *testing.B) {
			ns := nats.NewInbox()
			r := echoRuntime()
			a, err := ConnectNATS(url, ns, "a", r, 32)
			if err != nil {
				b.Fatal(err)
			}
			defer a.conn.Close()
			defer r.Drain(context.Background())
			target := actorhost.Target{Instance: "a", Identity: actorhost.Identity{Kind: "role", Key: "1"}}
			if remote {
				rb := echoRuntime()
				other, err := ConnectNATS(url, ns, "b", rb, 32)
				if err != nil {
					b.Fatal(err)
				}
				defer other.conn.Close()
				defer rb.Drain(context.Background())
				target.Instance = "b"
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			delivery := &actorpb.Delivery{MessageType: "move", Payload: make([]byte, 128)}
			if _, err := a.Call(ctx, target, delivery); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := a.Call(ctx, target, delivery); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
